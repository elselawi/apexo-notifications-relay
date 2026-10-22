import { env, fetchMock } from 'cloudflare:test';
import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import worker from '../src/index';

// A correctly-typed `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

const CLINIC_SERVER = 'clinic-1';
const CLINIC_KEY = 'clinic-secret-key';
const ACCOUNT_ID = 'account-1';
const DEVICE_TOKEN = 'device-token-1';
const FCM_PROJECT = 'test-project';
const FCM_ORIGIN = 'https://fcm.googleapis.com';
const OAUTH_ORIGIN = 'https://oauth2.googleapis.com';

async function hashKey(key: string): Promise<string> {
	const msgUint8 = new TextEncoder().encode(key);
	const hashBuffer = await crypto.subtle.digest('SHA-256', msgUint8);
	const hashArray = Array.from(new Uint8Array(hashBuffer));
	return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
}

function mockOAuthToken() {
	fetchMock
		.get(OAUTH_ORIGIN)
		.intercept({ method: 'POST', path: '/token' })
		.reply(200, JSON.stringify({ access_token: 'test-access-token' }));
}

async function putDeviceViaApi(token = DEVICE_TOKEN, platform?: string) {
	const request = new IncomingRequest('http://example.com/put-device', {
		method: 'POST',
		body: JSON.stringify({
			clinicServer: CLINIC_SERVER,
			clinicKey: CLINIC_KEY,
			deviceToken: token,
			accountId: ACCOUNT_ID,
			...(platform == null ? {} : { platform }),
		}),
	});
	return await worker.fetch(request);
}

const ALERT = { title: 'New appointment', body: 'Ada Lovelace: appointment created' };

async function push(accountIds: string[] = [ACCOUNT_ID], alert?: { title: string; body: string }) {
	const request = new IncomingRequest('http://example.com/push', {
		method: 'POST',
		body: JSON.stringify({
			clinicServer: CLINIC_SERVER,
			clinicKey: CLINIC_KEY,
			accountIds,
			data: { title: 'hello' },
			...(alert == null ? {} : { alert }),
		}),
	});
	return await worker.fetch(request);
}

/// Answers FCM, and returns the message bodies it was given.
function captureFcm() {
	const bodies: Record<string, unknown>[] = [];
	fetchMock
		.get(FCM_ORIGIN)
		.intercept({
			method: 'POST',
			path: `/v1/projects/${FCM_PROJECT}/messages:send`,
		})
		.reply((opts) => {
			bodies.push(JSON.parse(opts.body as string));
			return {
				statusCode: 200,
				data: JSON.stringify({ name: 'projects/test-project/messages/msg-1' }),
			};
		});
	return bodies;
}

// `cleanMocks()` exists at runtime on each per-origin mock client but is not
// exposed by the `Interceptable` type, so cast around it.
function cleanFetchMocks() {
	const cleanable = (origin: string) =>
		(fetchMock.get(origin) as unknown as { cleanMocks(): void }).cleanMocks();
	cleanable(FCM_ORIGIN);
	cleanable(OAUTH_ORIGIN);
}

describe('Apexo Notifications Relay', () => {
	beforeAll(() => {
		fetchMock.activate();
		fetchMock.disableNetConnect();
	});

	beforeEach(() => {
		cleanFetchMocks();
	});

	it('responds to /health with "ok"', async () => {
		const request = new IncomingRequest('http://example.com/health');
		const response = await worker.fetch(request);
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');
	});

	it('sends the expected FCM request including the Android and APNs blocks', async () => {
		await putDeviceViaApi();
		mockOAuthToken();

		let capturedBody: Record<string, unknown> | null = null;
		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply((opts) => {
				capturedBody = JSON.parse(opts.body as string);
				return {
					statusCode: 200,
					data: JSON.stringify({ name: 'projects/test-project/messages/msg-1' }),
				};
			});

		const response = await push();
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		expect(capturedBody).not.toBeNull();
		const message = capturedBody!.message as Record<string, unknown>;
		expect(message.token).toBe(DEVICE_TOKEN);
		expect(message.data).toEqual({ title: 'hello' });
		expect(message.notification).toBeUndefined();
		expect(message.android).toEqual({ priority: 'high' });
		expect(message.apns).toEqual({
			headers: { 'apns-push-type': 'background', 'apns-priority': '5' },
			payload: { aps: { 'content-available': 1 } },
		});
	});

	it('sends FCM for legacy device records whose device value is a string', async () => {
		// Legacy record shape: `devices` maps an FCM token to a plain-string account ID.
		await env.apexo_notifications_relay.put(
			CLINIC_SERVER,
			JSON.stringify({
				key: await hashKey(CLINIC_KEY),
				devices: { [DEVICE_TOKEN]: ACCOUNT_ID },
			})
		);
		mockOAuthToken();

		let fcmCalled = false;
		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply(() => {
				fcmCalled = true;
				return {
					statusCode: 200,
					data: JSON.stringify({ name: 'projects/test-project/messages/msg-1' }),
				};
			});

		const response = await push();
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');
		expect(fcmCalled).toBe(true);
	});

	it('sends an iOS device an alert push, which the system displays on its own', async () => {
		await putDeviceViaApi(DEVICE_TOKEN, 'ios');
		mockOAuthToken();
		const bodies = captureFcm();

		const response = await push([ACCOUNT_ID], ALERT);
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		expect(bodies).toHaveLength(1);
		const message = bodies[0].message as Record<string, unknown>;
		expect(message.token).toBe(DEVICE_TOKEN);
		expect(message.data).toEqual({ title: 'hello' });
		expect(message.notification).toBeUndefined();
		expect(message.android).toBeUndefined();
		expect(message.apns).toEqual({
			headers: { 'apns-push-type': 'alert', 'apns-priority': '10' },
			payload: {
				aps: {
					alert: { title: ALERT.title, body: ALERT.body },
					sound: 'default',
					badge: 1,
				},
			},
		});
	});

	it('keeps the silent push for a Mac, even when an alert is offered', async () => {
		await putDeviceViaApi(DEVICE_TOKEN, 'macos');
		mockOAuthToken();
		const bodies = captureFcm();

		const response = await push([ACCOUNT_ID], ALERT);
		expect(response.status).toBe(200);

		const message = bodies[0].message as Record<string, unknown>;
		expect(message.android).toEqual({ priority: 'high' });
		expect(message.apns).toEqual({
			headers: { 'apns-push-type': 'background', 'apns-priority': '5' },
			payload: { aps: { 'content-available': 1 } },
		});
	});

	it('moves the platform with the token on /replace-token', async () => {
		await putDeviceViaApi('old-token', 'ios');
		mockOAuthToken();

		const replaced = await worker.fetch(
			new IncomingRequest('http://example.com/replace-token', {
				method: 'POST',
				body: JSON.stringify({
					clinicServer: CLINIC_SERVER,
					clinicKey: CLINIC_KEY,
					oldToken: 'old-token',
					newToken: 'new-token',
				}),
			})
		);
		expect(replaced.status).toBe(200);

		const bodies = captureFcm();
		const response = await push([ACCOUNT_ID], ALERT);
		expect(response.status).toBe(200);

		const message = bodies[0].message as Record<string, unknown>;
		expect(message.token).toBe('new-token');
		expect((message.apns as Record<string, unknown>).headers).toEqual({
			'apns-push-type': 'alert',
			'apns-priority': '10',
		});
	});

	it('rejects an alert without a title and body', async () => {
		await putDeviceViaApi(DEVICE_TOKEN, 'ios');
		mockOAuthToken();

		const response = await push([ACCOUNT_ID], { title: '', body: 'body' });
		expect(response.status).toBe(400);
	});

	it('removes the token when FCM responds with 404', async () => {
		await putDeviceViaApi();
		mockOAuthToken();

		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply(404, JSON.stringify({ error: { message: 'unregistered token' } }));

		const response = await push();
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		const stored = await env.apexo_notifications_relay.get(CLINIC_SERVER);
		expect(stored).not.toBeNull();
		const clinic = JSON.parse(stored!) as { devices: Record<string, string> };
		expect(clinic.devices[DEVICE_TOKEN]).toBeUndefined();
	});

	it('maps FCM 500 to HTTP 503 (retryable)', async () => {
		await putDeviceViaApi();
		mockOAuthToken();

		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply(500, 'Internal Server Error');

		const response = await push();
		expect(response.status).toBe(503);
	});

	it('maps FCM 400 to HTTP 400 (permanent)', async () => {
		await putDeviceViaApi();
		mockOAuthToken();

		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply(400, 'Bad Request');

		const response = await push();
		expect(response.status).toBe(400);
	});
});
