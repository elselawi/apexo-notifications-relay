import { env, fetchMock } from 'cloudflare:test';
import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import worker, { resetWnsTokenCacheForTests } from '../src/index';

// A correctly-typed `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

const CLINIC_SERVER = 'clinic-1';
const CLINIC_KEY = 'clinic-secret-key';
const ACCOUNT_ID = 'account-1';
const DEVICE_TOKEN = 'device-token-1';
const FCM_PROJECT = 'test-project';
const FCM_ORIGIN = 'https://fcm.googleapis.com';
const OAUTH_ORIGIN = 'https://oauth2.googleapis.com';
const WNS_CHANNEL = 'https://wns.notify.windows.com/channel-1';
const WNS_CHANNEL_2 = 'https://wns.notify.windows.com/channel-2';
const WNS_CHANNEL_3 = 'https://wns.notify.windows.com/channel-3';
const WNS_ORIGIN = 'https://wns.notify.windows.com';
const ENTRA_ORIGIN = 'https://login.microsoftonline.com';
const ENTRA_PATH = '/test-tenant/oauth2/v2.0/token';
const ATTACKER_ORIGIN = 'https://attacker.example';
const WNS_ACCESS_TOKEN = 'test-wns-access-token';

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

function mockWnsAccessToken(
	onRequest?: (body: string) => void,
	expiresIn = 3600
) {
	fetchMock
		.get(ENTRA_ORIGIN)
		.intercept({ method: 'POST', path: ENTRA_PATH })
		.reply((opts) => {
			onRequest?.(typeof opts.body == 'string' ? opts.body : '');
			return {
				statusCode: 200,
				data: JSON.stringify({
					access_token: WNS_ACCESS_TOKEN,
					expires_in: expiresIn,
				}),
			};
		});
}

async function putDeviceViaApi(
	token = DEVICE_TOKEN,
	platform?: string,
	accountId = ACCOUNT_ID
) {
	const request = new IncomingRequest('http://example.com/put-device', {
		method: 'POST',
		body: JSON.stringify({
			clinicServer: CLINIC_SERVER,
			clinicKey: CLINIC_KEY,
			deviceToken: token,
			accountId,
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

/// Answers WNS, and returns the requests it was given.
function captureWns(path = '/channel-1') {
	const requests: { headers: Headers; body: string }[] = [];
	fetchMock
		.get(WNS_ORIGIN)
		.intercept({ method: 'POST', path })
		.reply((opts) => {
			requests.push({
				headers: new Headers(opts.headers as Record<string, string>),
				body: typeof opts.body == 'string' ? opts.body : '',
			});
			return { statusCode: 200, data: '' };
		})
		.persist();
	return requests;
}

async function storedClinic() {
	const stored = await env.apexo_notifications_relay.get(CLINIC_SERVER);
	return JSON.parse(stored!) as {
		devices: Record<string, unknown>;
		platforms: Record<string, string>;
	};
}

// `cleanMocks()` exists at runtime on each per-origin mock client but is not
// exposed by the `Interceptable` type, so cast around it.
function cleanFetchMocks() {
	const cleanable = (origin: string) =>
		(fetchMock.get(origin) as unknown as { cleanMocks(): void }).cleanMocks();
	cleanable(FCM_ORIGIN);
	cleanable(OAUTH_ORIGIN);
	cleanable(ENTRA_ORIGIN);
	cleanable(WNS_ORIGIN);
	cleanable(ATTACKER_ORIGIN);
}

describe('Apexo Notifications Relay', () => {
	beforeAll(() => {
		fetchMock.activate();
		fetchMock.disableNetConnect();
	});

	beforeEach(() => {
		cleanFetchMocks();
		resetWnsTokenCacheForTests();
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

	it('sends a raw WNS notification for a device registered with platform "wns"', async () => {
		const registered = await putDeviceViaApi(WNS_CHANNEL, 'wns');
		expect(registered.status).toBe(200);

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});

		mockWnsAccessToken();
		const wnsRequests = captureWns();

		const response = await push();
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		expect(wnsRequests).toHaveLength(1);
		const request = wnsRequests[0];
		expect(request.headers.get('authorization')).toBe(`Bearer ${WNS_ACCESS_TOKEN}`);
		expect(request.headers.get('content-type')).toBe('application/octet-stream');
		expect(request.headers.get('x-wns-type')).toBe('wns/raw');
		expect(request.headers.get('x-wns-ttl')).toBe('86400');
		expect(JSON.parse(request.body)).toEqual({ title: 'hello' });
	});

	it('registers a device without a platform as a legacy string FCM device', async () => {
		await putDeviceViaApi();

		const stored = await storedClinic();
		expect(stored.devices[DEVICE_TOKEN]).toBe(ACCOUNT_ID);

		mockOAuthToken();
		const bodies = captureFcm();

		expect((await push()).status).toBe(200);
		expect(bodies).toHaveLength(1);
		expect((bodies[0].message as Record<string, unknown>).token).toBe(DEVICE_TOKEN);
	});

	it('keeps a known iOS platform when the token is re-registered as "fcm"', async () => {
		await putDeviceViaApi(DEVICE_TOKEN, 'ios');
		expect((await putDeviceViaApi(DEVICE_TOKEN, 'fcm')).status).toBe(200);

		const stored = await storedClinic();
		expect(stored.devices[DEVICE_TOKEN]).toBe(ACCOUNT_ID);
		expect(stored.platforms[DEVICE_TOKEN]).toBe('ios');

		mockOAuthToken();
		const bodies = captureFcm();
		expect((await push([ACCOUNT_ID], ALERT)).status).toBe(200);

		const message = bodies[0].message as Record<string, unknown>;
		expect((message.apns as Record<string, unknown>).headers).toEqual({
			'apns-push-type': 'alert',
			'apns-priority': '10',
		});
	});

	it('rejects an unknown platform with HTTP 400', async () => {
		const response = await putDeviceViaApi(DEVICE_TOKEN, 'windows');
		expect(response.status).toBe(400);
		expect(await response.text()).toBe('invalid platform');
		expect(await env.apexo_notifications_relay.get(CLINIC_SERVER)).toBeNull();
	});

	it('rejects a WNS registration whose token is not an https *.notify.windows.com URL', async () => {
		for (const token of [
			'https://example.com/channel',
			'https://notify.windows.com.attacker.example/channel',
			'https://wns.notify.windows.com.attacker.example/channel',
			'http://wns.notify.windows.com/channel',
			'https://notify.windows.com/channel',
			'https://user:pass@wns.notify.windows.com/channel',
			'https://user@wns.notify.windows.com/channel',
			'https://wns.notify.windows.com:8443/channel',
			'not-a-url',
		]) {
			const response = await putDeviceViaApi(token, 'wns');
			expect(response.status).toBe(400);
			expect(await response.text()).toBe('invalid WNS channel url');
		}
		expect(await env.apexo_notifications_relay.get(CLINIC_SERVER)).toBeNull();
	});

	it('does not send to an invalid WNS channel read from storage', async () => {
		const invalidChannel = 'https://notify.windows.com.attacker.example/channel';
		await env.apexo_notifications_relay.put(
			CLINIC_SERVER,
			JSON.stringify({
				key: await hashKey(CLINIC_KEY),
				devices: {
					[invalidChannel]: { accountId: ACCOUNT_ID, platform: 'wns' },
				},
				platforms: {},
			})
		);

		// No Entra interceptor: a token request or channel fetch would fail the
		// disabled network with a 503 instead of the expected permanent 400.
		const response = await push();
		expect(response.status).toBe(400);
		expect(await response.text()).toBe('invalid stored WNS channel url');

		const stored = await storedClinic();
		expect(stored.devices[invalidChannel]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('does not follow a WNS redirect or forward the bearer token', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		mockWnsAccessToken();

		let reachedAttacker = false;
		fetchMock
			.get(ATTACKER_ORIGIN)
			.intercept({ method: 'POST', path: '/steal' })
			.reply(() => {
				reachedAttacker = true;
				return { statusCode: 200, data: '' };
			});
		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(302, '', {
				headers: { location: `${ATTACKER_ORIGIN}/steal` },
			});

		const response = await push();
		expect(response.status).toBe(503);
		expect(reachedAttacker).toBe(false);
		expect((await storedClinic()).devices[WNS_CHANNEL]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('does not follow an Entra redirect with the client secret', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');

		let reachedAttacker = false;
		fetchMock
			.get(ATTACKER_ORIGIN)
			.intercept({ method: 'POST', path: '/steal' })
			.reply(() => {
				reachedAttacker = true;
				return {
					statusCode: 200,
					data: JSON.stringify({
						access_token: WNS_ACCESS_TOKEN,
						expires_in: 3600,
					}),
				};
			});
		fetchMock
			.get(ENTRA_ORIGIN)
			.intercept({ method: 'POST', path: ENTRA_PATH })
			.reply(302, '', {
				headers: { location: `${ATTACKER_ORIGIN}/steal` },
			});

		expect((await push()).status).toBe(503);
		expect(reachedAttacker).toBe(false);
	});

	it('removes a WNS device when WNS responds with 410', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		mockWnsAccessToken();

		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(410, '');

		const response = await push();
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toBeUndefined();
	});

	it('keeps a device re-registered while its WNS 410 is in flight', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		await putDeviceViaApi(WNS_CHANNEL_2, 'wns');
		mockWnsAccessToken();

		let dispatched: () => void = () => {};
		const firstSendStarted = new Promise<void>((resolve) => {
			dispatched = resolve;
		});

		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(() => {
				dispatched();
				return { statusCode: 410, data: '' };
			});
		// Keeps the push in flight while the re-registration completes.
		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-2' })
			.reply(200, '')
			.delay(250);

		const pending = push();
		await firstSendStarted;

		// The initial clinic read is done; re-register the same channel for
		// another account before cleanup runs.
		expect((await putDeviceViaApi(WNS_CHANNEL, 'wns', 'account-2')).status).toBe(200);

		const response = await pending;
		expect(response.status).toBe(200);
		expect(await response.text()).toBe('ok');

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toEqual({
			accountId: 'account-2',
			platform: 'wns',
		});
		expect(stored.devices[WNS_CHANNEL_2]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('keeps a channel refreshed with /replace-token while its old WNS 410 is in flight', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		await putDeviceViaApi(WNS_CHANNEL_2, 'wns');
		mockWnsAccessToken();

		let dispatched: () => void = () => {};
		const firstSendStarted = new Promise<void>((resolve) => {
			dispatched = resolve;
		});

		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(() => {
				dispatched();
				return { statusCode: 410, data: '' };
			});
		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-2' })
			.reply(200, '')
			.delay(250);

		const pending = push();
		await firstSendStarted;

		const replaced = await worker.fetch(
			new IncomingRequest('http://example.com/replace-token', {
				method: 'POST',
				body: JSON.stringify({
					clinicServer: CLINIC_SERVER,
					clinicKey: CLINIC_KEY,
					oldToken: WNS_CHANNEL,
					newToken: WNS_CHANNEL_3,
				}),
			})
		);
		expect(replaced.status).toBe(200);

		expect((await pending).status).toBe(200);

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toBeUndefined();
		expect(stored.devices[WNS_CHANNEL_3]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
		expect(stored.devices[WNS_CHANNEL_2]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('maps WNS 500 to HTTP 503 (retryable)', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		mockWnsAccessToken();

		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(500, 'Internal Server Error');

		expect((await push()).status).toBe(503);

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('maps an Entra token failure to HTTP 503 without removing the device', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');

		fetchMock
			.get(ENTRA_ORIGIN)
			.intercept({ method: 'POST', path: ENTRA_PATH })
			.reply(400, JSON.stringify({ error: 'invalid_client' }));

		expect((await push()).status).toBe(503);

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});
	});

	it('copies a WNS device object to the new token on /replace-token', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');

		const rejected = await worker.fetch(
			new IncomingRequest('http://example.com/replace-token', {
				method: 'POST',
				body: JSON.stringify({
					clinicServer: CLINIC_SERVER,
					clinicKey: CLINIC_KEY,
					oldToken: WNS_CHANNEL,
					newToken: 'https://example.com/channel',
				}),
			})
		);
		expect(rejected.status).toBe(400);
		expect(await rejected.text()).toBe('invalid WNS channel url');
		expect((await storedClinic()).devices[WNS_CHANNEL]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});

		const replaced = await worker.fetch(
			new IncomingRequest('http://example.com/replace-token', {
				method: 'POST',
				body: JSON.stringify({
					clinicServer: CLINIC_SERVER,
					clinicKey: CLINIC_KEY,
					oldToken: WNS_CHANNEL,
					newToken: WNS_CHANNEL_2,
				}),
			})
		);
		expect(replaced.status).toBe(200);

		const stored = await storedClinic();
		expect(stored.devices[WNS_CHANNEL]).toBeUndefined();
		expect(stored.devices[WNS_CHANNEL_2]).toEqual({
			accountId: ACCOUNT_ID,
			platform: 'wns',
		});

		mockWnsAccessToken();
		const wnsRequests = captureWns('/channel-2');
		expect((await push()).status).toBe(200);
		expect(wnsRequests).toHaveLength(1);
	});

	it('fetches the Entra token once and reuses it for concurrent and later sends', async () => {
		await putDeviceViaApi(WNS_CHANNEL, 'wns');

		let tokenRequests = 0;
		let tokenBody = '';
		mockWnsAccessToken((body) => {
			tokenRequests += 1;
			tokenBody = body;
		});
		captureWns();

		const concurrent = await Promise.all([push(), push()]);
		expect(concurrent.map((response) => response.status)).toEqual([200, 200]);
		expect(tokenRequests).toBe(1);

		expect((await push()).status).toBe(200);
		expect(tokenRequests).toBe(1);

		const params = new URLSearchParams(tokenBody);
		expect(params.get('grant_type')).toBe('client_credentials');
		expect(params.get('client_id')).toBe('test-client-id');
		expect(params.get('client_secret')).toBe('test-client-secret');
		expect(params.get('scope')).toBe('https://wns.windows.com/.default');
	});

	it('returns 400 when a mixed push has both permanent and retryable failures', async () => {
		await putDeviceViaApi(DEVICE_TOKEN);
		await putDeviceViaApi(WNS_CHANNEL, 'wns');
		mockOAuthToken();
		mockWnsAccessToken();

		fetchMock
			.get(FCM_ORIGIN)
			.intercept({
				method: 'POST',
				path: `/v1/projects/${FCM_PROJECT}/messages:send`,
			})
			.reply(400, 'Bad Request');
		fetchMock
			.get(WNS_ORIGIN)
			.intercept({ method: 'POST', path: '/channel-1' })
			.reply(500, 'WNS server error');

		const response = await push();
		expect(response.status).toBe(400);
		expect(await response.text()).toBe('Bad Request');
	});

	it('does not rewrite the clinic record for an identical WNS registration', async () => {
		await env.apexo_notifications_relay.put(
			CLINIC_SERVER,
			JSON.stringify({
				key: await hashKey(CLINIC_KEY),
				devices: {
					[WNS_CHANNEL]: { accountId: ACCOUNT_ID, platform: 'wns' },
				},
				platforms: {},
				marker: 'preserved',
			})
		);

		expect((await putDeviceViaApi(WNS_CHANNEL, 'wns')).status).toBe(200);

		const stored = JSON.parse(
			(await env.apexo_notifications_relay.get(CLINIC_SERVER))!
		) as { marker?: string };
		expect(stored.marker).toBe('preserved');
	});
});
