import { env } from "cloudflare:workers";
import GoogleAuth, { GoogleKey } from 'cloudflare-workers-and-google-oauth';

async function hashKey(key: string): Promise<string> {
	const msgUint8 = new TextEncoder().encode(key);
	const hashBuffer = await crypto.subtle.digest("SHA-256", msgUint8);
	const hashArray = Array.from(new Uint8Array(hashBuffer));
	const hashHex = hashArray
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");

	return hashHex;
}


/// How a device receives pushes.
/// FCM devices keep the legacy string value (the account ID); WNS devices are
/// stored as an object so one clinic can mix both transports.
type Device = string | { accountId: string; platform: "wns" };

type ClinicData = {
	key: string;
	devices: Record<string, Device>;
	// The OS platform each FCM token runs on, so a push can be shaped per
	// device. Absent for devices registered before this was recorded.
	platforms?: Record<string, string>;
};

type DeviceTransport = "fcm" | "wns";

function normalizeDevice(value: Device): { accountId: string; platform: DeviceTransport } {
	if (typeof value == "string") return { accountId: value, platform: "fcm" };
	return { accountId: value.accountId, platform: value.platform };
}

const osPlatforms = new Set(["ios", "macos", "android", "web"]);

/// The OS platform a registration reports, when it reports one.
/// `fcm` is the legacy/default transport value and never changes the OS map.
function osPlatformFor(platform: string | undefined): string | null {
	return platform != null && osPlatforms.has(platform) ? platform : null;
}

function isKnownPlatform(platform: string | undefined): boolean {
	return platform == null || platform == "" || platform == "fcm" || platform == "wns" || osPlatforms.has(platform);
}

/// WNS only accepts channel URIs hosted by the Windows Notification Service.
/// Lookalike suffixes, embedded credentials, and non-default ports are rejected
/// so the Entra bearer token can only ever be sent to a Windows channel host.
function isWnsChannelUri(token: string): boolean {
	try {
		const url = new URL(token);
		return url.protocol == "https:" &&
			url.hostname.endsWith(".notify.windows.com") &&
			url.username == "" &&
			url.password == "" &&
			url.port == "";
	} catch {
		return false;
	}
}

function sameDevice(a: Device | undefined, b: Device): boolean {
	if (typeof a == "string" || typeof b == "string") return a === b;
	return a != null && a.accountId === b.accountId && a.platform === b.platform;
}

class ClinicServer {
	server: string;
	key: string;
	devices: Record<string, Device>; // the key is the device token; string values are FCM account IDs
	platforms: Record<string, string>;

	constructor({ server, value }: { server: string, value: string }) {
		const { key, devices, platforms } = JSON.parse(value) as ClinicData;
		this.server = server;
		this.key = key;
		this.devices = devices;
		this.platforms = platforms ?? {};
	}

	toSaveString() {
		return JSON.stringify({
			key: this.key,
			devices: this.devices,
			platforms: this.platforms
		});
	}
}


async function putDevice({ clinicServer, clinicKey, deviceToken, accountId, platform }: { clinicServer: string, clinicKey: string, deviceToken: string, accountId: string, platform?: string }): Promise<true | string> {
	for (const arg of [clinicServer, clinicKey, deviceToken, accountId]) {
		if (arg == "" || arg == null) return "missing parameters";
	}

	if (!isKnownPlatform(platform)) return "invalid platform";

	const wns = platform == "wns";
	if (wns && !isWnsChannelUri(deviceToken)) return "invalid WNS channel url";

	clinicKey = await hashKey(clinicKey);

	const osPlatform = osPlatformFor(platform);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value != null) {
		const clinic = new ClinicServer({ server: clinicServer, value });
		if (clinic.key == clinicKey) {
			const current = clinic.devices[deviceToken];

			// Updating the same token/account must not rewrite the clinic record.
			const unchanged = wns
				? typeof current == "object" && current != null && current.accountId == accountId && current.platform == "wns"
				: current === accountId && (osPlatform == null || clinic.platforms[deviceToken] === osPlatform);
			if (unchanged) return true;

			if (wns) {
				clinic.devices[deviceToken] = { accountId, platform: "wns" };
			} else {
				clinic.devices[deviceToken] = accountId;
				// Legacy/default registrations must not disturb a known OS platform.
				if (osPlatform != null) clinic.platforms[deviceToken] = osPlatform;
			}
			await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
			return true
		} else return "clinic key does not match";
	} else {
		const devices: Record<string, Device> = {};
		devices[deviceToken] = wns ? { accountId, platform: "wns" } : accountId;
		const platforms: Record<string, string> = {};
		if (osPlatform != null) platforms[deviceToken] = osPlatform;
		const clinic = new ClinicServer({ server: clinicServer, value: JSON.stringify({ key: clinicKey, devices, platforms }) });
		await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
		return true;
	}
}

async function replaceToken({ clinicServer, clinicKey, oldToken, newToken }: { clinicServer: string, clinicKey: string, oldToken: string, newToken: string }): Promise<true | string> {
	for (const arg of [clinicServer, clinicKey, oldToken, newToken]) {
		if (arg == "" || arg == null) return "missing parameters";
	}

	if (oldToken === newToken) return true;

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value == null) return "clinic not found";

	const clinic = new ClinicServer({ server: clinicServer, value });
	if (clinic.key != clinicKey) return "clinic key does not match";

	if (clinic.devices[newToken] != null) return "new token already in use";
	const device = clinic.devices[oldToken];
	if (device == null) return "old token not found";

	// A WNS push sends the Entra bearer token to this URI, so a WNS device may
	// only move to another Windows Notification Service channel.
	if (normalizeDevice(device).platform == "wns" && !isWnsChannelUri(newToken)) return "invalid WNS channel url";

	clinic.devices[newToken] = device;
	delete clinic.devices[oldToken];
	const platform = clinic.platforms[oldToken];
	if (platform != null) {
		clinic.platforms[newToken] = platform;
		delete clinic.platforms[oldToken];
	}
	await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
	return true

}

type Alert = { title: string, body: string };

/// The FCM message for one device.
///
/// An iOS device that has an [alert] gets an alert push, so iOS displays the
/// notice itself — including when the app was never running. Every other
/// device gets the data-only push the app renders.
function fcmMessage(token: string, data: Object, alert?: Alert) {
	if (alert != null) {
		return {
			token,
			data,
			apns: {
				headers: {
					"apns-push-type": "alert",
					"apns-priority": "10"
				},
				payload: {
					aps: {
						alert: { title: alert.title, body: alert.body },
						sound: "default",
						badge: 1
					}
				}
			}
		};
	}

	return {
		token,
		data,
		android: {
			// Data-only messages default to normal priority, which Doze and
			// App Standby may defer until the app is next opened. High
			// priority is delivered while the app is backgrounded.
			priority: "high"
		},
		apns: {
			headers: {
				"apns-push-type": "background",
				"apns-priority": "5"
			},
			payload: {
				aps: {
					"content-available": 1
				}
			}
		}
	};
}

async function sendFCM({ oauth, token, data, alert }: { oauth: string, token: string, data: Object, alert?: Alert }): Promise<Response> {
	const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT) as GoogleKey;

	try {
		return await fetch(`https://fcm.googleapis.com/v1/projects/${serviceAccount.project_id}/messages:send`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				"Authorization": `Bearer ${oauth}`
			},
			body: JSON.stringify({
				message: fcmMessage(token, data, alert)
			})
		});
	} catch (error) {
		// Network/abort failures are transient: surface them as retryable so the
		// caller returns HTTP 503 and the client retries instead of dropping the push.
		return new Response("FCM request failed", { status: 503 });
	}
}

async function getFCMAuth(): Promise<string> {
	const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT) as GoogleKey;

	const oauth = new GoogleAuth(
		serviceAccount,
		["https://www.googleapis.com/auth/firebase.messaging"]
	);

	const res = await oauth.getGoogleAuthToken();

	if (typeof res != "string") throw new Error("error getting FCM access token");
	return res;
}

const WNS_SCOPE = "https://wns.windows.com/.default";

type WnsTokenCache = { credentials: string; token: string; expiresAt: number };

// Per-isolate cache: a new isolate authenticates once, and concurrent pushes
// share a single in-flight request.
let wnsTokenCache: WnsTokenCache | null = null;
let wnsTokenRequest: { credentials: string; promise: Promise<string> } | null = null;

function wnsCredentials(): string {
	return `${env.WNS_TENANT_ID}\u0000${env.WNS_CLIENT_ID}\u0000${env.WNS_CLIENT_SECRET}`;
}

async function requestWnsAccessToken(): Promise<{ token: string; expiresIn: number }> {
	const response = await fetch(`https://login.microsoftonline.com/${env.WNS_TENANT_ID}/oauth2/v2.0/token`, {
		method: "POST",
		// Never follow a redirect with the client secret in the body.
		redirect: "manual",
		headers: {
			"Content-Type": "application/x-www-form-urlencoded"
		},
		body: new URLSearchParams({
			grant_type: "client_credentials",
			client_id: env.WNS_CLIENT_ID,
			client_secret: env.WNS_CLIENT_SECRET,
			scope: WNS_SCOPE
		})
	});

	if (!response.ok) throw new Error("error getting WNS access token");

	const body = await response.json() as { access_token?: unknown, expires_in?: unknown };
	if (typeof body.access_token != "string" || body.access_token == "" || typeof body.expires_in != "number" || !Number.isFinite(body.expires_in)) {
		throw new Error("error getting WNS access token");
	}

	return { token: body.access_token, expiresIn: body.expires_in };
}

async function getWnsAccessToken(): Promise<string> {
	const credentials = wnsCredentials();
	const now = Date.now();

	if (wnsTokenCache != null && wnsTokenCache.credentials == credentials && wnsTokenCache.expiresAt > now) {
		return wnsTokenCache.token;
	}

	if (wnsTokenRequest != null && wnsTokenRequest.credentials == credentials) {
		return wnsTokenRequest.promise;
	}

	const promise = requestWnsAccessToken()
		.then(({ token, expiresIn }) => {
			// Refresh a minute early so a cached token cannot expire mid-push.
			wnsTokenCache = { credentials, token, expiresAt: Date.now() + Math.max(0, expiresIn - 60) * 1000 };
			return token;
		})
		.finally(() => {
			if (wnsTokenRequest != null && wnsTokenRequest.promise === promise) wnsTokenRequest = null;
		});

	wnsTokenRequest = { credentials, promise };
	return promise;
}

/// Test-only: clears the per-isolate Entra token cache so tests are order-independent.
export function resetWnsTokenCacheForTests() {
	wnsTokenCache = null;
	wnsTokenRequest = null;
}

async function sendWns({ accessToken, token, data }: { accessToken: string, token: string, data: Object }): Promise<Response> {
	// Re-check immediately before sending: KV data may predate these rules.
	if (!isWnsChannelUri(token)) {
		return new Response("invalid stored WNS channel url", { status: 400 });
	}

	try {
		return await fetch(token, {
			method: "POST",
			// Never follow a redirect, which could forward the bearer token.
			redirect: "manual",
			headers: {
				"Authorization": `Bearer ${accessToken}`,
				"Content-Type": "application/octet-stream",
				"X-WNS-Type": "wns/raw",
				"X-WNS-TTL": "86400"
			},
			body: JSON.stringify(data)
		});
	} catch (error) {
		// Network/abort failures are transient: surface them as retryable so the
		// caller returns HTTP 503 and the client retries instead of dropping the push.
		return new Response("WNS request failed", { status: 503 });
	}
}

type PushTarget = {
	token: string;
	device: Device;
	accountId: string;
	platform: DeviceTransport;
	osPlatform: string | undefined;
};

type PendingSend = {
	token: string;
	device: Device;
	osPlatform: string | undefined;
	platform: DeviceTransport;
	send: () => Promise<Response>;
};

type PushResult =
	| { ok: true }
	| { ok: false; status: 400 | 503; message: string };

async function pushData({ clinicServer, clinicKey, accountIds, data, alert }: { clinicServer: string, clinicKey: string, accountIds: Array<string>, data: Object, alert?: Alert }): Promise<PushResult> {
	for (const arg of [clinicServer, clinicKey, accountIds, data]) {
		if (arg == "" || arg == null) return { ok: false, status: 400, message: "missing parameters" };
	}
	if (alert != null && (typeof alert.title != "string" || alert.title == "" || typeof alert.body != "string" || alert.body == "")) {
		return { ok: false, status: 400, message: "invalid alert" };
	}

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value == null) return { ok: false, status: 400, message: "clinic not found" };

	const clinic = new ClinicServer({ server: clinicServer, value });
	if (clinic.key != clinicKey) return { ok: false, status: 400, message: "clinic key does not match" };

	// Find all devices that belong to the target accountIds
	const targets: PushTarget[] = Object.entries(clinic.devices)
		.map(([token, device]) => ({
			token,
			device,
			...normalizeDevice(device),
			osPlatform: clinic.platforms[token]
		}))
		.filter((target) => accountIds.includes(target.accountId));

	if (targets.length == 0) return { ok: false, status: 400, message: "none of the account ids were found to have device tokens: " + accountIds };

	const fcmTargets = targets.filter((target) => target.platform == "fcm");
	const wnsTargets = targets.filter((target) => target.platform == "wns");

	const requests: PendingSend[] = [];

	if (fcmTargets.length > 0) {
		// generate FCM access token only when there is something to send over FCM
		const oauth = await getFCMAuth();

		for (const target of fcmTargets) {
			// Only iOS gets an alert, so the system shows the notice even when the
			// app is not running. macOS keeps the silent push (it only notifies
			// while running), and tokens registered before the platform was
			// recorded keep today's behaviour.
			const tokenAlert = clinic.platforms[target.token] == "ios" ? alert : undefined;
			requests.push({
				token: target.token,
				device: target.device,
				osPlatform: target.osPlatform,
				platform: "fcm",
				send: () => sendFCM({ oauth, token: target.token, data, alert: tokenAlert })
			});
		}
	}

	// Stored data may be malformed or predate the current WNS URI rules: re-check
	// immediately before sending and never fetch a URI that fails validation.
	for (const target of wnsTargets) {
		if (isWnsChannelUri(target.token)) continue;
		requests.push({
			token: target.token,
			device: target.device,
			osPlatform: target.osPlatform,
			platform: "wns",
			send: () => Promise.resolve(new Response("invalid stored WNS channel url", { status: 400 }))
		});
	}

	const sendableWnsTargets = wnsTargets.filter((target) => isWnsChannelUri(target.token));

	if (sendableWnsTargets.length > 0) {
		let accessToken: string | null = null;
		try {
			accessToken = await getWnsAccessToken();
		} catch (error) {
			// Credential/network problems are retryable and must not remove devices.
			accessToken = null;
		}

		for (const target of sendableWnsTargets) {
			const bearer = accessToken;
			requests.push({
				token: target.token,
				device: target.device,
				osPlatform: target.osPlatform,
				platform: "wns",
				send: bearer == null
					? () => Promise.resolve(new Response("WNS access token request failed", { status: 503 }))
					: () => sendWns({ accessToken: bearer, token: target.token, data })
			});
		}
	}

	const failedDevices: PendingSend[] = [];

	let retryableError: string | null = null;
	let permanentError: string | null = null;

	// Process in chunks of 40 to avoid hitting Cloudflare's concurrent fetch
	// limit. Each chunk only starts its requests when awaited, so both
	// transports stay within the limit.
	for (let i = 0; i < requests.length; i += 40) {
		const chunk = requests.slice(i, i + 40);
		const responses = await Promise.all(chunk.map((request) => request.send()));

		for (let index = 0; index < responses.length; index++) {
			const response = responses[index];
			const request = chunk[index];
			const status = response.status;

			if (status == 404 || (request.platform == "wns" && status == 410)) {
				// Unregistered/invalid token: remove it below; not a failure for the caller.
				failedDevices.push(request);
			}
			else if (status >= 200 && status < 300) {
				// Success: nothing to do.
			}
			else if (status == 401 || status == 403 || status == 429 || status >= 500) {
				// Relay-side credential/rate-limit/server problem or transient 5xx: retryable.
				if (retryableError == null) retryableError = await response.text();
			}
			else if (status >= 400 && status < 500) {
				// Other 4xx: permanent client error.
				if (permanentError == null) permanentError = await response.text();
			}
			else {
				// Unexpected (e.g. 3xx): treat as transient so the client retries.
				if (retryableError == null) retryableError = await response.text();
			}
		}
	}

	if (failedDevices.length > 0) {
		// Re-fetch the clinic to minimize race conditions with concurrent device registrations
		// since the network requests take time and might overlap with a user logging in.
		const currentValue = await env.apexo_notifications_relay.get(clinicServer);
		if (currentValue != null) {
			const currentClinic = new ClinicServer({ server: clinicServer, value: currentValue });
			let clinicModified = false;

			// Remove only the exact device the send failed for. A channel refreshed
			// or a device re-registered while the send was in flight must survive.
			for (const failed of failedDevices) {
				const current = currentClinic.devices[failed.token];
				if (current == null) continue;
				if (!sameDevice(current, failed.device) || currentClinic.platforms[failed.token] !== failed.osPlatform) continue;

				delete currentClinic.devices[failed.token];
				delete currentClinic.platforms[failed.token];
				clinicModified = true;
			}

			// update the clinic only if we actually deleted something
			if (clinicModified) {
				await env.apexo_notifications_relay.put(clinicServer, currentClinic.toSaveString());
			}
		}
	}

	// Prefer a permanent client error over a retryable one so the client stops
	// retrying a push that can never succeed for one of its targets.
	if (permanentError != null) return { ok: false, status: 400, message: permanentError };
	if (retryableError != null) return { ok: false, status: 503, message: retryableError };
	return { ok: true };
}


const corsHeaders = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, HEAD, OPTIONS',
	'Access-Control-Max-Age': '86400',
	'Access-Control-Allow-Headers': 'x-worker-key,Content-Type,x-custom-metadata,Content-MD5,x-amz-meta-fileid,x-amz-meta-account_id,x-amz-meta-clientid,x-amz-meta-file_id,x-amz-meta-opportunity_id,x-amz-meta-client_id,x-amz-meta-webhook,authorization',
	'Access-Control-Allow-Credentials': 'true',
	'Allow': 'GET, POST, PUT, DELETE, HEAD, OPTIONS'
};

function corsRes(res: string, status: number) {
	return new Response(res, { headers: corsHeaders, status });
}

export default {
	async fetch(request: Request) {
		const url = new URL(request.url);

		if (url.pathname == "/put-device" && request.method == "POST") {
			const body = await request.text();
			const { clinicServer, clinicKey, deviceToken, accountId, platform } = JSON.parse(body) as { clinicServer: string, clinicKey: string, deviceToken: string, accountId: string, platform?: string };
			const result = await putDevice({ clinicServer, clinicKey, deviceToken, accountId, platform });
			if (typeof result == "string") return corsRes(result, 400);
			return corsRes("ok", 200);
		}

		if (url.pathname == "/replace-token" && request.method == "POST") {
			const body = await request.text();
			const { clinicServer, clinicKey, oldToken, newToken } = JSON.parse(body) as { clinicServer: string, clinicKey: string, oldToken: string, newToken: string };
			const result = await replaceToken({ clinicServer, clinicKey, oldToken, newToken });
			if (typeof result == "string") return corsRes(result, 400);
			return corsRes("ok", 200);
		}

		if (url.pathname == "/push" && request.method == "POST") {
			const body = await request.text();
			const { clinicServer, clinicKey, accountIds, data, alert } = JSON.parse(body) as { clinicServer: string, clinicKey: string, accountIds: Array<string>, data: Object, alert?: Alert };
			const result = await pushData({ clinicServer, clinicKey, accountIds, data, alert });
			if (!result.ok) return corsRes(result.message, result.status);
			return corsRes("ok", 200);
		}

		if (url.pathname == "/health" && request.method == "GET") {
			return corsRes("ok", 200);
		}

		if (request.method === "OPTIONS") {
			return new Response("OK", {
				headers: corsHeaders
			});
		}

		return corsRes(`unknown path or method: ${url.pathname}`, 404);
	}
} satisfies ExportedHandler<Env>;
