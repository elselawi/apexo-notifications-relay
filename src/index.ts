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


class ClinicServer {
	server: string;
	key: string;
	devices: Record<string, string>; // where the key is the FCM token and the value is the account ID
	// The platform each token runs on, so a push can be shaped per device.
	// Absent for devices registered before this was recorded.
	platforms: Record<string, string>;

	constructor({ server, value }: { server: string, value: string }) {
		const { key, devices, platforms } = JSON.parse(value) as { key: string, devices: Record<string, string>, platforms?: Record<string, string> };
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

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value != null) {
		const clinic = new ClinicServer({ server: clinicServer, value });
		if (clinic.key == clinicKey) {
			// add or update device token, and the platform that shapes its pushes
			const unchanged = clinic.devices[deviceToken] === accountId &&
				(platform == null || platform == "" || clinic.platforms[deviceToken] === platform);
			if (unchanged) return true;
			clinic.devices[deviceToken] = accountId;
			if (platform != null && platform != "") clinic.platforms[deviceToken] = platform;
			await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
			return true
		} else return "clinic key does not match";
	} else {
		const devices: Record<string, string> = {};
		devices[deviceToken] = accountId;
		const platforms: Record<string, string> = {};
		if (platform != null && platform != "") platforms[deviceToken] = platform;
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
	if (clinic.devices[oldToken] == null) return "old token not found";


	clinic.devices[newToken] = clinic.devices[oldToken];
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

	// Find all tokens that belong to the target accountIds
	const targetTokens = Object.entries(clinic.devices)
		.filter(([_, accountId]) => accountIds.includes(accountId))
		.map(([token]) => token);

	if (targetTokens.length == 0) return { ok: false, status: 400, message: "none of the account ids were found to have FCM tokens: " + accountIds };

	// generate FCM access token
	const oauth = await getFCMAuth();

	const failedTokens: string[] = [];

	const requests: [string, Promise<Response>][] = targetTokens.map((token) => {
		// Only iOS gets an alert, so the system shows the notice even when the
		// app is not running. macOS keeps the silent push (it only notifies
		// while running), and tokens registered before the platform was
		// recorded keep today's behaviour.
		const tokenAlert = clinic.platforms[token] == "ios" ? alert : undefined;
		return [token, sendFCM({ oauth, token, data, alert: tokenAlert })];
	});

	const responses: Response[] = [];

	// Process in chunks of 40 to avoid hitting Cloudflare's 50 concurrent fetch limit
	for (let i = 0; i < requests.length; i += 40) {
		const chunk = requests.slice(i, i + 40);
		const chunkResponses = await Promise.all(chunk.map((req) => req[1]));
		responses.push(...chunkResponses);
	}

	let retryableError: string | null = null;
	let permanentError: string | null = null;
	for (let index = 0; index < responses.length; index++) {
		const response = responses[index];
		const status = response.status;

		if (status == 404) {
			// Unregistered/invalid token: remove it below; not a failure for the caller.
			failedTokens.push(requests[index][0]);
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

	if (failedTokens.length > 0) {
		// Re-fetch the clinic to minimize race conditions with concurrent device registrations
		// since the FCM network requests take time and might overlap with a user logging in.
		const currentValue = await env.apexo_notifications_relay.get(clinicServer);
		if (currentValue != null) {
			const currentClinic = new ClinicServer({ server: clinicServer, value: currentValue });
			let clinicModified = false;

			// remove tokens that failed
			for (const token of failedTokens) {
				if (currentClinic.devices[token] != null) {
					delete currentClinic.devices[token];
					delete currentClinic.platforms[token];
					clinicModified = true;
				}
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
