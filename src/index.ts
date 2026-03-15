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

	constructor({ server, value }: { server: string, value: string }) {
		const { key, devices } = JSON.parse(value) as { key: string, devices: Record<string, string> };
		this.server = server;
		this.key = key;
		this.devices = devices;
	}

	toSaveString() {
		return JSON.stringify({
			key: this.key,
			devices: this.devices
		});
	}
}


async function putDevice({ clinicServer, clinicKey, deviceToken, accountId }: { clinicServer: string, clinicKey: string, deviceToken: string, accountId: string }): Promise<true | string> {
	for (const arg of [clinicServer, clinicKey, deviceToken, accountId]) {
		if (arg == "" || arg == null) return "missing parameters";
	}

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value != null) {
		const clinic = new ClinicServer({ server: clinicServer, value });
		if (clinic.key == clinicKey) {
			// add device token only if not already present
			if (clinic.devices[deviceToken]) return true;
			clinic.devices[deviceToken] = accountId;
			await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
			return true
		} else return "clinic key does not match";
	} else {
		const devices: Record<string, string> = {};
		devices[deviceToken] = accountId;
		const clinic = new ClinicServer({ server: clinicServer, value: JSON.stringify({ key: clinicKey, devices }) });
		await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
		return true;
	}
}

async function replaceToken({ clinicServer, clinicKey, oldToken, newToken }: { clinicServer: string, clinicKey: string, oldToken: string, newToken: string }): Promise<true | string> {
	for (const arg of [clinicServer, clinicKey, oldToken, newToken]) {
		if (arg == "" || arg == null) return "missing parameters";
	}

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value == null) return "clinic not found";

	const clinic = new ClinicServer({ server: clinicServer, value });
	if (clinic.key != clinicKey) return "clinic key does not match";

	if (clinic.devices[newToken] != null) return "new token already in use";
	if (clinic.devices[oldToken] == null) return "old token not found";


	clinic.devices[newToken] = clinic.devices[oldToken];
	delete clinic.devices[oldToken];
	await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
	return true

}

async function sendFCM({ oauth, token, data }: { oauth: string, token: string, data: Object }): Promise<Response> {
	const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT) as GoogleKey;

	return await fetch(`https://fcm.googleapis.com/v1/projects/${serviceAccount.project_id}/messages:send`, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"Authorization": `Bearer ${oauth}`
		},
		body: JSON.stringify({
			message: { token, data }
		})
	});
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

async function pushData({ clinicServer, clinicKey, accountId, data }: { clinicServer: string, clinicKey: string, accountId: string, data: Object }): Promise<true | string> {
	for (const arg of [clinicServer, clinicKey, accountId, data]) {
		if (arg == "" || arg == null) return "missing parameters";
	}

	clinicKey = await hashKey(clinicKey);

	const value = await env.apexo_notifications_relay.get(clinicServer);
	if (value == null) return "clinic not found";

	const clinic = new ClinicServer({ server: clinicServer, value });

	const IDs = Object.values(clinic.devices);
	const tokens = Object.keys(clinic.devices);

	const IDsPresent = IDs.filter((id) => id == accountId);
	if (IDsPresent.length == 0) return "account not found: " + accountId;

	if (clinic.key != clinicKey) return "clinic key does not match";

	// generate FCM access token
	const oauth = await getFCMAuth();

	const failedTokens: string[] = [];

	const requests: [string, Promise<Response>][] = [];

	for (let index = 0; index < IDs.length; index++) {
		const id = IDs[index];
		const token = tokens[index];

		if (id == accountId) {
			// send data using the token
			// if the sending fails due to token being invalid
			// mark it for removal when we're out of this loop
			requests.push([token, sendFCM({ oauth, token, data })]);
		}
	}

	const responses = await Promise.all(requests.map((request) => request[1]));

	for (let index = 0; index < responses.length; index++) {
		const response = responses[index];
		if (response.status == 404) {
			failedTokens.push(requests[index][0]);
		}
		else if (response.status != 200) {
			return await response.text();
		}
	}

	if (failedTokens.length > 0) {
		// remove tokens that failed
		for (const token of failedTokens) {
			delete clinic.devices[token];
		}
		// update the clinic
		await env.apexo_notifications_relay.put(clinicServer, clinic.toSaveString());
	}

	return true;
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
			const { clinicServer, clinicKey, deviceToken, accountId } = JSON.parse(body) as { clinicServer: string, clinicKey: string, deviceToken: string, accountId: string };
			const result = await putDevice({ clinicServer, clinicKey, deviceToken, accountId });
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
			const { clinicServer, clinicKey, accountId, data } = JSON.parse(body) as { clinicServer: string, clinicKey: string, accountId: string, data: Object };
			const result = await pushData({ clinicServer, clinicKey, accountId, data });
			if (typeof result == "string") return corsRes(result, 400);
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
