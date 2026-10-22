# Apexo Notifications Relay

A relay to distribute notifications to devices using FCM and Windows Notification
Service (WNS), deployed on Cloudflare Workers

- registers a device (and clinic) on FCM or WNS
- replaces a device token when needed
- sends push notifications (data) to target devices

## Device registry

`POST /put-device` accepts an optional `platform`:

| `platform` | Stored device value | Delivery |
| --- | --- | --- |
| omitted, `""`, or `"fcm"` | `"<accountId>"` | FCM |
| `"ios"`, `"macos"`, `"android"`, or `"web"` | `"<accountId>"`, with the OS recorded in `platforms[token]` | FCM, shaped for the OS |
| `"wns"` | `{ "accountId": "…", "platform": "wns" }` | WNS |

Any other non-empty value is rejected with HTTP 400. Legacy string devices
(registered before platforms existed) are still read as FCM, and re-registering
a token with `"fcm"` or no platform never clears a previously recorded OS
platform. A `"wns"` token must be an `https://…notify.windows.com` channel URI
with no embedded credentials and no non-default port; lookalike hosts such as
`notify.windows.com.attacker.example` are rejected with HTTP 400.

`POST /replace-token` copies the stored value (string or WNS object) and the OS
platform entry to the new token. A WNS device can only move to another valid
`https://…notify.windows.com` channel URI.

## Push delivery

`POST /push` sends a data-only FCM message (no `notification` block) to every
FCM device token registered for the requested account IDs, and a raw WNS
notification to every Windows channel registered for them. For Apple devices,
each FCM message also carries an APNs block so the push is delivered as a
background update:

- `apns.headers["apns-push-type"] = "background"`
- `apns.headers["apns-priority"] = "5"`
- `apns.payload.aps["content-available"] = 1`

For Android devices, each message carries `android.priority = "high"` so the
data message is delivered while the app is backgrounded instead of being
deferred by Doze or App Standby. Web targets ignore both platform blocks.

### iOS alerts

A request may carry an `alert`:

```json
{
  "clinicServer": "…",
  "clinicKey": "…",
  "accountIds": ["…"],
  "data": { "payload": "{…}" },
  "alert": { "title": "New appointment", "body": "Ada Lovelace: appointment created" }
}
```

`/put-device` records the OS `platform` a token runs on (`ios`, `macos`, `android`,
`web`). Tokens recorded as `ios` receive an alert push instead of the silent
one, so iOS displays the notice itself — including when the app is not running:

- `apns.headers["apns-push-type"] = "alert"`
- `apns.headers["apns-priority"] = "10"`
- `apns.payload.aps` = `{ alert, sound: "default", badge: 1 }`

No `content-available` is sent with an alert, so the app is not woken and does
not post a second notification for the same event. Tapping the alert opens the
app with `data` attached, which is what the client navigates with.

Every other token — macOS, Android, Web, and any device registered before
`platform` existed — keeps the silent data push. The text in `alert` is rendered
by the sending client, so it is in the sender's language.

### Windows (WNS)

Devices registered with `platform: "wns"` receive a raw WNS notification. The
relay authenticates to Entra ID with the `WNS_TENANT_ID`, `WNS_CLIENT_ID`, and
`WNS_CLIENT_SECRET` secrets using the client-credentials flow, and caches the
access token in module scope until a minute before `expires_in`. Concurrent
pushes share one in-flight token request. The cache lives in the isolate, so a
new or evicted isolate authenticates again. Secrets and tokens are never logged.

Each channel URI receives:

- `Authorization: Bearer <Entra access token>`
- `Content-Type: application/octet-stream`
- `X-WNS-Type: wns/raw`
- `X-WNS-TTL: 86400`
- body: `JSON.stringify(data)` — the same `data` object sent to FCM

The Windows app receives the raw payload and renders its own notification. WNS
`404` and `410` responses remove the channel from the clinic and are not treated
as failures. Cleanup re-reads the clinic and removes only the exact device value
that failed, so a channel refreshed or a device re-registered while the push was
in flight survives. WNS `401`/`403`/`429`/`5xx` and Entra token/network errors
are retryable; any other WNS `4xx` is permanent.

Channel URIs are validated again immediately before each send. A malformed or
outdated stored URI is reported as a permanent (`400`) failure, is never
fetched, and is left in place for the client to re-register. Neither the Entra
token request nor a WNS send follows redirects, so the client secret and bearer
token cannot be forwarded to another host.

## Response codes

`/push` responds with:

| HTTP status | Meaning |
| --- | --- |
| `200` | Success (`ok`). Also returned when an unregistered device is removed (FCM `404`, WNS `404`/`410`). |
| `400` | Permanent failure: other FCM/WNS `4xx`, missing/invalid parameters, or an invalid stored WNS channel URI (which is never fetched). |
| `503` | Retryable failure: FCM/WNS `401`/`403` (relay-side credentials/server), `429` (rate limit), `500`/`502`/`503`/`504` (server errors), Entra token failures, an unexpected redirect, or network/abort errors. The caller should retry. |

When a push produces both permanent and retryable failures, `400` is returned
(matching the relay's existing FCM behavior); retryable-only failures return
`503`. Invalid/unregistered devices are removed from the clinic's list
automatically and are not treated as a failure.