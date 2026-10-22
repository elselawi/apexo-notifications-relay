# Apexo Notifications Relay

A relay to distribute notifications to devices using FCM deployed on cloudflare workers

- registers a device (and clinic)
- replaces FCM token when needed
- sends push notifications (data) to target devices

## Push delivery

`POST /push` sends a data-only FCM message (no `notification` block) to every
device token registered for the requested account IDs. For Apple devices, each
message also carries an APNs block so the push is delivered as a background
update:

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

`/put-device` records the `platform` a token runs on (`ios`, `macos`, `android`,
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

## Response codes

`/push` responds with:

| HTTP status | Meaning |
| --- | --- |
| `200` | Success (`ok`). Also returned when an unregistered token (FCM `404`) is removed. |
| `400` | Permanent failure (other FCM `4xx`, missing/invalid parameters). |
| `503` | Retryable failure: FCM `401`/`403` (relay-side credentials/server), `429` (rate limit), `500`/`502`/`503`/`504` (server errors), or network/abort errors. The caller should retry. |

Invalid/unregistered tokens (FCM `404`) are removed from the clinic's device
list automatically and are not treated as a failure.