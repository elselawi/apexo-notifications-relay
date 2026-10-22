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

Android/Web targets ignore the APNs block.

## Response codes

`/push` responds with:

| HTTP status | Meaning |
| --- | --- |
| `200` | Success (`ok`). Also returned when an unregistered token (FCM `404`) is removed. |
| `400` | Permanent failure (other FCM `4xx`, missing/invalid parameters). |
| `503` | Retryable failure: FCM `401`/`403` (relay-side credentials/server), `429` (rate limit), `500`/`502`/`503`/`504` (server errors), or network/abort errors. The caller should retry. |

Invalid/unregistered tokens (FCM `404`) are removed from the clinic's device
list automatically and are not treated as a failure.