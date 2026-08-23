---
status: accepted
---

# Deliver iOS system notifications directly through APNs with a durable outbox

The self-hosted Multica server will send eligible notifications directly to Apple Push Notification service instead of using Expo as a delivery relay. Each delivery will first enter a durable database outbox, then use at-least-once delivery with the inbox event identity for deduplication. This keeps the delivery path under self-hosted control and allows temporary APNs or server failures to be retried.

APNs credentials remain server-only secrets. The app requests notification permission after login, existing per-workspace system notification preferences gate delivery, and related notifications group by workspace and issue without collapsing individual events.

## Consequences

- A recipient device is bound to the current authenticated user, revoked on logout, and rebound after another login.
- Sandbox and Production device registrations remain separate and include their Bundle ID.
- The app badge represents unread inbox notifications across all workspaces.
- Temporary failures use exponential backoff for no more than 24 hours. An APNs response that declares a device token invalid disables that recipient device immediately.
- A server without APNs credentials reports push capability as unavailable while inbox notifications and realtime updates continue to work.
- Delivery logs contain delivery identity, status, failure reason, and timing, but never device tokens, APNs private keys, or complete comment bodies.
- The first release targets iOS only and uses standard visible alerts with the default sound. It does not use Critical Alerts, silent background pushes, notification action buttons, or product-specific quiet hours.
- Tapping a notification requires current authentication and authorization. The app resumes the destination after login when access remains valid, and otherwise opens the inbox.
- Device registration and push delivery are additive capabilities. Existing mobile, web, and desktop clients continue to use the current inbox and realtime protocols unchanged.
- Release acceptance covers foreground, background, terminated-app, permission-disabled, logged-out, multi-device, invalid-token, cross-workspace destination, and badge synchronization scenarios on a physical iPhone.

## Considered Options

- Expo push delivery was rejected because it adds a third-party relay to the self-hosted notification path.
- WebSocket-only delivery was rejected because iOS suspends the socket while the app is in the background.
- Direct send without an outbox was rejected because a process restart or temporary APNs failure could lose a notification.
