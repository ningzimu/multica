# Notification Delivery

The Notification Delivery context defines when a persistent Multica inbox notification also becomes an iOS system push.

## Language

**Inbox Notification**:
A persistent notification addressed to a Multica user that can be unread, read, or archived inside the app.
_Avoid_: Push notification, alert row

**Eligible Notification**:
An inbox notification that is not caused by its recipient and is allowed by the recipient's event and system notification preferences.
_Avoid_: Sendable event, push candidate

**System Notification Preference**:
A per-user, per-workspace choice that allows or blocks system pushes without removing inbox notifications.
_Avoid_: Inbox preference, global push permission

**Push Delivery**:
The tracked attempt to send one eligible notification to one recipient device.
_Avoid_: APNs call, push request

**Recipient Device**:
An authenticated Multica app installation registered to receive system pushes for its current user.
_Avoid_: Token row, phone

**Device Binding**:
The association between one recipient device and its current authenticated user. A revoked binding is not eligible for delivery.
_Avoid_: Token owner, device session

**Delivery Group**:
The workspace and issue identity used to group related system pushes while keeping each notification visible.
_Avoid_: Collapse key, notification bucket

**Delivery Expiry**:
The point after which an undelivered system push is no longer useful and must not be retried.
_Avoid_: Retry timeout, stale row

**Push Capability**:
The server's configured ability to send system pushes. An unavailable capability does not disable inbox notifications or realtime updates.
_Avoid_: APNs flag, push mode
