# Context Map

## Contexts

- [Mobile Notifications](./apps/mobile/CONTEXT.md): presents system notifications, manages notification permission, and opens notification destinations on iOS.
- [Notification Delivery](./server/CONTEXT.md): decides which inbox notifications are eligible for system push and tracks delivery to recipient devices.

## Relationships

- **Notification Delivery -> Mobile Notifications**: an eligible inbox notification becomes a system push for each registered recipient device.
- **Mobile Notifications -> Notification Delivery**: an authenticated app installation registers or revokes its recipient device.
- **Inbox -> Notification Delivery**: the persistent inbox notification is the source of truth for system push content, preference checks, and badge counts.
