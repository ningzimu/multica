# Mobile Notifications

The Mobile Notifications context defines how an authenticated iOS app presents and responds to notifications created by Multica.

## Language

**System Push**:
An iOS notification shown outside the Multica inbox when the app is in the background or is not running.
_Avoid_: Background message, APNs message

**Notification Permission**:
The user's iOS-level consent for Multica to show system pushes, sounds, and badges.
_Avoid_: Push setting, alert access

**Notification Destination**:
The workspace, issue, comment, or inbox location requested after the user taps a system push. Opening it requires an authenticated user with current access; otherwise the app opens login or the inbox.
_Avoid_: Push link, callback page

**App Badge**:
The number on the Multica app icon representing the user's unread inbox notification count across all workspaces.
_Avoid_: Push count, alert count

**Recipient Device**:
An authenticated Multica app installation that is eligible to receive system pushes for its current user.
_Avoid_: Phone token, APNs client

**Device Binding**:
The association between one recipient device and its current authenticated user. The association ends when that user logs out.
_Avoid_: Token owner, device login

**Push Environment**:
The isolated Apple delivery environment for a recipient device: Sandbox for development builds or Production for release builds.
_Avoid_: Build mode, server mode
