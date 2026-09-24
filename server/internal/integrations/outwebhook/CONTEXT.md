# Outbound Webhooks

The Outbound Webhooks context publishes selected Multica product events to external systems under an administrator-controlled resource scope.

## Language

**Webhook Subscription**:
An administrator-managed destination combined with a resource scope and a selected set of product events.
_Avoid_: Integration, hook, endpoint row

**Product Event**:
A stable, externally documented statement that something happened in Multica.
_Avoid_: Bus event, WebSocket event, notification type

**Event Selection**:
The set of product events a webhook subscription is allowed to deliver.
_Avoid_: Push range, event filter list

**Workspace Scope**:
A resource scope that includes matching events from every project and unprojected issue in one workspace.
_Avoid_: Global scope, instance-wide scope

**Project Scope**:
A resource scope that includes matching events associated with one or more designated projects in one workspace.
_Avoid_: Repository scope, local scope

**Webhook Delivery**:
One subscription's attempt to publish one product event to its external destination.
_Avoid_: Push notification, callback, APNs delivery

**External Receiver**:
A system that consumes a Multica webhook delivery and decides how to render or act on it.
_Avoid_: product-specific receiver names, webhook provider
