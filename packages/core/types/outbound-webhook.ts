export const outboundWebhookEventTypes = [
  "issue.created",
  "issue.status_changed",
  "issue.assignee_changed",
  "issue.priority_changed",
  "issue.project_changed",
  "comment.created",
  "comment.updated",
  "comment.deleted",
] as const;

export type OutboundWebhookEventType = (typeof outboundWebhookEventTypes)[number];

export interface OutboundWebhookSubscription {
  id: string;
  workspaceId: string;
  name: string;
  destinationHint: string;
  events: string[];
  eventCatalogVersion: number;
  scopeMode: "workspace";
  status: "active" | "paused";
  pauseReason: string | null;
  consecutiveTerminalFailures: number;
  createdAt: string;
  updatedAt: string;
}

export interface ListOutboundWebhookSubscriptionsResponse {
  subscriptions: OutboundWebhookSubscription[];
}

export interface CreateOutboundWebhookSubscriptionRequest {
  name: string;
  destination: string;
  events: OutboundWebhookEventType[];
  scopeMode: "workspace";
}

export interface CreateOutboundWebhookSubscriptionResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}

export interface UpdateOutboundWebhookEventsRequest {
  events: string[];
}
