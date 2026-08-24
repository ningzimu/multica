export const OUTBOUND_WEBHOOK_ISSUE_EVENTS = [
  "issue.created",
  "issue.status_changed",
  "issue.assignee_changed",
  "issue.priority_changed",
  "issue.project_changed",
] as const;

export type OutboundWebhookIssueEvent = (typeof OUTBOUND_WEBHOOK_ISSUE_EVENTS)[number];

export interface OutboundWebhookSubscription {
  id: string;
  workspaceId: string;
  name: string;
  destinationHint: string;
  events: string[];
  eventCatalogVersion: number;
  scopeMode: "workspace";
  createdAt: string;
  updatedAt: string;
}

export interface ListOutboundWebhookSubscriptionsResponse {
  subscriptions: OutboundWebhookSubscription[];
}

export interface CreateOutboundWebhookSubscriptionRequest {
  name: string;
  destination: string;
  events: OutboundWebhookIssueEvent[];
  scopeMode: "workspace";
}

export interface CreateOutboundWebhookSubscriptionResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}

export interface UpdateOutboundWebhookEventsRequest {
  events: string[];
}
