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
export type OutboundWebhookScopeMode = "workspace" | "project";
export interface OutboundWebhookSubscription {
  id: string;
  workspaceId: string;
  name: string;
  destinationHint: string;
  events: string[];
  eventCatalogVersion: number;
  scopeMode: OutboundWebhookScopeMode;
  projectIds: string[];
  status: "active" | "paused";
  pauseReason: "manual" | "scope_empty" | "failure_threshold" | null;
  consecutiveTerminalFailures: number;
  signingSecretHint: string;
  secretVersion: number;
  createdAt: string;
  updatedAt: string;
}

export interface ListOutboundWebhookSubscriptionsResponse {
  subscriptions: OutboundWebhookSubscription[];
  capabilityAvailable: boolean;
}

export type OutboundWebhookEvent =
  | "issue.created"
  | "issue.status_changed"
  | "issue.assignee_changed"
  | "issue.priority_changed"
  | "issue.project_changed"
  | "comment.created"
  | "comment.updated"
  | "comment.deleted";

export interface CreateOutboundWebhookSubscriptionRequest {
  name: string;
  destination: string;
  events: OutboundWebhookEventType[];
  scopeMode: OutboundWebhookScopeMode;
  projectIds: string[];
}

export interface CreateOutboundWebhookSubscriptionResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}

export interface UpdateOutboundWebhookSubscriptionRequest {
  name: string;
  destination?: string;
  events: string[];
  scopeMode: OutboundWebhookScopeMode;
  projectIds: string[];
}

export interface RotateOutboundWebhookSecretResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}

export interface TestOutboundWebhookSubscriptionResponse {
  deliveryId: string;
  eventId: string;
  state: "pending";
}

export type OutboundWebhookDeliveryState = "pending" | "succeeded" | "failed";

export interface OutboundWebhookDelivery {
  id: string;
  eventId: string;
  subscriptionId: string;
  eventType: string;
  state: OutboundWebhookDeliveryState;
  attemptCount: number;
  responseStatus: number | null;
  responseExcerpt: string | null;
  failureReason: string | null;
  redeliveryOf: string | null;
  createdAt: string;
  lastAttemptAt: string | null;
  completedAt: string | null;
}

export interface ListOutboundWebhookDeliveriesResponse {
  deliveries: OutboundWebhookDelivery[];
  total: number;
  nextOffset: number | null;
}
