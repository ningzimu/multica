export interface OutboundWebhookSubscription {
  id: string;
  workspaceId: string;
  name: string;
  destinationHint: string;
  events: string[];
  eventCatalogVersion: number;
  scopeMode: "workspace" | "projects";
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
  events: OutboundWebhookEvent[];
  scopeMode: "workspace";
}

export interface CreateOutboundWebhookSubscriptionResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}

export interface UpdateOutboundWebhookSubscriptionRequest {
  name: string;
  destination?: string;
  events: string[];
  scopeMode: "workspace" | "projects";
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
