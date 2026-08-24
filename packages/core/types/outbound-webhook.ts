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
  events: ["issue.created"];
  scopeMode: "workspace";
}

export interface CreateOutboundWebhookSubscriptionResponse {
  subscription: OutboundWebhookSubscription;
  signingSecret: string;
}
