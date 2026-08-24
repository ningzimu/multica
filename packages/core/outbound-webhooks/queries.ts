import { queryOptions } from "@tanstack/react-query";
import { api } from "../api";

export const outboundWebhookKeys = {
  all: (wsId: string) => ["outbound-webhooks", wsId] as const,
  detail: (wsId: string, subscriptionId: string) =>
    [...outboundWebhookKeys.all(wsId), subscriptionId] as const,
};

export const outboundWebhookSubscriptionsOptions = (wsId: string) =>
  queryOptions({
    queryKey: outboundWebhookKeys.all(wsId),
    queryFn: () => api.listOutboundWebhookSubscriptions(wsId),
    enabled: !!wsId,
  });

export const outboundWebhookSubscriptionOptions = (
  wsId: string,
  subscriptionId: string,
) =>
  queryOptions({
    queryKey: outboundWebhookKeys.detail(wsId, subscriptionId),
    queryFn: () =>
      api.getOutboundWebhookSubscription(wsId, subscriptionId),
    enabled: !!wsId && !!subscriptionId,
  });
