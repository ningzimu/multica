import { queryOptions } from "@tanstack/react-query";
import { api } from "../api";

export const outboundWebhookKeys = {
  all: (wsId: string) => ["outbound-webhooks", wsId] as const,
  detail: (wsId: string, subscriptionId: string) =>
    [...outboundWebhookKeys.all(wsId), subscriptionId] as const,
  deliveries: (wsId: string, subscriptionId: string, offset: number) =>
    [...outboundWebhookKeys.detail(wsId, subscriptionId), "deliveries", offset] as const,
  delivery: (wsId: string, subscriptionId: string, deliveryId: string) =>
    [...outboundWebhookKeys.detail(wsId, subscriptionId), "delivery", deliveryId] as const,
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

export const outboundWebhookDeliveriesOptions = (
  wsId: string,
  subscriptionId: string,
  offset = 0,
) => queryOptions({
  queryKey: outboundWebhookKeys.deliveries(wsId, subscriptionId, offset),
  queryFn: () => api.listOutboundWebhookDeliveries(wsId, subscriptionId, { offset, limit: 25 }),
  enabled: !!wsId && !!subscriptionId,
});

export const outboundWebhookDeliveryOptions = (
  wsId: string,
  subscriptionId: string,
  deliveryId: string,
) => queryOptions({
  queryKey: outboundWebhookKeys.delivery(wsId, subscriptionId, deliveryId),
  queryFn: () => api.getOutboundWebhookDelivery(wsId, subscriptionId, deliveryId),
  enabled: !!wsId && !!subscriptionId && !!deliveryId,
});
