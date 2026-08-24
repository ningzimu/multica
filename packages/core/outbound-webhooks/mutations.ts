import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import type { CreateOutboundWebhookSubscriptionRequest } from "../types";
import { outboundWebhookKeys } from "./queries";

export function useCreateOutboundWebhookSubscription(wsId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateOutboundWebhookSubscriptionRequest) =>
      api.createOutboundWebhookSubscription(wsId, input),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: outboundWebhookKeys.all(wsId) }),
  });
}

export function useDeleteOutboundWebhookSubscription(wsId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (subscriptionId: string) =>
      api.deleteOutboundWebhookSubscription(wsId, subscriptionId),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: outboundWebhookKeys.all(wsId) }),
  });
}

export function useUpdateOutboundWebhookEvents(wsId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ subscriptionId, events }: { subscriptionId: string; events: string[] }) =>
      api.updateOutboundWebhookEvents(wsId, subscriptionId, events),
    onSuccess: (subscription) => {
      queryClient.setQueryData(outboundWebhookKeys.detail(wsId, subscription.id), subscription);
      return queryClient.invalidateQueries({ queryKey: outboundWebhookKeys.all(wsId) });
    },
  });
}
