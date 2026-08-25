import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import type {
  CreateOutboundWebhookSubscriptionRequest,
  UpdateOutboundWebhookSubscriptionRequest,
} from "../types";
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

function useOutboundWebhookLifecycleMutation<TInput, TResult>(
  wsId: string,
  mutationFn: (input: TInput) => Promise<TResult>,
) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn,
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: outboundWebhookKeys.all(wsId) }),
  });
}

export function useUpdateOutboundWebhookSubscription(wsId: string) {
  return useOutboundWebhookLifecycleMutation(
    wsId,
    ({ subscriptionId, input }: { subscriptionId: string; input: UpdateOutboundWebhookSubscriptionRequest }) =>
      api.updateOutboundWebhookSubscription(wsId, subscriptionId, input),
  );
}

export function usePauseOutboundWebhookSubscription(wsId: string) {
  return useOutboundWebhookLifecycleMutation(wsId, (subscriptionId: string) =>
    api.pauseOutboundWebhookSubscription(wsId, subscriptionId),
  );
}

export function useResumeOutboundWebhookSubscription(wsId: string) {
  return useOutboundWebhookLifecycleMutation(wsId, (subscriptionId: string) =>
    api.resumeOutboundWebhookSubscription(wsId, subscriptionId),
  );
}

export function useTestOutboundWebhookSubscription(wsId: string) {
  return useOutboundWebhookLifecycleMutation(wsId, (subscriptionId: string) =>
    api.testOutboundWebhookSubscription(wsId, subscriptionId),
  );
}

export function useRotateOutboundWebhookSecret(wsId: string) {
  return useOutboundWebhookLifecycleMutation(wsId, (subscriptionId: string) =>
    api.rotateOutboundWebhookSecret(wsId, subscriptionId),
  );
}

export function useRedeliverOutboundWebhookDelivery(wsId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ subscriptionId, deliveryId }: { subscriptionId: string; deliveryId: string }) =>
      api.redeliverOutboundWebhookDelivery(wsId, subscriptionId, deliveryId),
    onSuccess: (_result, { subscriptionId }) =>
      queryClient.invalidateQueries({ queryKey: outboundWebhookKeys.detail(wsId, subscriptionId) }),
  });
}
