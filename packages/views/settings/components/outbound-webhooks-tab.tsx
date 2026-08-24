"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
import { useCurrentWorkspace } from "@multica/core/paths";
import { useCurrentMember } from "@multica/core/permissions";
import {
  outboundWebhookEventTypes,
  type OutboundWebhookEventType,
} from "@multica/core/types";
import {
  outboundWebhookSubscriptionOptions,
  outboundWebhookSubscriptionsOptions,
  useCreateOutboundWebhookSubscription,
  useDeleteOutboundWebhookSubscription,
  useUpdateOutboundWebhookEvents,
} from "@multica/core/outbound-webhooks";
import { Button } from "@multica/ui/components/ui/button";
import { Checkbox } from "@multica/ui/components/ui/checkbox";
import { Input } from "@multica/ui/components/ui/input";
import { Label } from "@multica/ui/components/ui/label";
import { useT } from "../../i18n";
import { SettingsCard } from "./settings-layout";

export function OutboundWebhooksTab() {
  const { t } = useT("settings");
  const workspace = useCurrentWorkspace();
  const wsId = workspace?.id ?? "";
  const member = useCurrentMember(wsId);
  const canManage = member.role === "owner" || member.role === "admin";
  const subscriptions = useQuery(outboundWebhookSubscriptionsOptions(wsId));
  const [name, setName] = useState("");
  const [destination, setDestination] = useState("");
  const [selectedEvents, setSelectedEvents] = useState<OutboundWebhookEventType[]>([
    "issue.created",
  ]);
  const [disclosedSecret, setDisclosedSecret] = useState<string | null>(null);
  const [selectedSubscriptionId, setSelectedSubscriptionId] = useState("");
  const [editingEvents, setEditingEvents] = useState<string[]>([]);
  const selectedSubscription = useQuery(
    outboundWebhookSubscriptionOptions(wsId, selectedSubscriptionId),
  );

  const createSubscription = useCreateOutboundWebhookSubscription(wsId);
  const deleteSubscription = useDeleteOutboundWebhookSubscription(wsId);
  const updateEvents = useUpdateOutboundWebhookEvents(wsId);

  return (
    <div className="space-y-4">
      {canManage && (
        <SettingsCard>
          <form
            className="space-y-4 p-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (!name.trim() || !destination.trim() || selectedEvents.length === 0) return;
              createSubscription.mutate(
                {
                  name,
                  destination,
                  events: selectedEvents,
                  scopeMode: "workspace",
                },
                {
                  onSuccess: (result) => {
                    if (!result.subscription.id || !result.signingSecret) {
                      toast.error(t(($) => $.outbound_webhooks.create_partial_failure));
                      return;
                    }
                    setDisclosedSecret(result.signingSecret);
                    setName("");
                    setDestination("");
                    setSelectedEvents(["issue.created"]);
                    toast.success(t(($) => $.outbound_webhooks.created));
                  },
                  onError: (error) =>
                    toast.error(
                      error instanceof Error
                        ? error.message
                        : t(($) => $.outbound_webhooks.create_failed),
                    ),
                },
              );
            }}
          >
            <div className="space-y-1.5">
              <Label htmlFor="outbound-webhook-name">{t(($) => $.outbound_webhooks.name)}</Label>
              <Input id="outbound-webhook-name" value={name} onChange={(event) => setName(event.target.value)} required maxLength={100} />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="outbound-webhook-destination">{t(($) => $.outbound_webhooks.destination)}</Label>
              <Input id="outbound-webhook-destination" type="url" value={destination} onChange={(event) => setDestination(event.target.value)} required placeholder="https://example.com/webhooks" />
            </div>
            <div className="space-y-2">
              <p className="text-body font-medium">{t(($) => $.outbound_webhooks.event_selection)}</p>
              {outboundWebhookEventTypes.map((eventType) => (
                <Label key={eventType} className="flex items-center gap-2 font-normal">
                  <Checkbox
                    checked={selectedEvents.includes(eventType)}
                    onCheckedChange={(checked) =>
                      setSelectedEvents((current) =>
                        checked === true
                          ? current.includes(eventType)
                            ? current
                            : [...current, eventType]
                          : current.filter((candidate) => candidate !== eventType),
                      )
                    }
                  />
                  <code>{eventType}</code>
                </Label>
              ))}
            </div>
            <p className="text-caption text-muted-foreground">{t(($) => $.outbound_webhooks.workspace_scope)}</p>
            <Button type="submit" disabled={selectedEvents.length === 0 || createSubscription.isPending}>{t(($) => $.outbound_webhooks.create)}</Button>
          </form>
        </SettingsCard>
      )}

      {disclosedSecret && (
        <SettingsCard>
          <div className="space-y-2 p-4">
            <p className="text-body font-medium">{t(($) => $.outbound_webhooks.secret_once)}</p>
            <code className="block break-all rounded-md bg-muted p-3 text-caption">{disclosedSecret}</code>
            <Button variant="outline" onClick={() => setDisclosedSecret(null)}>{t(($) => $.outbound_webhooks.secret_saved)}</Button>
          </div>
        </SettingsCard>
      )}

      {subscriptions.isError ? (
        <p className="text-body text-destructive">{t(($) => $.outbound_webhooks.load_failed)}</p>
      ) : subscriptions.data?.subscriptions.length ? (
        <SettingsCard>
          <div className="divide-y divide-border">
            {subscriptions.data.subscriptions.map((subscription) => (
              <div key={subscription.id}>
                <div className="flex items-center gap-3 p-4">
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-body font-medium">{subscription.name}</p>
                    <p className="truncate text-caption text-muted-foreground">{subscription.destinationHint}</p>
                    <p className="text-caption text-muted-foreground"><code>{subscription.events.join(", ")}</code> · {t(($) => $.outbound_webhooks.workspace_scope_short)}</p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      const opening = selectedSubscriptionId !== subscription.id;
                      setSelectedSubscriptionId(opening ? subscription.id : "");
                      setEditingEvents(opening ? subscription.events : []);
                    }}
                  >
                    {selectedSubscriptionId === subscription.id
                      ? t(($) => $.outbound_webhooks.hide_details)
                      : t(($) => $.outbound_webhooks.inspect)}
                  </Button>
                  {canManage && (
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label={t(($) => $.outbound_webhooks.delete)}
                      onClick={() =>
                        deleteSubscription.mutate(subscription.id, {
                          onSuccess: () => {
                            setSelectedSubscriptionId("");
                            toast.success(t(($) => $.outbound_webhooks.deleted));
                          },
                          onError: () => toast.error(t(($) => $.outbound_webhooks.delete_failed)),
                        })
                      }
                      disabled={deleteSubscription.isPending}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  )}
                </div>
                {selectedSubscriptionId === subscription.id && selectedSubscription.data && (
                  <dl className="grid gap-2 border-t border-border p-4 text-caption sm:grid-cols-2">
                    <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.destination_hint)}</dt><dd>{selectedSubscription.data.destinationHint}</dd></div>
                    <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.event_catalog_version)}</dt><dd>{selectedSubscription.data.eventCatalogVersion}</dd></div>
                    <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.event_selection)}</dt><dd>{selectedSubscription.data.events.join(", ")}</dd></div>
                    <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.scope)}</dt><dd>{t(($) => $.outbound_webhooks.workspace_scope_short)}</dd></div>
                    {canManage && (
                      <div className="space-y-2 sm:col-span-2">
                        {outboundWebhookEventTypes.map((eventName) => (
                          <Label key={eventName} className="flex items-center gap-2 font-normal">
                            <Checkbox
                              checked={editingEvents.includes(eventName)}
                              onCheckedChange={(checked) =>
                                setEditingEvents((current) =>
                                  checked === true
                                    ? [...current, eventName].filter((value, index, values) => values.indexOf(value) === index)
                                    : current.filter((candidate) => candidate !== eventName),
                                )
                              }
                            />
                            <code>{eventName}</code>
                          </Label>
                        ))}
                        <Button
                          size="sm"
                          disabled={editingEvents.length === 0 || updateEvents.isPending}
                          onClick={() =>
                            updateEvents.mutate(
                              { subscriptionId: selectedSubscriptionId, events: editingEvents },
                              {
                                onSuccess: () => toast.success(t(($) => $.outbound_webhooks.events_saved)),
                                onError: () => toast.error(t(($) => $.outbound_webhooks.events_save_failed)),
                              },
                            )
                          }
                        >
                          {t(($) => $.outbound_webhooks.save_events)}
                        </Button>
                      </div>
                    )}
                  </dl>
                )}
              </div>
            ))}
          </div>
        </SettingsCard>
      ) : (
        <p className="text-body text-muted-foreground">{t(($) => $.outbound_webhooks.empty)}</p>
      )}
    </div>
  );
}
