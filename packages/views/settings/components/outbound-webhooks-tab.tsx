"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
import { useCurrentWorkspace } from "@multica/core/paths";
import { useCurrentMember } from "@multica/core/permissions";
import { projectListOptions } from "@multica/core/projects/queries";
import type { OutboundWebhookEvent, OutboundWebhookScopeMode, OutboundWebhookSubscription } from "@multica/core/types";
import {
  outboundWebhookSubscriptionOptions,
  outboundWebhookSubscriptionsOptions,
  outboundWebhookDeliveriesOptions,
  outboundWebhookDeliveryOptions,
  useCreateOutboundWebhookSubscription,
  useDeleteOutboundWebhookSubscription,
  usePauseOutboundWebhookSubscription,
  useResumeOutboundWebhookSubscription,
  useRotateOutboundWebhookSecret,
  useTestOutboundWebhookSubscription,
  useUpdateOutboundWebhookSubscription,
  useRedeliverOutboundWebhookDelivery,
} from "@multica/core/outbound-webhooks";
import { Button } from "@multica/ui/components/ui/button";
import { Checkbox } from "@multica/ui/components/ui/checkbox";
import { Input } from "@multica/ui/components/ui/input";
import { Label } from "@multica/ui/components/ui/label";
import { useT } from "../../i18n";
import { SettingsCard } from "./settings-layout";

const EVENT_CATALOG: OutboundWebhookEvent[] = [
  "issue.created", "issue.status_changed", "issue.assignee_changed", "issue.priority_changed",
  "issue.project_changed", "comment.created", "comment.updated", "comment.deleted",
];

const DEFAULT_EVENTS: OutboundWebhookEvent[] = [
  "issue.created", "issue.status_changed", "issue.assignee_changed", "comment.created",
];

function EventSelection({ selected, onChange }: {
  selected: string[];
  onChange: (events: string[]) => void;
}) {
  return (
    <div className="grid gap-2 sm:grid-cols-2">
      {EVENT_CATALOG.map((eventType) => (
        <Label key={eventType} className="flex items-center gap-2 font-normal">
          <Checkbox
            checked={selected.includes(eventType)}
            onCheckedChange={(checked) => onChange(
              checked === true ? [...selected, eventType] : selected.filter((item) => item !== eventType),
            )}
          />
          <code>{eventType}</code>
        </Label>
      ))}
    </div>
  );
}

function ProjectSelection({ projects, selected, onChange, requiredLabel }: {
  projects: Array<{ id: string; title: string }>;
  selected: string[];
  onChange: (projectIds: string[]) => void;
  requiredLabel: string;
}) {
  return (
    <div className="space-y-2">
      {projects.map((project) => (
        <Label key={project.id} className="flex items-center gap-2 font-normal">
          <Checkbox
            checked={selected.includes(project.id)}
            onCheckedChange={(checked) => onChange(
              checked === true
                ? [...new Set([...selected, project.id])]
                : selected.filter((id) => id !== project.id),
            )}
          />
          {project.title}
        </Label>
      ))}
      {selected.length === 0 && <p className="text-caption text-destructive">{requiredLabel}</p>}
    </div>
  );
}

function DeliveryHistory({ wsId, subscription, canManage }: { wsId: string; subscription: OutboundWebhookSubscription; canManage: boolean }) {
  const { t } = useT("settings");
  const [offset, setOffset] = useState(0);
  const [selectedDeliveryId, setSelectedDeliveryId] = useState("");
  const history = useQuery(outboundWebhookDeliveriesOptions(wsId, subscription.id, offset));
  const detail = useQuery(outboundWebhookDeliveryOptions(wsId, subscription.id, selectedDeliveryId));
  const redeliver = useRedeliverOutboundWebhookDelivery(wsId);

  return (
    <div className="space-y-3 border-t border-border p-4">
      <p className="text-body font-medium">{t(($) => $.outbound_webhooks.delivery_history)}</p>
      {history.isError ? (
        <p className="text-caption text-destructive">{t(($) => $.outbound_webhooks.history_failed)}</p>
      ) : history.data?.deliveries.length ? (
        <div className="space-y-2">
          {history.data.deliveries.map((delivery) => (
            <div key={delivery.id} className="rounded-md border border-border p-3 text-caption">
              <div className="flex flex-wrap items-center gap-2">
                <code className="font-medium">{delivery.eventType}</code>
                <span>{delivery.state}</span>
                <span>{t(($) => $.outbound_webhooks.attempts)}: {delivery.attemptCount}</span>
                {delivery.responseStatus !== null && <span>HTTP {delivery.responseStatus}</span>}
                <time className="text-muted-foreground">{new Date(delivery.createdAt).toLocaleString()}</time>
                <Button variant="outline" size="sm" onClick={() => setSelectedDeliveryId(selectedDeliveryId === delivery.id ? "" : delivery.id)}>
                  {selectedDeliveryId === delivery.id ? t(($) => $.outbound_webhooks.hide_details) : t(($) => $.outbound_webhooks.inspect)}
                </Button>
                {canManage && (
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={subscription.status === "paused" || redeliver.isPending}
                    onClick={() => redeliver.mutate({ subscriptionId: subscription.id, deliveryId: delivery.id }, {
                      onSuccess: () => toast.success(t(($) => $.outbound_webhooks.redelivery_queued)),
                      onError: (error) => toast.error(error instanceof Error ? error.message : t(($) => $.outbound_webhooks.redelivery_failed)),
                    })}
                  >{t(($) => $.outbound_webhooks.redeliver)}</Button>
                )}
              </div>
              {selectedDeliveryId === delivery.id && detail.data && (
                <dl className="mt-3 grid gap-1 rounded-md bg-muted p-3">
                  <div><dt className="inline text-muted-foreground">{t(($) => $.outbound_webhooks.delivery_id)}: </dt><dd className="inline break-all">{detail.data.id}</dd></div>
                  <div><dt className="inline text-muted-foreground">{t(($) => $.outbound_webhooks.event_id)}: </dt><dd className="inline break-all">{detail.data.eventId}</dd></div>
                  {detail.data.redeliveryOf && <div><dt className="inline text-muted-foreground">{t(($) => $.outbound_webhooks.redelivery_of)}: </dt><dd className="inline break-all">{detail.data.redeliveryOf}</dd></div>}
                  {detail.data.failureReason && <div><dt className="inline text-muted-foreground">{t(($) => $.outbound_webhooks.failure_reason)}: </dt><dd className="inline">{detail.data.failureReason}</dd></div>}
                  {detail.data.responseExcerpt && <div><dt className="inline text-muted-foreground">{t(($) => $.outbound_webhooks.response_excerpt)}: </dt><dd className="inline break-words">{detail.data.responseExcerpt}</dd></div>}
                </dl>
              )}
            </div>
          ))}
          <div className="flex items-center justify-between">
            <Button variant="outline" size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 25))}>{t(($) => $.outbound_webhooks.previous)}</Button>
            <span className="text-caption text-muted-foreground">
              {t(($) => $.outbound_webhooks.deliveries, { count: history.data.total })}
            </span>
            <Button variant="outline" size="sm" disabled={history.data.nextOffset === null} onClick={() => setOffset(history.data?.nextOffset ?? offset)}>{t(($) => $.outbound_webhooks.next)}</Button>
          </div>
        </div>
      ) : (
        <p className="text-caption text-muted-foreground">{t(($) => $.outbound_webhooks.no_deliveries)}</p>
      )}
    </div>
  );
}

export function OutboundWebhooksTab() {
  const { t } = useT("settings");
  const workspace = useCurrentWorkspace();
  const wsId = workspace?.id ?? "";
  const member = useCurrentMember(wsId);
  const canManage = member.role === "owner" || member.role === "admin";
  const subscriptions = useQuery(outboundWebhookSubscriptionsOptions(wsId));
  const projects = useQuery(projectListOptions(wsId));
  const [name, setName] = useState("");
  const [destination, setDestination] = useState("");
  const [events, setEvents] = useState<string[]>(DEFAULT_EVENTS);
  const [scopeMode, setScopeMode] = useState<OutboundWebhookScopeMode>("workspace");
  const [selectedProjectIds, setSelectedProjectIds] = useState<string[]>([]);
  const [disclosedSecret, setDisclosedSecret] = useState<string | null>(null);
  const [selectedSubscriptionId, setSelectedSubscriptionId] = useState("");
  const [editing, setEditing] = useState<OutboundWebhookSubscription | null>(null);
  const [editName, setEditName] = useState("");
  const [editDestination, setEditDestination] = useState("");
  const [editEvents, setEditEvents] = useState<string[]>([]);
  const [editScopeMode, setEditScopeMode] = useState<OutboundWebhookScopeMode>("workspace");
  const [editProjectIds, setEditProjectIds] = useState<string[]>([]);
  const selectedSubscription = useQuery(
    outboundWebhookSubscriptionOptions(wsId, selectedSubscriptionId),
  );

  const createSubscription = useCreateOutboundWebhookSubscription(wsId);
  const updateSubscription = useUpdateOutboundWebhookSubscription(wsId);
  const deleteSubscription = useDeleteOutboundWebhookSubscription(wsId);
  const pauseSubscription = usePauseOutboundWebhookSubscription(wsId);
  const resumeSubscription = useResumeOutboundWebhookSubscription(wsId);
  const testSubscription = useTestOutboundWebhookSubscription(wsId);
  const rotateSecret = useRotateOutboundWebhookSecret(wsId);
  const showMutationError = (error: unknown, fallback: string) =>
    toast.error(error instanceof Error ? error.message : fallback);

  if (subscriptions.data?.capabilityAvailable === false) {
    return (
      <SettingsCard>
        <div className="space-y-1 p-4">
          <p className="text-body font-medium">{t(($) => $.outbound_webhooks.unavailable)}</p>
          <p className="text-caption text-muted-foreground">{t(($) => $.outbound_webhooks.unavailable_description)}</p>
        </div>
      </SettingsCard>
    );
  }

  return (
    <div className="space-y-4">
      {canManage && (
        <SettingsCard>
          <form className="space-y-4 p-4" onSubmit={(event) => {
            event.preventDefault();
            if (!name.trim() || !destination.trim() || events.length === 0 || (scopeMode === "project" && selectedProjectIds.length === 0)) return;
            createSubscription.mutate({
              name,
              destination,
              events: events as OutboundWebhookEvent[],
              scopeMode,
              projectIds: scopeMode === "project" ? selectedProjectIds : [],
            }, {
              onSuccess: (result) => {
                if (!result.subscription.id || !result.signingSecret) {
                  toast.error(t(($) => $.outbound_webhooks.create_partial_failure));
                  return;
                }
                setDisclosedSecret(result.signingSecret);
                setName("");
                setDestination("");
                setEvents(DEFAULT_EVENTS);
                setScopeMode("workspace");
                setSelectedProjectIds([]);
                toast.success(t(($) => $.outbound_webhooks.created));
              },
              onError: (error) => showMutationError(error, t(($) => $.outbound_webhooks.create_failed)),
            });
          }}>
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
              <EventSelection selected={events} onChange={setEvents} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="outbound-webhook-scope">{t(($) => $.outbound_webhooks.scope)}</Label>
              <select id="outbound-webhook-scope" className="flex h-9 w-full rounded-md border border-input bg-background px-3 text-body" value={scopeMode} onChange={(event) => setScopeMode(event.target.value as OutboundWebhookScopeMode)}>
                <option value="workspace">{t(($) => $.outbound_webhooks.workspace_scope_short)}</option>
                <option value="project">{t(($) => $.outbound_webhooks.project_scope)}</option>
              </select>
              {scopeMode === "workspace" ? (
                <p className="text-caption text-muted-foreground">{t(($) => $.outbound_webhooks.workspace_scope)}</p>
              ) : (
                <ProjectSelection projects={projects.data ?? []} selected={selectedProjectIds} onChange={setSelectedProjectIds} requiredLabel={t(($) => $.outbound_webhooks.project_required)} />
              )}
            </div>
            <Button type="submit" disabled={events.length === 0 || (scopeMode === "project" && selectedProjectIds.length === 0) || createSubscription.isPending}>{t(($) => $.outbound_webhooks.create)}</Button>
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
            {subscriptions.data.subscriptions.map((subscription) => {
              const paused = subscription.status === "paused";
              return (
                <div key={subscription.id}>
                  <div className="flex flex-wrap items-center gap-3 p-4">
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-body font-medium">{subscription.name}</p>
                      <p className="truncate text-caption text-muted-foreground">{subscription.destinationHint}</p>
                      <p className="text-caption text-muted-foreground">
                        {paused ? t(($) => $.outbound_webhooks.paused) : t(($) => $.outbound_webhooks.active)}
                        {paused && subscription.pauseReason ? ` · ${subscription.pauseReason}` : ""}
                      </p>
                    </div>
                    <Button variant="outline" size="sm" onClick={() => setSelectedSubscriptionId(selectedSubscriptionId === subscription.id ? "" : subscription.id)}>
                      {selectedSubscriptionId === subscription.id ? t(($) => $.outbound_webhooks.hide_details) : t(($) => $.outbound_webhooks.inspect)}
                    </Button>
                    {canManage && (
                      <>
                        <Button variant="outline" size="sm" onClick={() => {
                          setEditing(subscription);
                          setEditName(subscription.name);
                          setEditDestination("");
                          setEditEvents(subscription.events);
                          setEditScopeMode(subscription.scopeMode);
                          setEditProjectIds(subscription.projectIds);
                        }}>{t(($) => $.outbound_webhooks.edit)}</Button>
                        <Button variant="outline" size="sm" onClick={() => {
                          const mutation = paused ? resumeSubscription : pauseSubscription;
                          mutation.mutate(subscription.id, {
                            onSuccess: () => toast.success(paused ? t(($) => $.outbound_webhooks.resumed) : t(($) => $.outbound_webhooks.paused_success)),
                            onError: (error) => showMutationError(error, t(($) => $.outbound_webhooks.lifecycle_failed)),
                          });
                        }}>{paused ? t(($) => $.outbound_webhooks.resume) : t(($) => $.outbound_webhooks.pause)}</Button>
                        <Button variant="outline" size="sm" onClick={() => testSubscription.mutate(subscription.id, {
                          onSuccess: (result) => {
                            if (!result.deliveryId || !result.eventId) {
                              toast.error(t(($) => $.outbound_webhooks.test_failed));
                              return;
                            }
                            toast.success(t(($) => $.outbound_webhooks.test_queued));
                          },
                          onError: (error) => showMutationError(error, t(($) => $.outbound_webhooks.test_failed)),
                        })}>{t(($) => $.outbound_webhooks.test)}</Button>
                        <Button variant="outline" size="sm" onClick={() => rotateSecret.mutate(subscription.id, {
                          onSuccess: (result) => {
                            if (!result.signingSecret) {
                              toast.error(t(($) => $.outbound_webhooks.rotate_failed));
                              return;
                            }
                            setDisclosedSecret(result.signingSecret);
                            toast.success(t(($) => $.outbound_webhooks.rotated));
                          },
                          onError: (error) => showMutationError(error, t(($) => $.outbound_webhooks.rotate_failed)),
                        })}>{t(($) => $.outbound_webhooks.rotate)}</Button>
                        <Button variant="ghost" size="icon" aria-label={t(($) => $.outbound_webhooks.delete)} onClick={() =>
                          deleteSubscription.mutate(subscription.id, {
                            onSuccess: () => { setSelectedSubscriptionId(""); toast.success(t(($) => $.outbound_webhooks.deleted)); },
                            onError: () => toast.error(t(($) => $.outbound_webhooks.delete_failed)),
                          })
                        } disabled={deleteSubscription.isPending}>
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </>
                    )}
                  </div>
                  {editing?.id === subscription.id && (
                    <form className="space-y-3 border-t border-border p-4" onSubmit={(event) => {
                      event.preventDefault();
                      if (!editName.trim() || editEvents.length === 0 || (editScopeMode === "project" && editProjectIds.length === 0)) return;
                      updateSubscription.mutate({ subscriptionId: subscription.id, input: {
                        name: editName,
                        destination: editDestination.trim() || undefined,
                        events: editEvents,
                        scopeMode: editScopeMode,
                        projectIds: editScopeMode === "project" ? editProjectIds : [],
                      } }, {
                        onSuccess: () => { setEditing(null); toast.success(t(($) => $.outbound_webhooks.updated)); },
                        onError: (error) => showMutationError(error, t(($) => $.outbound_webhooks.update_failed)),
                      });
                    }}>
                      <div className="space-y-1.5"><Label htmlFor={`outbound-webhook-edit-name-${subscription.id}`}>{t(($) => $.outbound_webhooks.name)}</Label><Input id={`outbound-webhook-edit-name-${subscription.id}`} value={editName} onChange={(event) => setEditName(event.target.value)} maxLength={100} required /></div>
                      <div className="space-y-1.5"><Label htmlFor={`outbound-webhook-edit-destination-${subscription.id}`}>{t(($) => $.outbound_webhooks.destination)}</Label><Input id={`outbound-webhook-edit-destination-${subscription.id}`} type="url" value={editDestination} onChange={(event) => setEditDestination(event.target.value)} placeholder={t(($) => $.outbound_webhooks.destination_unchanged)} /></div>
                      <EventSelection selected={editEvents} onChange={setEditEvents} />
                      <div className="space-y-2">
                        <Label htmlFor={`outbound-webhook-edit-scope-${subscription.id}`}>{t(($) => $.outbound_webhooks.scope)}</Label>
                        <select id={`outbound-webhook-edit-scope-${subscription.id}`} className="flex h-9 w-full rounded-md border border-input bg-background px-3 text-body" value={editScopeMode} onChange={(event) => setEditScopeMode(event.target.value as OutboundWebhookScopeMode)}>
                          <option value="workspace">{t(($) => $.outbound_webhooks.workspace_scope_short)}</option>
                          <option value="project">{t(($) => $.outbound_webhooks.project_scope)}</option>
                        </select>
                        {editScopeMode === "project" && <ProjectSelection projects={projects.data ?? []} selected={editProjectIds} onChange={setEditProjectIds} requiredLabel={t(($) => $.outbound_webhooks.project_required)} />}
                      </div>
                      <div className="flex gap-2"><Button type="submit" disabled={editEvents.length === 0 || (editScopeMode === "project" && editProjectIds.length === 0) || updateSubscription.isPending}>{t(($) => $.outbound_webhooks.save)}</Button><Button type="button" variant="outline" onClick={() => setEditing(null)}>{t(($) => $.outbound_webhooks.cancel)}</Button></div>
                    </form>
                  )}
                  {selectedSubscriptionId === subscription.id && selectedSubscription.data && (
                    <><dl className="grid gap-2 border-t border-border p-4 text-caption sm:grid-cols-2">
                      <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.destination_hint)}</dt><dd>{selectedSubscription.data.destinationHint}</dd></div>
                      <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.secret_hint)}</dt><dd>{selectedSubscription.data.signingSecretHint || "—"}</dd></div>
                      <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.event_catalog_version)}</dt><dd>{selectedSubscription.data.eventCatalogVersion}</dd></div>
                      <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.event_selection)}</dt><dd>{selectedSubscription.data.events.join(", ")}</dd></div>
                      <div><dt className="text-muted-foreground">{t(($) => $.outbound_webhooks.scope)}</dt><dd>{selectedSubscription.data.scopeMode === "workspace" ? t(($) => $.outbound_webhooks.workspace_scope_short) : t(($) => $.outbound_webhooks.project_scope)}</dd></div>
                    </dl>{canManage && <DeliveryHistory wsId={wsId} subscription={selectedSubscription.data} canManage />}</>
                  )}
                </div>
              );
            })}
          </div>
        </SettingsCard>
      ) : (
        <p className="text-body text-muted-foreground">{t(($) => $.outbound_webhooks.empty)}</p>
      )}
    </div>
  );
}
