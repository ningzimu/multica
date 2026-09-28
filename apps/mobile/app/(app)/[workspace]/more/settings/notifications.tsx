/**
 * Notification preferences subscreen. 6 inbox groups + system_notifications
 * toggle, each backed by an optimistic PATCH /api/notification-preferences.
 *
 * Copy mirrors packages/views/settings/components/notifications-tab.tsx.
 * Group labels describe the same server-side semantics as web; divergent
 * labels would violate behavioral parity (apps/mobile/CLAUDE.md).
 */
import { useCallback, useState } from "react";
import { ActivityIndicator, AppState, ScrollView, View } from "react-native";
import { useFocusEffect } from "@react-navigation/native";
import { useQuery } from "@tanstack/react-query";
import type {
  NotificationGroupKey,
  NotificationPreferences,
} from "@multica/core/types";
import { Text } from "@/components/ui/text";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { useWorkspaceStore } from "@/data/workspace-store";
import { notificationPreferenceOptions } from "@/data/queries/notification-preferences";
import { useUpdateNotificationPreferences } from "@/data/mutations/notification-preferences";
import { notificationRegistration } from "@/data/native-notification-registration";
import type { NotificationPermissionStatus } from "@/data/notification-registration";
import { useT } from "@/lib/i18n";

const INBOX_GROUPS: {
  key: Exclude<NotificationGroupKey, "system_notifications">;
  labelKey: string;
  descriptionKey?: string;
}[] = [
  {
    key: "assignments",
    labelKey: "groups.assignments",
    descriptionKey: "groups.assignments_description",
  },
  {
    key: "status_changes",
    labelKey: "groups.status_changes",
  },
  {
    key: "comments",
    labelKey: "groups.comments",
    descriptionKey: "groups.comments_description",
  },
  {
    key: "mentions",
    labelKey: "groups.mentions",
    descriptionKey: "groups.mentions_description",
  },
  {
    key: "updates",
    labelKey: "groups.updates",
    descriptionKey: "groups.updates_description",
  },
  {
    key: "agent_activity",
    labelKey: "groups.agent_activity",
    descriptionKey: "groups.agent_activity_description",
  },
];

export default function NotificationsSettingsScreen() {
  const wsId = useWorkspaceStore((s) => s.currentWorkspaceId);
  const { t } = useT("settings");
  const { data, isLoading, error } = useQuery(
    notificationPreferenceOptions(wsId),
  );
  const mutation = useUpdateNotificationPreferences();
  const [permission, setPermission] =
    useState<NotificationPermissionStatus | null>(null);

  useFocusEffect(
    useCallback(() => {
      let active = true;
      const refreshPermission = () => {
        void notificationRegistration
          .getPermissionStatus()
          .then(async (status) => {
            if (status === "granted") {
              await notificationRegistration.onAuthenticated();
            }
            if (active) setPermission(status);
          })
          .catch(() => {
            if (active) setPermission(null);
          });
      };
      refreshPermission();
      const subscription = AppState.addEventListener("change", (state) => {
        if (state === "active") refreshPermission();
      });
      return () => {
        active = false;
        subscription.remove();
      };
    }, []),
  );

  const requestPermission = async () => {
    const result =
      await notificationRegistration.requestPermissionFromSettings();
    setPermission(
      result === "registered"
        ? "granted"
        : await notificationRegistration.getPermissionStatus(),
    );
  };

  const preferences: NotificationPreferences = data?.preferences ?? {};

  const onToggle = (key: NotificationGroupKey, enabled: boolean) => {
    const next: NotificationPreferences = { ...preferences };
    if (enabled) {
      // Default is "all" — omitting the key keeps the object clean.
      delete next[key];
    } else {
      next[key] = "muted";
    }
    mutation.mutate(next);
  };

  const systemEnabled = preferences.system_notifications !== "muted";

  if (isLoading) {
    return (
      <View className="flex-1 items-center justify-center bg-background">
        <ActivityIndicator />
      </View>
    );
  }

  if (error) {
    return (
      <View className="flex-1 items-center justify-center bg-background px-6">
        <Text className="text-sm text-destructive text-center">
          {t("errors.load_failed")}
        </Text>
      </View>
    );
  }

  return (
    <ScrollView
      className="flex-1 bg-background"
      contentContainerClassName="px-4 py-4 gap-6"
    >
      {permission === "denied" ? (
        <Section
          title={t("ios_permission.title")}
          description={t("ios_permission.denied")}
        >
          <View className="p-4 gap-3">
            <Text className="text-sm text-muted-foreground">
              {t("ios_permission.inbox_available")}
            </Text>
            <Button onPress={() => void notificationRegistration.openSettings()}>
              <Text>{t("ios_permission.open_settings")}</Text>
            </Button>
          </View>
        </Section>
      ) : null}

      {permission === "undetermined" ? (
        <Section
          title={t("ios_permission.title")}
          description={t("ios_permission.undetermined")}
        >
          <View className="p-4 gap-3">
            <Text className="text-sm text-muted-foreground">
              {t("ios_permission.inbox_available")}
            </Text>
            <Button onPress={() => void requestPermission()}>
              <Text>{t("ios_permission.enable")}</Text>
            </Button>
          </View>
        </Section>
      ) : null}

      <Section
        title={t("groups.inbox_notifications")}
      >
        {INBOX_GROUPS.map((group, idx) => {
          const enabled = preferences[group.key] !== "muted";
          const isLast = idx === INBOX_GROUPS.length - 1;
          return (
            <View key={group.key}>
              <View className="flex-row items-center px-4 py-3 gap-3">
                <View className="flex-1">
                  <Text className="text-base font-medium text-foreground">
                    {t(group.labelKey)}
                  </Text>
                  {group.descriptionKey ? (
                    <Text className="text-xs text-muted-foreground mt-0.5">
                      {t(group.descriptionKey)}
                    </Text>
                  ) : null}
                </View>
                <Switch
                  checked={enabled}
                  onCheckedChange={(checked) => onToggle(group.key, checked)}
                />
              </View>
              {!isLast ? <Separator /> : null}
            </View>
          );
        })}
      </Section>

      <Section
        title={t("groups.system")}
      >
        <View className="flex-row items-center px-4 py-3 gap-3">
          <View className="flex-1">
            <Text className="text-base font-medium text-foreground">
              {t("groups.system_notifications")}
            </Text>
            <Text className="text-xs text-muted-foreground mt-0.5">
              {t("groups.system_description")}
            </Text>
          </View>
          <Switch
            checked={systemEnabled}
            onCheckedChange={(checked) =>
              onToggle("system_notifications", checked)
            }
          />
        </View>
      </Section>
    </ScrollView>
  );
}

function Section({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <View className="gap-2">
      <View className="px-1">
        <Text className="text-xs uppercase tracking-wider text-muted-foreground">
          {title}
        </Text>
        {description ? (
          <Text className="text-xs text-muted-foreground mt-1">
            {description}
          </Text>
        ) : null}
      </View>
      <View className="rounded-md border border-border bg-card overflow-hidden">
        {children}
      </View>
    </View>
  );
}
