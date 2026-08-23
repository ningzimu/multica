import { router } from "expo-router";
import * as Notifications from "expo-notifications";
import { useAuthStore } from "./auth-store";
import { createNotificationEventCoordinator } from "./notification-events";
import { queryClient } from "./query-client";
import { inboxKeys } from "./queries/inbox";

Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowBanner: false,
    shouldShowList: false,
    shouldPlaySound: false,
    shouldSetBadge: false,
  }),
});

const coordinator = createNotificationEventCoordinator({
  auth: {
    isAuthenticated: () => useAuthStore.getState().user !== null,
  },
  inbox: {
    invalidate: async (workspaceID) => {
      await queryClient.invalidateQueries({ queryKey: inboxKeys.list(workspaceID) });
    },
  },
  navigation: {
    openIssue: (workspaceSlug, issueID, commentID) => {
      router.push({
        pathname: "/[workspace]/issue/[id]",
        params: {
          workspace: workspaceSlug,
          id: issueID,
          highlight: commentID || undefined,
          h: String(Date.now()),
        },
      });
    },
    openInbox: (workspaceSlug) => {
      router.push(`/${workspaceSlug}/inbox`);
    },
  },
});

function responseData(response: Notifications.NotificationResponse): unknown {
  return response.notification.request.content.data;
}

export function installNativeNotificationEventBridge(): () => void {
  const received = Notifications.addNotificationReceivedListener((notification) => {
    void coordinator
      .onReceived(notification.request.content.data)
      .catch(() => undefined);
  });
  const tapped = Notifications.addNotificationResponseReceivedListener((response) => {
    void coordinator.onTapped(responseData(response)).catch(() => undefined);
  });

  void Notifications.getLastNotificationResponseAsync().then((response) => {
    if (!response) return;
    void coordinator.onTapped(responseData(response)).catch(() => undefined);
    void Notifications.clearLastNotificationResponseAsync();
  }).catch(() => undefined);

  return () => {
    received.remove();
    tapped.remove();
  };
}
