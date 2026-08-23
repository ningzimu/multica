export interface NotificationDestination {
  notificationID: string;
  workspaceID: string;
  workspaceSlug: string;
  issueID: string;
  commentID: string;
}

export interface NotificationEventDependencies {
  auth: {
    isAuthenticated: () => boolean;
  };
  inbox: {
    invalidate: (workspaceID: string) => Promise<void>;
  };
  navigation: {
    openIssue: (
      workspaceSlug: string,
      issueID: string,
      commentID: string,
    ) => void;
    openInbox: (workspaceSlug: string) => void;
  };
}

export interface NotificationEventCoordinator {
  onReceived: (data: unknown) => Promise<void>;
  onTapped: (data: unknown) => Promise<void>;
}

export function parseNotificationDestination(
  data: unknown,
): NotificationDestination | null {
  if (!data || typeof data !== "object") return null;
  const raw = data as Record<string, unknown>;
  const value = raw.multica && typeof raw.multica === "object"
    ? (raw.multica as Record<string, unknown>)
    : raw;
  const stringValue = (key: string) =>
    typeof value[key] === "string" ? value[key] : "";
  const notificationID = stringValue("notification_id");
  const workspaceID = stringValue("workspace_id");
  if (!notificationID || !workspaceID) return null;
  return {
    notificationID,
    workspaceID,
    workspaceSlug: stringValue("workspace_slug"),
    issueID: stringValue("issue_id"),
    commentID: stringValue("comment_id"),
  };
}

export function createNotificationEventCoordinator(
  dependencies: NotificationEventDependencies,
): NotificationEventCoordinator {
  return {
    onReceived: async (data) => {
      const destination = parseNotificationDestination(data);
      if (!destination) return;
      await dependencies.inbox.invalidate(destination.workspaceID);
    },
    onTapped: async (data) => {
      const destination = parseNotificationDestination(data);
      if (!destination) return;
      await dependencies.inbox.invalidate(destination.workspaceID);
      if (!dependencies.auth.isAuthenticated() || !destination.workspaceSlug) {
        return;
      }
      if (destination.issueID) {
        dependencies.navigation.openIssue(
          destination.workspaceSlug,
          destination.issueID,
          destination.commentID,
        );
      } else {
        dependencies.navigation.openInbox(destination.workspaceSlug);
      }
    },
  };
}
