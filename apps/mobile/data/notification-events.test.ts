// @vitest-environment node

import { describe, expect, it, vi } from "vitest";
import { createNotificationEventCoordinator } from "./notification-events";

const payload = {
  multica: {
    notification_id: "notification-1",
    workspace_id: "workspace-1",
    workspace_slug: "personal",
    issue_id: "issue-1",
    comment_id: "comment-1",
  },
};

function dependencies(authenticated = true) {
  return {
    auth: { isAuthenticated: vi.fn(() => authenticated) },
    inbox: { invalidate: vi.fn().mockResolvedValue(undefined) },
    navigation: {
      openIssue: vi.fn(),
      openInbox: vi.fn(),
    },
  };
}

describe("notification event coordinator", () => {
  it("refreshes the source workspace Inbox on foreground receipt", async () => {
    const deps = dependencies();
    await createNotificationEventCoordinator(deps).onReceived(payload);
    expect(deps.inbox.invalidate).toHaveBeenCalledWith("workspace-1");
    expect(deps.navigation.openIssue).not.toHaveBeenCalled();
  });

  it("opens the exact issue and comment after a tap", async () => {
    const deps = dependencies();
    await createNotificationEventCoordinator(deps).onTapped(payload);
    expect(deps.navigation.openIssue).toHaveBeenCalledWith(
      "personal",
      "issue-1",
      "comment-1",
    );
  });

  it("falls back to Inbox when the payload has no issue", async () => {
    const deps = dependencies();
    await createNotificationEventCoordinator(deps).onTapped({
      multica: { ...payload.multica, issue_id: "" },
    });
    expect(deps.navigation.openInbox).toHaveBeenCalledWith("personal");
  });

  it("does not navigate a logged-out user or malformed payload", async () => {
    const deps = dependencies(false);
    const coordinator = createNotificationEventCoordinator(deps);
    await coordinator.onTapped(payload);
    await coordinator.onTapped({ unexpected: true });
    expect(deps.navigation.openIssue).not.toHaveBeenCalled();
    expect(deps.navigation.openInbox).not.toHaveBeenCalled();
  });
});
