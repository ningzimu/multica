// @vitest-environment node

import { describe, expect, it, vi } from "vitest";
import {
  createNotificationRegistrationCoordinator,
  type NotificationRegistrationDependencies,
  type NotificationPermissionStatus,
} from "./notification-registration";

function createDependencies(
  initialPermission: NotificationPermissionStatus = "undetermined",
): NotificationRegistrationDependencies & {
  emitToken: (token: string) => Promise<void>;
} {
  let permission = initialPermission;
  let tokenListener: ((token: string) => void | Promise<void>) | null = null;

  return {
    installation: {
      getOrCreateID: vi.fn().mockResolvedValue("018f6f4f-2dd0-7f47-9f71-2af8d5a8a821"),
    },
    permissionEducation: {
      hasBeenShown: vi.fn().mockResolvedValue(false),
      markShown: vi.fn().mockResolvedValue(undefined),
      explain: vi.fn().mockResolvedValue(true),
    },
    notifications: {
      getPermissionStatus: vi.fn(async () => permission),
      requestPermission: vi.fn(async () => {
        permission = "granted";
        return permission;
      }),
      getDeviceToken: vi.fn().mockResolvedValue("native-token-first"),
      addDeviceTokenListener: vi.fn((listener) => {
        tokenListener = listener;
      }),
    },
    recipientDevices: {
      register: vi.fn().mockResolvedValue(undefined),
      revoke: vi.fn().mockResolvedValue(undefined),
    },
    settings: {
      open: vi.fn().mockResolvedValue(undefined),
    },
    device: {
      platform: "ios",
      bundleID: "vip.example.multica",
      pushEnvironment: "sandbox",
    },
    emitToken: async (token: string) => {
      if (!tokenListener) throw new Error("token listener was not installed");
      await tokenListener(token);
    },
  };
}

describe("Notification Registration coordinator", () => {
  it("requests permission only after authentication and registers the native token", async () => {
    const deps = createDependencies();
    const coordinator = createNotificationRegistrationCoordinator(deps);

    expect(deps.notifications.requestPermission).not.toHaveBeenCalled();
    expect(deps.recipientDevices.register).not.toHaveBeenCalled();

    await expect(coordinator.onAuthenticated()).resolves.toBe("registered");

    expect(deps.permissionEducation.explain).toHaveBeenCalledOnce();
    expect(deps.permissionEducation.markShown).toHaveBeenCalledOnce();
    expect(deps.notifications.requestPermission).toHaveBeenCalledOnce();
    expect(deps.recipientDevices.register).toHaveBeenCalledWith({
      installationID: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a821",
      platform: "ios",
      bundleID: "vip.example.multica",
      pushEnvironment: "sandbox",
      deviceToken: "native-token-first",
    });
  });

  it("respects a denied permission without prompting again", async () => {
    const deps = createDependencies("denied");
    const coordinator = createNotificationRegistrationCoordinator(deps);

    await expect(coordinator.onAuthenticated()).resolves.toBe("denied");
    await expect(coordinator.onAuthenticated()).resolves.toBe("denied");

    expect(deps.permissionEducation.explain).not.toHaveBeenCalled();
    expect(deps.notifications.requestPermission).not.toHaveBeenCalled();
    expect(deps.recipientDevices.register).not.toHaveBeenCalled();
  });

  it("does not repeat education when the user defers permission", async () => {
    const deps = createDependencies();
    vi.mocked(deps.permissionEducation.explain).mockResolvedValue(false);
    const coordinator = createNotificationRegistrationCoordinator(deps);

    await expect(coordinator.onAuthenticated()).resolves.toBe("deferred");
    vi.mocked(deps.permissionEducation.hasBeenShown).mockResolvedValue(true);
    await expect(coordinator.onAuthenticated()).resolves.toBe("deferred");

    expect(deps.permissionEducation.explain).toHaveBeenCalledOnce();
    expect(deps.notifications.requestPermission).not.toHaveBeenCalled();
  });

  it("refreshes a rotated token and revokes the binding on logout", async () => {
    const deps = createDependencies("granted");
    const coordinator = createNotificationRegistrationCoordinator(deps);

    await coordinator.onAuthenticated();
    await deps.emitToken("native-token-rotated");

    expect(deps.recipientDevices.register).toHaveBeenLastCalledWith({
      installationID: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a821",
      platform: "ios",
      bundleID: "vip.example.multica",
      pushEnvironment: "sandbox",
      deviceToken: "native-token-rotated",
    });

    await coordinator.onLogout();
    expect(deps.recipientDevices.revoke).toHaveBeenCalledWith(
      "018f6f4f-2dd0-7f47-9f71-2af8d5a8a821",
    );

    await deps.emitToken("native-token-after-logout");
    expect(deps.recipientDevices.register).toHaveBeenCalledTimes(2);
  });

  it("does not re-register after logout while authentication is in flight", async () => {
    const deps = createDependencies("granted");
    let resolveToken: ((token: string) => void) | undefined;
    vi.mocked(deps.notifications.getDeviceToken).mockImplementation(
      () => new Promise((resolve) => {
        resolveToken = resolve;
      }),
    );
    const coordinator = createNotificationRegistrationCoordinator(deps);

    const authentication = coordinator.onAuthenticated();
    await vi.waitFor(() => expect(resolveToken).toBeDefined());
    const logout = coordinator.onLogout();
    resolveToken?.("native-token-after-logout-started");

    await logout;
    await expect(authentication).resolves.toBe("unavailable");
    expect(deps.recipientDevices.register).not.toHaveBeenCalled();
    expect(deps.recipientDevices.revoke).toHaveBeenCalledOnce();
  });

  it("fails logout closed when the server cannot revoke the binding", async () => {
    const deps = createDependencies("granted");
    vi.mocked(deps.recipientDevices.revoke).mockRejectedValue(
      new Error("offline"),
    );
    const coordinator = createNotificationRegistrationCoordinator(deps);
    await coordinator.onAuthenticated();

    await expect(coordinator.onLogout()).rejects.toThrow("offline");
  });

  it("allows an explicit settings action to request permission and register", async () => {
    const deps = createDependencies();
    vi.mocked(deps.permissionEducation.hasBeenShown).mockResolvedValue(true);
    const coordinator = createNotificationRegistrationCoordinator(deps);

    await expect(coordinator.onAuthenticated()).resolves.toBe("deferred");
    await expect(coordinator.requestPermissionFromSettings()).resolves.toBe(
      "registered",
    );

    expect(deps.notifications.requestPermission).toHaveBeenCalledOnce();
    expect(deps.recipientDevices.register).toHaveBeenCalledOnce();
  });

  it("opens iOS Settings through the settings adapter", async () => {
    const deps = createDependencies("denied");
    const coordinator = createNotificationRegistrationCoordinator(deps);

    await coordinator.openSettings();

    expect(deps.settings.open).toHaveBeenCalledOnce();
  });
});
