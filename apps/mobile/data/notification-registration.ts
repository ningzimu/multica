export type NotificationPermissionStatus =
  | "granted"
  | "denied"
  | "undetermined";

export type PushEnvironment = "sandbox" | "production";

export type NotificationRegistrationResult =
  | "registered"
  | "denied"
  | "deferred"
  | "unavailable";

export interface RecipientDeviceRegistration {
  installationID: string;
  platform: "ios";
  bundleID: string;
  pushEnvironment: PushEnvironment;
  deviceToken: string;
}

export interface NotificationRegistrationDependencies {
  installation: {
    getOrCreateID: () => Promise<string>;
  };
  permissionEducation: {
    hasBeenShown: () => Promise<boolean>;
    markShown: () => Promise<void>;
    explain: () => Promise<boolean>;
  };
  notifications: {
    getPermissionStatus: () => Promise<NotificationPermissionStatus>;
    requestPermission: () => Promise<NotificationPermissionStatus>;
    getDeviceToken: () => Promise<string>;
    addDeviceTokenListener: (
      listener: (token: string) => void | Promise<void>,
    ) => void;
  };
  recipientDevices: {
    register: (registration: RecipientDeviceRegistration) => Promise<void>;
    revoke: (installationID: string) => Promise<void>;
  };
  settings: {
    open: () => Promise<void>;
  };
  device: {
    platform: "ios";
    bundleID: string;
    pushEnvironment: PushEnvironment;
  };
}

export interface NotificationRegistrationCoordinator {
  onAuthenticated: () => Promise<NotificationRegistrationResult>;
  requestPermissionFromSettings: () => Promise<NotificationRegistrationResult>;
  onLogout: () => Promise<void>;
  getPermissionStatus: () => Promise<NotificationPermissionStatus>;
  openSettings: () => Promise<void>;
}

export function createNotificationRegistrationCoordinator(
  dependencies: NotificationRegistrationDependencies,
): NotificationRegistrationCoordinator {
  let authenticated = false;
  let authenticationGeneration = 0;
  let listeningForTokenChanges = false;
  let authenticationWork: Promise<NotificationRegistrationResult> | null = null;
  let deviceMutationWork: Promise<void> = Promise.resolve();

  const registerToken = (
    deviceToken: string,
    generation: number,
  ): Promise<void> => {
    deviceMutationWork = deviceMutationWork.catch(() => undefined).then(async () => {
      if (!authenticated || generation !== authenticationGeneration) return;
      const installationID = await dependencies.installation.getOrCreateID();
      if (!authenticated || generation !== authenticationGeneration) return;
      await dependencies.recipientDevices.register({
        installationID,
        platform: dependencies.device.platform,
        bundleID: dependencies.device.bundleID,
        pushEnvironment: dependencies.device.pushEnvironment,
        deviceToken,
      });
    });
    return deviceMutationWork;
  };

  const listenForTokenChanges = (): void => {
    if (listeningForTokenChanges) return;
    listeningForTokenChanges = true;
    dependencies.notifications.addDeviceTokenListener(async (deviceToken) => {
      if (!authenticated) return;
      const generation = authenticationGeneration;
      try {
        await registerToken(deviceToken, generation);
      } catch {
        // Token refresh is best-effort. A later authenticated sync retries with
        // the current native token without interrupting the user's session.
      }
    });
  };

  const authenticate = async (): Promise<NotificationRegistrationResult> => {
    authenticated = true;
    const generation = ++authenticationGeneration;
    listenForTokenChanges();

    let permission = await dependencies.notifications.getPermissionStatus();
    if (permission === "denied") return "denied";

    if (permission === "undetermined") {
      if (await dependencies.permissionEducation.hasBeenShown()) {
        return "deferred";
      }

      await dependencies.permissionEducation.markShown();
      const shouldRequest = await dependencies.permissionEducation.explain();
      if (!shouldRequest) return "deferred";

      permission = await dependencies.notifications.requestPermission();
      if (permission === "denied") return "denied";
      if (permission !== "granted") return "deferred";
    }

    try {
      const token = await dependencies.notifications.getDeviceToken();
      if (!authenticated || generation !== authenticationGeneration) {
        return "unavailable";
      }
      await registerToken(token, generation);
      return "registered";
    } catch {
      return "unavailable";
    }
  };

  return {
    onAuthenticated: async () => {
      if (!authenticationWork) {
        authenticationWork = authenticate()
          .catch(() => "unavailable" as const)
          .finally(() => {
            authenticationWork = null;
          });
      }
      return authenticationWork;
    },

    requestPermissionFromSettings: async () => {
      authenticated = true;
      const generation = ++authenticationGeneration;
      listenForTokenChanges();

      let permission = await dependencies.notifications.getPermissionStatus();
      if (permission === "undetermined") {
        permission = await dependencies.notifications.requestPermission();
      }
      if (permission === "denied") return "denied";
      if (permission !== "granted") return "deferred";

      try {
        const token = await dependencies.notifications.getDeviceToken();
        if (!authenticated || generation !== authenticationGeneration) {
          return "unavailable";
        }
        await registerToken(token, generation);
        return "registered";
      } catch {
        return "unavailable";
      }
    },

    onLogout: async () => {
      authenticated = false;
      authenticationGeneration += 1;
      try {
        await deviceMutationWork.catch(() => undefined);
        const installationID = await dependencies.installation.getOrCreateID();
        await dependencies.recipientDevices.revoke(installationID);
      } catch {
        // Local logout must continue even when the server is unreachable or
        // the session has already expired. A future login safely rebinds the
        // installation before it becomes eligible again.
      }
    },

    getPermissionStatus: () =>
      dependencies.notifications.getPermissionStatus(),

    openSettings: () => dependencies.settings.open(),
  };
}
