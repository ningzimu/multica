import { Alert, Linking } from "react-native";
import Constants from "expo-constants";
import * as Notifications from "expo-notifications";
import {
  registerRecipientDevice,
  revokeRecipientDevice,
} from "./mutations/recipient-devices";
import {
  createNotificationRegistrationCoordinator,
  type NotificationPermissionStatus,
  type PushEnvironment,
} from "./notification-registration";
import {
  getOrCreateNotificationInstallationID,
  hasSeenNotificationPermissionEducation,
  markNotificationPermissionEducationSeen,
} from "./secure-storage";

function permissionStatus(
  status: Notifications.PermissionStatus,
): NotificationPermissionStatus {
  if (status === Notifications.PermissionStatus.GRANTED) return "granted";
  if (status === Notifications.PermissionStatus.DENIED) return "denied";
  return "undetermined";
}

function explainNotificationPermission(): Promise<boolean> {
  return new Promise((resolve) => {
    Alert.alert(
      "Stay up to date",
      "Allow Multica to notify you about activity while the app is in the background.",
      [
        { text: "Not now", style: "cancel", onPress: () => resolve(false) },
        { text: "Enable notifications", onPress: () => resolve(true) },
      ],
      { cancelable: false },
    );
  });
}

function configuredPushEnvironment(): PushEnvironment {
  return Constants.expoConfig?.extra?.APNS_ENVIRONMENT === "production"
    ? "production"
    : "sandbox";
}

export const notificationRegistration =
  createNotificationRegistrationCoordinator({
    installation: {
      getOrCreateID: getOrCreateNotificationInstallationID,
    },
    permissionEducation: {
      hasBeenShown: hasSeenNotificationPermissionEducation,
      markShown: markNotificationPermissionEducationSeen,
      explain: explainNotificationPermission,
    },
    notifications: {
      getPermissionStatus: async () =>
        permissionStatus((await Notifications.getPermissionsAsync()).status),
      requestPermission: async () =>
        permissionStatus(
          (
            await Notifications.requestPermissionsAsync({
              ios: {
                allowAlert: true,
                allowBadge: true,
                allowSound: true,
              },
            })
          ).status,
        ),
      getDeviceToken: async () => {
        const token = await Notifications.getDevicePushTokenAsync();
        if (typeof token.data !== "string" || token.data.length === 0) {
          throw new Error("native notification token is unavailable");
        }
        return token.data;
      },
      addDeviceTokenListener: (listener) => {
        Notifications.addPushTokenListener((token) => {
          if (typeof token.data === "string" && token.data.length > 0) {
            void listener(token.data);
          }
        });
      },
    },
    recipientDevices: {
      register: registerRecipientDevice,
      revoke: revokeRecipientDevice,
    },
    settings: {
      open: () => Linking.openSettings(),
    },
    device: {
      platform: "ios",
      bundleID: Constants.expoConfig?.ios?.bundleIdentifier ?? "",
      pushEnvironment: configuredPushEnvironment(),
    },
  });
