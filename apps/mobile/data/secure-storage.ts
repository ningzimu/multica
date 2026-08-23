/**
 * Thin wrapper around expo-secure-store for the auth token.
 * Keyed identically to web/desktop ("multica_token") so logic stays aligned
 * with packages/core/auth/store.ts even though storage backends differ.
 */
import * as SecureStore from "expo-secure-store";
import * as Crypto from "expo-crypto";

const TOKEN_KEY = "multica_token";
const NOTIFICATION_INSTALLATION_ID_KEY = "multica_notification_installation_id";
const NOTIFICATION_EDUCATION_SEEN_KEY = "multica_notification_education_seen";

export async function getToken(): Promise<string | null> {
  return SecureStore.getItemAsync(TOKEN_KEY);
}

export async function setToken(token: string): Promise<void> {
  await SecureStore.setItemAsync(TOKEN_KEY, token);
}

export async function clearToken(): Promise<void> {
  await SecureStore.deleteItemAsync(TOKEN_KEY);
}

export async function getOrCreateNotificationInstallationID(): Promise<string> {
  const existing = await SecureStore.getItemAsync(
    NOTIFICATION_INSTALLATION_ID_KEY,
  );
  if (existing) return existing;

  const created = Crypto.randomUUID();
  await SecureStore.setItemAsync(NOTIFICATION_INSTALLATION_ID_KEY, created);
  return created;
}

export async function hasSeenNotificationPermissionEducation(): Promise<boolean> {
  return (
    (await SecureStore.getItemAsync(NOTIFICATION_EDUCATION_SEEN_KEY)) === "true"
  );
}

export async function markNotificationPermissionEducationSeen(): Promise<void> {
  await SecureStore.setItemAsync(NOTIFICATION_EDUCATION_SEEN_KEY, "true");
}
