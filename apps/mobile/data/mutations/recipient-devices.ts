import { api } from "@/data/api";
import type { RecipientDeviceRegistration } from "@/data/notification-registration";

export async function registerRecipientDevice(
  registration: RecipientDeviceRegistration,
): Promise<void> {
  await api.registerRecipientDevice(registration);
}

export async function revokeRecipientDevice(
  installationID: string,
): Promise<void> {
  await api.revokeRecipientDevice(installationID);
}
