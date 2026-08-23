// @vitest-environment node

import { describe, expect, it } from "vitest";
import { RecipientDeviceResponseSchema } from "./schemas";

const wireDevice = {
  id: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a851",
  installation_id: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a852",
  user_id: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a853",
  platform: "ios",
  bundle_id: "vip.example.multica",
  push_environment: "sandbox",
  enabled: true,
  bound_at: "2026-08-24T00:00:00Z",
  revoked_at: null,
  invalidated_at: null,
  last_seen_at: "2026-08-24T00:00:00Z",
  created_at: "2026-08-24T00:00:00Z",
  updated_at: "2026-08-24T00:00:00Z",
};

describe("RecipientDeviceResponseSchema", () => {
  it("converts the wire response to camelCase", () => {
    expect(RecipientDeviceResponseSchema.parse(wireDevice)).toMatchObject({
      installationID: wireDevice.installation_id,
      userID: wireDevice.user_id,
      bundleID: wireDevice.bundle_id,
      pushEnvironment: "sandbox",
      lastSeenAt: wireDevice.last_seen_at,
    });
  });

  it("rejects missing or malformed required fields", () => {
    const { bundle_id: _missing, ...withoutBundleID } = wireDevice;
    expect(RecipientDeviceResponseSchema.safeParse(withoutBundleID).success).toBe(
      false,
    );
    expect(
      RecipientDeviceResponseSchema.safeParse({
        ...wireDevice,
        push_environment: "unknown",
      }).success,
    ).toBe(false);
  });
});
