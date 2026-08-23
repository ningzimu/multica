import { afterEach, describe, expect, it } from "vitest";
import type { ConfigContext } from "expo/config";
import createConfig from "../app.config";

const originalAppEnv = process.env.APP_ENV;
const originalProductionBundleID = process.env.EXPO_BUNDLE_IDENTIFIER_PROD;

afterEach(() => {
  if (originalAppEnv === undefined) delete process.env.APP_ENV;
  else process.env.APP_ENV = originalAppEnv;

  if (originalProductionBundleID === undefined) {
    delete process.env.EXPO_BUNDLE_IDENTIFIER_PROD;
  } else {
    process.env.EXPO_BUNDLE_IDENTIFIER_PROD = originalProductionBundleID;
  }
});

function configFor(environment: "development" | "production") {
  process.env.APP_ENV = environment;
  process.env.EXPO_BUNDLE_IDENTIFIER_PROD = "vip.example.multica";
  return createConfig({
    config: { name: "Multica", slug: "multica-mobile" },
  } as ConfigContext);
}

describe("iOS notification build configuration", () => {
  it("generates a sandbox entitlement for development", () => {
    const config = configFor("development");

    expect(config.ios?.entitlements?.["aps-environment"]).toBe("development");
    expect(config.extra?.APNS_ENVIRONMENT).toBe("sandbox");
    expect(config.plugins).toContain("expo-notifications");
  });

  it("generates a production entitlement for the self-hosted Release variant", () => {
    const config = configFor("production");

    expect(config.ios?.bundleIdentifier).toBe("vip.example.multica");
    expect(config.ios?.entitlements?.["aps-environment"]).toBe("production");
    expect(config.extra?.APNS_ENVIRONMENT).toBe("production");
  });
});
