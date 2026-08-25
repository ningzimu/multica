// @vitest-environment node

import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  api: {
    getMe: vi.fn(),
    sendCode: vi.fn(),
    verifyCode: vi.fn(),
    setToken: vi.fn(),
  },
  getToken: vi.fn(),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  restoreSlug: vi.fn(),
  onAuthenticated: vi.fn(),
  onLogout: vi.fn(),
}));

vi.mock("./api", () => ({
  api: mocks.api,
  ApiError: class ApiError extends Error {
    constructor(public status: number) {
      super(`API error ${status}`);
    }
  },
}));

vi.mock("./secure-storage", () => ({
  getToken: mocks.getToken,
  setToken: mocks.setToken,
  clearToken: mocks.clearToken,
}));

vi.mock("./workspace-store", () => ({
  useWorkspaceStore: {
    getState: () => ({ restoreSlug: mocks.restoreSlug }),
  },
}));

vi.mock("./native-notification-registration", () => ({
  notificationRegistration: {
    onAuthenticated: mocks.onAuthenticated,
    onLogout: mocks.onLogout,
  },
}));

import { useAuthStore } from "./auth-store";

const user = {
  id: "018f6f4f-2dd0-7f47-9f71-2af8d5a8a841",
  email: "mobile@example.com",
  name: "Mobile User",
  avatar_url: null,
  onboarded_at: null,
  onboarding_questionnaire: {},
  starter_content_state: null,
  language: null,
  profile_description: "",
  timezone: null,
  created_at: "2026-08-24T00:00:00Z",
  updated_at: "2026-08-24T00:00:00Z",
};

describe("mobile auth notification registration", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.restoreSlug.mockResolvedValue(undefined);
    mocks.setToken.mockResolvedValue(undefined);
    mocks.clearToken.mockResolvedValue(undefined);
    mocks.onAuthenticated.mockResolvedValue("registered");
    mocks.onLogout.mockResolvedValue(undefined);
    useAuthStore.setState({ user: null, isLoading: true });
  });

  it("registers notifications after restoring an authenticated session", async () => {
    mocks.getToken.mockResolvedValue("restored-token");
    mocks.api.getMe.mockResolvedValue(user);

    await useAuthStore.getState().initialize();

    expect(mocks.api.setToken).toHaveBeenCalledWith("restored-token");
    expect(mocks.onAuthenticated).toHaveBeenCalledOnce();
    expect(useAuthStore.getState().user).toEqual(user);
  });

  it("registers notifications only after verification stores the token", async () => {
    mocks.api.verifyCode.mockResolvedValue({ token: "new-token", user });

    await useAuthStore.getState().verifyCode(user.email, "123456");

    expect(mocks.setToken).toHaveBeenCalledWith("new-token");
    expect(mocks.api.setToken).toHaveBeenCalledWith("new-token");
    expect(mocks.onAuthenticated).toHaveBeenCalledOnce();
  });

  it("revokes notifications before clearing the authenticated token", async () => {
    const order: string[] = [];
    mocks.onLogout.mockImplementation(async () => {
      order.push("revoke");
    });
    mocks.clearToken.mockImplementation(async () => {
      order.push("clear");
    });

    await useAuthStore.getState().logout();

    expect(order).toEqual(["revoke", "clear"]);
    expect(mocks.api.setToken).toHaveBeenCalledWith(null);
    expect(useAuthStore.getState().user).toBeNull();
  });

  it("keeps the session when device revocation fails", async () => {
    useAuthStore.setState({ user, isLoading: false });
    mocks.onLogout.mockRejectedValue(new Error("offline"));

    await expect(useAuthStore.getState().logout()).rejects.toThrow("offline");

    expect(mocks.clearToken).not.toHaveBeenCalled();
    expect(mocks.api.setToken).not.toHaveBeenCalledWith(null);
    expect(useAuthStore.getState().user).toEqual(user);
  });
});
