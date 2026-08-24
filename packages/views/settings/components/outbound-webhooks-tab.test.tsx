// @vitest-environment jsdom

import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nProvider } from "@multica/core/i18n/react";
import enCommon from "../../locales/en/common.json";
import enSettings from "../../locales/en/settings.json";

const mockList = vi.hoisted(() => vi.fn());
const mockGet = vi.hoisted(() => vi.fn());
const mockCreate = vi.hoisted(() => vi.fn());
const mockDelete = vi.hoisted(() => vi.fn());
const mockToastError = vi.hoisted(() => vi.fn());

vi.mock("@multica/core/api", () => ({
  api: {
    listOutboundWebhookSubscriptions: mockList,
    getOutboundWebhookSubscription: mockGet,
    createOutboundWebhookSubscription: mockCreate,
    deleteOutboundWebhookSubscription: mockDelete,
  },
}));

vi.mock("@multica/core/paths", () => ({
  useCurrentWorkspace: () => ({ id: "workspace-1", name: "Acme", slug: "acme" }),
}));

vi.mock("@multica/core/permissions", () => ({
  useCurrentMember: () => ({ role: "owner", isLoading: false }),
}));

vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: mockToastError } }));

import { OutboundWebhooksTab } from "./outbound-webhooks-tab";

function Wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return (
    <QueryClientProvider client={client}>
      <I18nProvider locale="en" resources={{ en: { common: enCommon, settings: enSettings } }}>
        {children}
      </I18nProvider>
    </QueryClientProvider>
  );
}

describe("OutboundWebhooksTab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockList.mockResolvedValue({ subscriptions: [] });
    mockCreate.mockResolvedValue({
      subscription: { id: "sub-1", workspaceId: "workspace-1", name: "Receiver", destinationHint: "https://example.com", events: ["issue.created"], eventCatalogVersion: 1, scopeMode: "workspace", createdAt: "now", updatedAt: "now" },
      signingSecret: "whsec_once",
    });
    mockGet.mockResolvedValue({ id: "sub-1", workspaceId: "workspace-1", name: "Receiver", destinationHint: "https://example.com", events: ["issue.created"], eventCatalogVersion: 1, scopeMode: "workspace", createdAt: "now", updatedAt: "now" });
  });

  it("creates an explicit issue.created workspace subscription and discloses its secret once", async () => {
    const user = userEvent.setup();
    render(<OutboundWebhooksTab />, { wrapper: Wrapper });

    await user.type(screen.getByLabelText("Name"), "Receiver");
    await user.type(screen.getByLabelText("Destination URL"), "https://example.com/events");
    expect(screen.getByRole("checkbox", { name: "issue.created" })).toBeChecked();
    await user.click(screen.getByRole("button", { name: "Create webhook" }));

    await waitFor(() => expect(mockCreate).toHaveBeenCalledWith("workspace-1", {
      name: "Receiver",
      destination: "https://example.com/events",
      events: ["issue.created"],
      scopeMode: "workspace",
    }));
    expect(await screen.findByText("whsec_once")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "I saved it" }));
    expect(screen.queryByText("whsec_once")).not.toBeInTheDocument();
  });

  it("loads a safe detail view for inspection", async () => {
    mockList.mockResolvedValue({ subscriptions: [
      { id: "sub-1", workspaceId: "workspace-1", name: "Receiver", destinationHint: "https://example.com", events: ["issue.created"], eventCatalogVersion: 1, scopeMode: "workspace", createdAt: "now", updatedAt: "now" },
    ] });
    const user = userEvent.setup();
    render(<OutboundWebhooksTab />, { wrapper: Wrapper });

    await user.click(await screen.findByRole("button", { name: "Inspect" }));
    await waitFor(() => expect(mockGet).toHaveBeenCalledWith("workspace-1", "sub-1"));
    expect(await screen.findByText("Event catalog version")).toBeInTheDocument();
    expect(screen.queryByText("whsec_once")).not.toBeInTheDocument();
  });

  it("keeps the form and reports when the one-time secret is missing", async () => {
    mockCreate.mockResolvedValue({
      subscription: { id: "", workspaceId: "", name: "", destinationHint: "", events: [], eventCatalogVersion: 1, scopeMode: "workspace", createdAt: "", updatedAt: "" },
      signingSecret: "",
    });
    const user = userEvent.setup();
    render(<OutboundWebhooksTab />, { wrapper: Wrapper });

    await user.type(screen.getByLabelText("Name"), "Receiver");
    await user.type(screen.getByLabelText("Destination URL"), "https://example.com/events");
    await user.click(screen.getByRole("button", { name: "Create webhook" }));

    await waitFor(() => expect(mockToastError).toHaveBeenCalledWith(expect.stringContaining("may have been created")));
    expect(screen.getByLabelText("Name")).toHaveValue("Receiver");
    expect(screen.queryByText("whsec_once")).not.toBeInTheDocument();
  });
});
