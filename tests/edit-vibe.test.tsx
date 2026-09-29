import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

const mock = vi.hoisted(() => ({
  getUser: vi.fn(),
  single: vi.fn(),
  update: vi.fn(),
  write: vi.fn(),
  error: vi.fn(),
  load: vi.fn(),
}));
vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (config: unknown) => config,
  useNavigate: () => vi.fn(),
  Link: ({ children }: { children: unknown }) => children,
}));
vi.mock("../src/lib/auth", () => ({
  useAuth: () => ({ session: { user: { id: "owner", user_metadata: {} } } }),
}));
vi.mock("../src/lib/user-storage", () => ({ getStorageUser: () => "owner" }));
vi.mock("../src/lib/supabase", () => ({
  supabase: { auth: { getUser: mock.getUser }, from: () => ({ update: mock.update }) },
}));
vi.mock("../src/hooks/useFeedSync", () => ({
  useFeedSync: () => ({
    profile: { city: "Miami", major: "Data Science", vibes: "Jazz" },
    savedEvents: [],
    attendedEvents: [],
  }),
  writeStoredProfile: mock.write,
}));
vi.mock("../src/hooks/useEventSources", () => ({
  useEventSources: () => {
    const state = { events: [], loading: false, loaded: true, error: null };
    return {
      picks: state,
      major: state,
      search: state,
      loadPicks: mock.load,
      loadMajor: mock.load,
      runSearch: mock.load,
    };
  },
}));
vi.mock("../src/lib/saved-events", () => ({
  getAttendance: vi.fn(),
  markAttended: vi.fn(),
  dbMarkAttended: vi.fn(),
}));
vi.mock("../src/components/PostGenerator", () => ({ PostGenerator: () => null }));
vi.mock("../src/components/AttendedSurvey", () => ({ AttendedSurvey: () => null }));
vi.mock("sonner", () => ({ toast: { error: mock.error } }));
import { Feed } from "../src/routes/_authenticated.feed";

beforeEach(() => {
  vi.clearAllMocks();
  mock.getUser.mockResolvedValue({ data: { user: { id: "owner" } }, error: null });
  mock.single.mockResolvedValue({ data: { id: "owner" }, error: null });
  mock.update.mockReturnValue({ eq: () => ({ select: () => ({ single: mock.single }) }) });
});
afterEach(cleanup);
test.each(["Search", "Data Science", "My Picks"])(
  "Edit vibe opens from %s and saves the confirmed preference",
  async (source) => {
    render(<Feed />);
    fireEvent.click(screen.getByRole("button", { name: source, exact: true }));
    fireEvent.click(screen.getByRole("button", { name: "Edit vibe" }));
    const editor = screen.getByRole("textbox", { name: "Your vibe" });
    expect((editor as HTMLTextAreaElement).value).toBe("Jazz");
    fireEvent.change(editor, { target: { value: "AI workshops" } });
    fireEvent.click(screen.getByRole("button", { name: "Find My Events" }));
    await waitFor(() => expect(mock.write).toHaveBeenCalledWith({ vibes: "AI workshops" }));
    expect(mock.update).toHaveBeenCalledWith({ vibes: "AI workshops" });
    expect(screen.queryByRole("textbox", { name: "Your vibe" })).toBeNull();
  },
);
test.each(["no session", "missing row", "database error"])(
  "%s does not claim the vibe was saved",
  async (failure) => {
    if (failure === "no session")
      mock.getUser.mockResolvedValue({ data: { user: null }, error: null });
    else
      mock.single.mockResolvedValue({
        data: null,
        error: failure === "database error" ? new Error("offline") : null,
      });
    render(<Feed />);
    fireEvent.click(screen.getByRole("button", { name: "Edit vibe" }));
    fireEvent.click(screen.getByRole("button", { name: "Find My Events" }));
    await waitFor(() => expect(mock.error).toHaveBeenCalled());
    expect(mock.write).not.toHaveBeenCalled();
    expect(screen.getByRole("textbox", { name: "Your vibe" })).toBeTruthy();
  },
);
