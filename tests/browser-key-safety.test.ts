import { expect, test } from "vitest";
import { assertBrowserSafeEnvironment } from "../src/lib/browser-key-safety";

const jwt = (role: string) =>
  `eyJhbGciOiJIUzI1NiJ9.${btoa(JSON.stringify({ role }))}.test-signature`;
test("publishable keys and legacy anon keys are allowed", () => {
  expect(() =>
    assertBrowserSafeEnvironment({ VITE_SUPABASE_KEY: "sb_publishable_test" }),
  ).not.toThrow();
  expect(() => assertBrowserSafeEnvironment({ VITE_SUPABASE_KEY: jwt("anon") })).not.toThrow();
});
test("a server key in any VITE variable blocks the build without printing the key", () => {
  const secret = "sb_secret_test_only_sentinel";
  let message = "";
  try {
    assertBrowserSafeEnvironment({ VITE_MISNAMED_VALUE: secret });
  } catch (error) {
    message = (error as Error).message;
  }
  expect(message).toContain("VITE_MISNAMED_VALUE contains a server credential");
  expect(message).not.toContain(secret);
});
test("legacy service-role and user tokens cannot be published either", () => {
  for (const role of ["service_role", "authenticated"]) {
    expect(() => assertBrowserSafeEnvironment({ VITE_SUPABASE_KEY: jwt(role) })).toThrow();
  }
});
test("server-only variables are not treated as client environment", () => {
  expect(() =>
    assertBrowserSafeEnvironment({ SUPABASE_SECRET_KEY: "sb_secret_test" }),
  ).not.toThrow();
});
