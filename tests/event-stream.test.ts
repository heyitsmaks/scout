import { afterEach, expect, test, vi } from "vitest";
import { EventStream } from "../src/lib/event-stream";

afterEach(() => vi.unstubAllGlobals());
test("streams split UTF-8 and CRLF frames with header auth, without reconnecting", async () => {
  const bytes = new TextEncoder().encode(
    ': keepalive\r\ndata: {"name":"café"}\r\n\r\ndata: {"status":"complete"}\n\n',
  );
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(
      new ReadableStream({
        start(controller) {
          for (const byte of bytes) controller.enqueue(new Uint8Array([byte]));
          controller.close();
        },
      }),
    ),
  );
  vi.stubGlobal("fetch", fetchMock);
  const stream = new EventStream("https://api.example.test/events?city=Miami", "private-token");
  const messages: unknown[] = [];
  stream.onerror = vi.fn();
  await new Promise<void>((resolve) => {
    stream.onmessage = ({ data }) => {
      const message = JSON.parse(data);
      messages.push(message);
      if (message.status === "complete") {
        stream.close();
        resolve();
      }
    };
  });
  expect(messages).toEqual([{ name: "café" }, { status: "complete" }]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const [url, options] = fetchMock.mock.calls[0];
  expect(url).not.toContain("private-token");
  expect(options.headers.Authorization).toBe("Bearer private-token");
  expect(stream.onerror).not.toHaveBeenCalled();
});
test.each([401, 429, 503])("HTTP %s surfaces failure instead of empty success", async (status) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status })));
  const stream = new EventStream("https://api.example.test/events", "token");
  await new Promise<void>((resolve) => {
    stream.onerror = resolve;
  });
});
test("EOF before complete is reported as an interrupted stream", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('data: {"events":[]}\n\n')));
  const stream = new EventStream("https://api.example.test/events", "token");
  await new Promise<void>((resolve) => {
    stream.onerror = resolve;
  });
});
