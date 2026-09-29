/** One SSE request, without reconnects or credentials in the URL. */
export class EventStream {
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  private controller = new AbortController();

  constructor(url: string, token: string) {
    void this.read(url, token);
  }

  close() {
    this.controller.abort();
  }

  private async read(url: string, token: string) {
    try {
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${token}`, Accept: "text/event-stream" },
        signal: this.controller.signal,
        cache: "no-store",
        referrerPolicy: "no-referrer",
      });
      if (!response.ok || !response.body) throw new Error("Stream unavailable");
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let data: string[] = [];
      try {
        while (!this.controller.signal.aborted) {
          const chunk = await reader.read();
          if (chunk.done) break;
          buffer += decoder.decode(chunk.value, { stream: true });
          let end: number;
          while ((end = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, end).replace(/\r$/, "");
            buffer = buffer.slice(end + 1);
            if (!line && data.length) {
              this.onmessage?.({ data: data.join("\n") });
              data = [];
              if (this.controller.signal.aborted) return;
            } else if (line.startsWith("data:")) {
              data.push(line.slice(5).replace(/^ /, ""));
            }
          }
        }
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
      // A complete/error SSE frame closes us from the hook. EOF before that
      // is an interrupted response, not a successful empty search.
      if (!this.controller.signal.aborted) this.onerror?.();
    } catch {
      if (!this.controller.signal.aborted) this.onerror?.();
    }
  }
}
