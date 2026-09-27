"""Receive webhooks from Ankusa's HTTP sink. Uses the `ankusa` SDK
(sdks/python, a uv path dependency, see pyproject.toml) to parse the
identity Ankusa attaches to every delivery.

Ankusa POSTs each hook's raw body here, with its identity in headers:
x-ankusa-id (dedupe on this), x-ankusa-source, x-ankusa-seq, and
x-ankusa-tenant when the source has one. Answer 2xx once the hook is safely
handled; anything else (or no answer within 5s) is retried, then
dead-lettered for replay.
"""

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ankusa import MissingHookIdError, parse_headers

PORT = int(os.environ.get("PORT", "8080"))

# In memory for the demo. A real worker records handled ids durably (a unique
# key in its database), so a redelivery is a no-op even across restarts.
handled: set[str] = set()


def handle(source: str, body: bytes) -> None:
    """Your business logic goes here."""


class Hooks(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/hooks":
            self.send_response(404)
            self.end_headers()
            return

        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        try:
            hook = parse_headers(self.headers)
        except MissingHookIdError:
            self.send_response(400)
            self.end_headers()
            return

        if hook.id in handled:
            print(f"duplicate id={hook.id} source={hook.source} (already handled)", flush=True)
        else:
            handle(hook.source, body)
            handled.add(hook.id)
            print(
                f"received id={hook.id} source={hook.source} "
                f"seq={hook.seq} bytes={len(body)} "
                f"body={body[:200].decode('utf-8', 'replace')}",
                flush=True,
            )

        self.send_response(204)
        self.end_headers()

    def log_message(self, *args) -> None:
        pass  # one line per hook, printed above


if __name__ == "__main__":
    print(f"worker listening on :{PORT}/hooks", flush=True)
    ThreadingHTTPServer(("", PORT), Hooks).serve_forever()
