"""Receive webhooks from Ankusa's HTTP sink. Uses the `ankusa` SDK
(packages/sdk-python, a uv path dependency, see pyproject.toml) to parse the
identity Ankusa attaches to every delivery.

Ankusa POSTs each hook's raw body here, with its identity in headers:
x-ankusa-id (dedupe on this), x-ankusa-source, and x-ankusa-tenant when the
source has one. Answer 2xx once the hook is safely handled; anything else (or
no answer within 5s) is retried, then dead-lettered for replay.
"""

import os

import uvicorn
from ankusa import MissingHookIdError, parse_headers
from fastapi import FastAPI, Request, Response

PORT = int(os.environ.get("PORT", "8080"))

app = FastAPI()

# In memory for the demo. A real worker records handled ids durably (a unique
# key in its database), so a redelivery is a no-op even across restarts.
handled: set[str] = set()


def handle(source: str, body: bytes) -> None:
    """Your business logic goes here."""


@app.post("/hooks")
async def hooks(request: Request) -> Response:
    body = await request.body()
    try:
        hook = parse_headers(request.headers)
    except MissingHookIdError:
        return Response(status_code=400)

    if hook.id in handled:
        print(f"duplicate id={hook.id} source={hook.source} (already handled)", flush=True)
    else:
        handle(hook.source, body)
        handled.add(hook.id)
        print(
            f"received id={hook.id} source={hook.source} "
            f"bytes={len(body)} "
            f"body={body[:200].decode('utf-8', 'replace')}",
            flush=True,
        )

    return Response(status_code=204)


if __name__ == "__main__":
    print(f"worker listening on :{PORT}/hooks", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
