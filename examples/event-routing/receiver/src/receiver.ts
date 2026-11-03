// Receive what Ankusa's HTTP sink sends. Only hooks whose event type passes the
// sink's filter (`filter: {event_type: [invoice.paid]}`) ever arrive here.
//
// Every delivery is signed (Standard Webhooks) with the sink's secret, so the
// receiver refuses anything it cannot verify: `receiveDelivery` from the
// `ankusa` SDK does both the verification and the header parsing. The event
// type is the `x-ankusa-event-type` header.
import { createServer } from "node:http";

import { InvalidSignatureError, MissingHookIdError, receiveDelivery } from "ankusa";

const port = Number(process.env.PORT ?? "3000");
const secret = process.env.SINK_WHSEC;
if (secret === undefined || secret === "") {
  console.error("SINK_WHSEC is required: the sink's secret, to verify deliveries");
  process.exit(1);
}

const server = createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (chunk: Buffer) => chunks.push(chunk));
  req.on("end", () => {
    if (req.method !== "POST" || req.url !== "/invoices") {
      res.writeHead(404).end();
      return;
    }

    const body = Buffer.concat(chunks);
    try {
      const hook = receiveDelivery({ headers: req.headers, body, secrets: secret });
      const eventType = req.headers["x-ankusa-event-type"] ?? "null";
      console.log(
        `received id=${hook.id} source=${hook.source} event_type=${String(eventType)} verified=${hook.verified} body=${body.toString("utf8")}`,
      );
      res.writeHead(204).end();
    } catch (error) {
      if (error instanceof InvalidSignatureError) {
        console.log(`refused: ${error.message}`);
        res.writeHead(401).end();
      } else if (error instanceof MissingHookIdError) {
        res.writeHead(400).end();
      } else {
        throw error;
      }
    }
  });
});

server.listen(port, "0.0.0.0", () => {
  console.log(`receiver listening on :${port}/invoices`);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
