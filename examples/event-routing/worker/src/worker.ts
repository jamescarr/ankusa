// Consume the topics Ankusa's templated Kafka sink produces to. The topic is
// named by the hook's event type (`events.{{ event_type ?? other }}`), so one
// source lands on several topics; this worker reads them all.
//
// Each record is an Ankusa message: `decodeMessage` (the `ankusa` SDK) gives
// the hook's id and, since the source says `event_type: {json: type}`, its
// `event_type` too. The body travels inline (these hooks are tiny).
import { decodeMessage } from "ankusa";
import { Kafka, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "redpanda:9092").split(",");
const topics = ["events.invoice.paid", "events.other"];

const kafka = new Kafka({ clientId: "event-routing-worker", brokers, logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId: "event-routing-worker" });

async function main(): Promise<void> {
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (message.value === null) return;
      const hook = decodeMessage(message.value);
      const body = hook.body === null ? "(claim)" : Buffer.from(hook.body).toString("utf8");
      console.log(`received topic=${topic} id=${hook.id} event_type=${hook.event_type ?? "null"} body=${body}`);
    },
  });

  console.log(`worker consuming ${topics.join(", ")} from ${brokers.join(",")}`);
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void consumer.disconnect().finally(() => process.exit(0));
  });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
