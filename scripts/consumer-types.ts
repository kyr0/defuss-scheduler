/** VERIFIED: The installed declarations allow parsed typed payloads and reject numeric worker identities. */
import { type Queue, defineHandler } from "defuss-scheduler";
import { startWorker } from "defuss-scheduler/worker";
export function typedConsumer(q: Queue) {
  const worker = startWorker(q, {
    workerId: "typed",
    handlers: {
      sum: defineHandler(
        (v) => {
          if (typeof v !== "number") throw Error("number expected");
          return v;
        },
        (v, ctx) => ({ sum: v + 1, fence: ctx.fence }),
      ),
    },
  });
  void q.create({ id: "typed", handler: "sum", cron: "* * * * *", payload: 1 });
  void q.runHistory("run").then((versions): number | undefined => {
    const [latest] = versions;
    return latest?.value?.updatedAt;
  });
  void q.list({ ref: "typed" });
  void q
    .terminate({ ref: "typed" }, { cause: "types" })
    .then((stopped): number => stopped.schedules + stopped.runs);
  // @ts-expect-error A hard stop names what it applies to.
  void q.terminate({}, { cause: "types" });
  void q.claim({
    // @ts-expect-error The transport requires a string worker identity.
    workerId: 5,
    sessionId: "typed",
    slot: 0,
    sequence: 1,
    requestId: "x",
    handlers: ["sum"],
  });
  return worker;
}
