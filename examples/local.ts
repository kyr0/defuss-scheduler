/** VERIFIED: The runnable example creates a schedule once and drains the worker on a termination signal. */
import { openScheduler, startWorker } from "defuss-scheduler";
const scheduler = await openScheduler({ directory: "./var/example" });
const queue = scheduler.scope({
  app: "demo",
  tenant: "local",
  bucket: "maintenance",
});
if (!(await queue.get("minute"))) {
  await queue.create({
    id: "minute",
    cron: "* * * * *",
    handler: "print",
    payload: { label: "minute" },
  });
}
const worker = startWorker(queue, {
  workerId: "console",
  handlers: {
    print: async (job, ctx) => {
      console.log(
        new Date(job.scheduledAt).toISOString(),
        ctx.runId,
        job.payload,
      );
    },
  },
});
async function stop() {
  await worker.stop();
  await scheduler.close();
}
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
