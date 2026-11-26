/** VERIFIED: tests/jobs.test.ts runs it in a real pool. A defuss-multicore task module: one task that succeeds, one that throws, one that never ends. */
import { expose } from "defuss-multicore/worker";

export const tasks = {
  square(payload: { n: number }) {
    return { square: payload.n * payload.n };
  },
  fail(): never {
    throw new Error("pool task failed");
  },
  hang() {
    return new Promise<never>(() => {});
  },
};

await expose(tasks);
