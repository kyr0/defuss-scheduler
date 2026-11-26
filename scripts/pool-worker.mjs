// VERIFIED: make e2e runs it under Node.js and Bun. The packed consumer's defuss-multicore task module, plain JavaScript so Node.js and Bun load it in a worker thread
// without a TypeScript step.
import { expose } from "defuss-multicore/worker";

await expose({
  square: (payload) => ({ square: payload.n * payload.n }),
});
