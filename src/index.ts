/** VERIFIED: A native whole-file lock survives event-loop pauses and releases on process death. The packed consumer kills an authority and reopens its directory under the other runtime. */
import { mkdir, realpath } from "node:fs/promises";
import { join } from "node:path";
import { tryOpenLock, Lock } from "@lickle/lock";
import { createFileBackend } from "defuss-dbsonl/server";
import { start, type Scheduler, type SchedulerOptions } from "./scheduler.js";
import { SchedulerError } from "./types.js";

export * from "./public.js";
/** Open the authority of one directory on local disk. */
export function openScheduler(options: SchedulerOptions): Promise<Scheduler> {
  return start(options, async (directory) => {
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const root = await realpath(directory);
    const guard = await tryOpenLock(
      join(root, "authority.lock"),
      Lock.Exclusive,
    );
    if (!guard)
      throw new SchedulerError(
        "OWNER_BUSY",
        "Scheduler directory has an active authority",
      );
    return {
      connect: () => createFileBackend(root),
      release: () => guard.drop(),
    };
  });
}
