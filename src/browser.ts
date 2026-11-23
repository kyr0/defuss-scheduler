/** VERIFIED: An exclusive Web Lock makes one page or worker of an origin the authority and is released when that context dies. The packed browser consumer closes a page that holds a lease and takes its store over from a second page. */
import { createBrowserBackend } from "defuss-dbsonl/browser";
import { createDirectoryStorage } from "defuss-storage/directory";
import { start, type Scheduler, type SchedulerOptions } from "./scheduler.js";
import { SchedulerError } from "./types.js";
import { invalid } from "./validation.js";

export * from "./public.js";

/**
 * Resolves to the release of a held exclusive lock, or to null while another context holds it.
 * The request settles only after the lock is gone, so an awaited release lets the next open succeed at once.
 */
function own(name: string): Promise<(() => Promise<void>) | null> {
  return new Promise((resolve, reject) => {
    const released: Promise<void> = navigator.locks
      .request(name, { mode: "exclusive", ifAvailable: true }, (lock) => {
        if (!lock) return resolve(null);
        return new Promise<void>((free) =>
          resolve(() => {
            free();
            return released;
          }),
        );
      })
      .then(() => {});
    released.catch(reject);
  });
}
/**
 * Open the authority of one store in the origin's private file system, from a window or a dedicated worker.
 * Directory handles are the one file API of both contexts; synchronous access handles exist only in workers.
 */
export function openScheduler(options: SchedulerOptions): Promise<Scheduler> {
  return start(options, async (directory) => {
    // Lowercase names without separators stay distinct on a case-insensitive backing store.
    if (
      typeof directory !== "string" ||
      !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(directory)
    )
      invalid("Browser store names use 1 to 64 of a-z, 0-9, _ and -");
    if (!globalThis.navigator?.locks || !navigator.storage?.getDirectory)
      throw new Error(
        "defuss-scheduler needs Web Locks and the origin private file system, which require a secure context",
      );
    const name = "defuss-scheduler:" + directory;
    const release = await own(name);
    if (!release)
      throw new SchedulerError(
        "OWNER_BUSY",
        "Scheduler directory has an active authority",
      );
    try {
      const origin = await navigator.storage.getDirectory();
      const parent = await origin.getDirectoryHandle("defuss-scheduler", {
        create: true,
      });
      const storage = await createDirectoryStorage(
        await parent.getDirectoryHandle(directory, { create: true }),
        { name },
      );
      return {
        connect: () => createBrowserBackend(storage, name, navigator.locks),
        release,
      };
    } catch (error) {
      await release();
      throw error;
    }
  });
}
