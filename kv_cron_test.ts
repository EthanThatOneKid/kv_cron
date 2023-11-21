import { assertEquals, fail } from "./deps.ts";
import { makeKvCron } from "./kv_cron.ts";

Deno.test("kv_cron", async () => {
  const kv = await Deno.openKv(":memory:");
  let count = 0;
  // Create a cron job manager.
  const { enqueue, process } = makeKvCron({
    kv,
    kvKeyPrefix: ["kv_cron_test"],
    jobs: {
      main() {
        count++;
      },
    },
  });

  // Enqueue a cron job.
  const enqueueResult = await enqueue("main", {
    schedule: { second: { start: 0, step: 1 } },
    amount: new Deno.KvU64(3n),
  });
  if (!enqueueResult.ok) {
    fail("Failed to enqueue cron job.");
  }

  // Wait for the cron job to be processed.
  const { promise, resolve } = Promise.withResolvers<void>();
  const listener = kv.listenQueue(async (message: unknown) => {
    await process(message);
    if (count > 2) {
      resolve();
    }
  });

  // Wait for the cron job to be processed.
  await promise;
  assertEquals(count, 3);
  kv.close();
  await listener;
});

Deno.test("kv_cron aborts", async () => {
  const kv = await Deno.openKv(":memory:");
  let count = 0;
  // Create a cron job manager.
  const { enqueue, process } = makeKvCron({
    kv,
    kvKeyPrefix: ["kv_cron_test"],
    jobs: {
      main() {
        count++;
      },
    },
  });

  // Set up abort controller.
  let abortCount = 0;
  const controller = new AbortController();
  controller.signal.addEventListener("abort", () => {
    abortCount++;
  });

  // Enqueue a cron job.
  const enqueueResult = await enqueue("main", {
    schedule: { second: { start: 0, step: 1 } },
    signal: controller.signal,
  });
  if (!enqueueResult.ok) {
    fail("Failed to enqueue cron job.");
  }

  // Listen for the cron job.
  const { promise, resolve } = Promise.withResolvers<void>();
  const listener = kv.listenQueue(async (message: unknown) => {
    await process(message);
    controller.abort();

    // Wait for the abort signal to be processed.
    setTimeout(resolve, 100);
  });

  // Wait for the cron job to be processed.
  await promise;
  assertEquals(count, 1);
  assertEquals(abortCount, 1);
  kv.close();
  await listener;
});
