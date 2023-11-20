import {
  assertEquals,
  fail,
} from "https://deno.land/std@0.207.0/assert/mod.ts";
import { makeKvCron } from "./kv_cron.ts";

Deno.test("kv_cron", async () => {
  const kv = await Deno.openKv(":memory:");
  const { enqueue, process } = makeKvCron({
    kv,
    kvKeyPrefix: ["kv_cron_test"],
    jobs: {
      main() {
        throw new Error("Expected error.");
      },
    },
  });

  console.log("Enqueuing cron job...");
  const enqueueResult = await enqueue("main", {
    schedule: { minute: 1 },
    amount: new Deno.KvU64(1n),
  });
  if (!enqueueResult.ok) {
    fail("Failed to enqueue cron job.");
  }

  console.log("Listening for cron job...");
  await kv.listenQueue(async (message: unknown) => {
    try {
      console.log("Processing cron job...", { message });
      await process(message);
    } catch (error) {
      assertEquals(error.message, "Expected error.");
    }

    // Your custom logic here...
  });
});
