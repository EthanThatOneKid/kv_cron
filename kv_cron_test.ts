import {
  assertEquals,
  fail,
} from "https://deno.land/std@0.207.0/assert/mod.ts";
import { makeKvCron } from "./kv_cron.ts";

Deno.test("kv_cron", async () => {
  const kv = await Deno.openKv(":memory:");
  const kvCron = makeKvCron({
    kv,
    kvKeyPrefix: ["kv_cron_test"],
    jobs: {
      main() {
        throw new Error("Expected error.");
      },
    },
  });

  await kv.listenQueue(async (message: unknown) => {
    try {
      await kvCron.process(message);
    } catch (error) {
      assertEquals(error.message, "Expected error.");
    }

    // Your custom logic here...
  });

  const enqueueResult = await kvCron.enqueue("main", {
    schedule: { minute: 1 },
    amount: new Deno.KvU64(1n),
  });
  if (!enqueueResult.ok) {
    fail("Failed to enqueue cron job.");
  }

  await new Promise((resolve) => setTimeout(resolve, 60_000));
});
