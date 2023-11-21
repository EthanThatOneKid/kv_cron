# kv_cron

Use the Deno Kv API to manage cron jobs.

## Usage

```ts
import { makeKvCron } from "https://deno.land/x/kv_cron/mod.ts";

if (import.meta.main) {
  // Open the KV store.
  const kv = await Deno.openKv();

  // Establish a cron job manager.
  const { enqueue, process } = makeKvCron({
    kv,
    jobs: {
      helloWorld() {
        console.log("Hello, world!");
      },
    },
  });

  // Enqueue the helloWorld job to run every second.
  await enqueue("helloWorld", {
    schedule: { second: { step: 1 } },
  });

  // Process the queue.
  await kv.listenQueue(async (message) => {
    await process(message);
  });
}
```

---

Created with 🦕 by [**@EthanThatOneKid**](https://etok.codes/)
