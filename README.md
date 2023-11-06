# kv_cron

Use Deno Kv API to register cronjobs.

## Usage

```typescript
import { createKvCron } from "https://deno.land/x/kv_cron/mod.ts";

const kv = await Deno.openKv();
const kvCron = createKvCron(kv);

kvCron.register("*/5 * * * *", async () => {
  console.log("Hello, world!");
});

kv.listenQueue(async (message) => {
  await kvCron.handleMessage(message);
});
```

---

Created with 🦕 by [**@EthanThatOneKid**](https://etok.codes/)
