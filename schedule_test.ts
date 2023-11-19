import { assertEquals } from "https://deno.land/std@0.207.0/assert/mod.ts";
import { toString } from "./schedule.ts";

Deno.test("toString", () => {
  assertEquals(
    toString({
      minute: 0,
      hour: 0,
      dayOfMonth: 1,
      month: 1,
      dayOfWeek: 1,
    }),
    "0 0 1 1 1",
  );

  assertEquals(
    toString({
      minute: { start: "*", step: 5 },
      hour: [1, 2, 3],
    }),
    "*/5 1,2,3 * * *",
  );
});

Deno.test("toString", () => {
});
