import { assertEquals } from "./deps.ts";
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

  assertEquals(
    toString({
      second: { start: 0, end: 10, step: 2 },
    }),
    "0-10/2 * * * * *",
  );
});
