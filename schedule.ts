import { Cron, parseCronExpression } from "./deps.ts";

/**
 * JSON_SCHEDULE_FIELD_NAMES defines the JSON-serializable field names of a
 * cron schedule.
 */
export const JSON_SCHEDULE_FIELD_NAMES = [
  "second", // Include if we ever support seconds.
  "minute",
  "hour",
  "dayOfMonth",
  "month",
  "dayOfWeek",
] as const;

/**
 * JSONScheduleFieldName is the name of a JSON-serializable field of a cron schedule.
 */
export type JSONScheduleFieldName = (typeof JSON_SCHEDULE_FIELD_NAMES)[number];

/**
 * JSONScheduleFieldRange is a possible value of a JSON-serializable field of a
 * cron schedule.
 */
export interface JSONScheduleFieldRange {
  /**
   * start is the starting value of the range.
   */
  start: number | "*";

  /**
   * end is the ending value of the range.
   */
  end?: number;

  /**
   * step is the step value of the range.
   */
  step?: number;
}

/**
 * JSONScheduleField is a possible value of a JSON-serializable field of a cron
 * schedule.
 */
export type JSONScheduleField =
  | number
  | JSONScheduleFieldRange;

/**
 * JSONSchedule is a JSON-serializable representation of a cron schedule.
 */
export type JSONSchedule = {
  [k in JSONScheduleFieldName]?: JSONScheduleField | JSONScheduleField[];
};

/**
 * fromScheduleField converts a JSON-serializable field of a cron schedule to a
 * string cron field.
 */
export function fromScheduleField(field: JSONScheduleField): string {
  if (typeof field === "number") {
    return field.toString();
  }

  return `${field.start}${field.end !== undefined ? `-${field.end}` : ""}${
    field.step !== undefined ? `/${field.step}` : ""
  }`;
}

/**
 * toString converts a JSON-serializable cron schedule to a string cron schedule.
 */
export function toString(schedule: JSONSchedule): string {
  return JSON_SCHEDULE_FIELD_NAMES
    .map((fieldName) => {
      const field = schedule[fieldName];
      if (field === undefined) {
        return "*";
      }

      if (Array.isArray(field)) {
        return field.map((f) => fromScheduleField(f)).join(",");
      }

      return fromScheduleField(field);
    })
    .join(" ");
}

/**
 * fromSchedule converts a cron expression or JSON-serializable cron schedule to a
 * Cron object.
 */
export function fromSchedule(schedule: string | JSONSchedule): Cron {
  if (typeof schedule === "string") {
    return parseCronExpression(schedule);
  }

  return fromSchedule(toString(schedule));
}
