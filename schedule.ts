import { Cron, parseCronExpression } from "./deps.ts";

/**
 * JSON_SCHEDULE_FIELD_NAMES defines the JSON-serializable field names of a
 * cron schedule.
 */
export const JSON_SCHEDULE_FIELD_NAMES = [
  "second",
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
export type JSONScheduleFieldRange =
  | {
    start: number | "*";
    end: number;
    step?: number;
  }
  | {
    step: number;
  };

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

  if ("start" in field && "end" in field) {
    return `${field.start}-${field.end}${
      field.step !== undefined ? `/${field.step}` : ""
    }`;
  }

  return `*/${field.step}`;
}

/**
 * toString converts a JSON-serializable cron schedule to a string cron schedule.
 */
export function toString(schedule: JSONSchedule): string {
  const names = schedule.second !== undefined
    ? JSON_SCHEDULE_FIELD_NAMES
    : JSON_SCHEDULE_FIELD_NAMES.slice(1);

  return names
    .map((fieldName) => {
      // field will never undefined in the 'second' case, but it will be in the
      // other cases. Thus, we only ever need to apply a default '*' value in
      // the other cases.
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
