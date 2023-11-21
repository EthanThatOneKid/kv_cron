import { fromSchedule, type JSONSchedule } from "./schedule.ts";

export const KV_CRON_KEY_PREFIX = ["kv_cron"] as const satisfies Deno.KvKey;
export const KV_CRON_KEY_PART_JOBS = "jobs" as const satisfies Deno.KvKeyPart;
export const KV_CRON_KEY_PART_ENQUEUED_COUNT =
  "enqueued_count" as const satisfies Deno.KvKeyPart;
export const KV_CRON_KEY_PART_PROCESSED_COUNT =
  "processed_count" as const satisfies Deno.KvKeyPart;

/**
 * makeKvCronKeyMaker creates a function that creates a key for a cron job.
 */
export function makeKvCronKeyMaker(prefix: Deno.KvKey) {
  return (...keys: Deno.KvKey): Deno.KvKey => [...prefix, ...keys];
}

/**
 * JobsSchema defines a map of possible cron job names to cron job handlers.
 */
export interface JobsSchema {
  [name: string | number | symbol]: () => void;
}

/**
 * KvCronOptions defines the options for creating a cron job manager.
 */
export interface KvCronOptions<T extends JobsSchema> {
  /**
   * kv is the Deno.Kv store to use for the cron job manager.
   */
  kv: Deno.Kv;

  /**
   * kvKeyPrefix is the prefix of the keys used by the cron job manager.
   */
  kvKeyPrefix?: Deno.KvKey;

  /**
   * jobs is a map of cron job names to cron job handlers.
   */
  jobs: T;

  /**
   * generateNonce is a function that generates a random nonce.
   */
  generateNonce?: () => string;
}

/**
 * EnqueueJobOpOptions defines the options for enqueuing a cron job.
 */
export interface EnqueueJobOpOptions
  extends Pick<KvCronOptions<JobsSchema>, "kv" | "kvKeyPrefix"> {
  /**
   * nonce is the nonce of the cron job.
   */
  nonce: string;

  /**
   * name is the name of the cron job.
   */
  name: string | number | symbol;

  /**
   * schedule is the cron expression that defines when the cron job is triggered.
   */
  schedule: string | JSONSchedule;

  /**
   * date is the date which the cron job is relative to.
   */
  date?: Date;

  /**
   * amount is the number of times the cron job should be triggered.
   */
  amount?: Deno.KvU64;

  /**
   * backoffSchedule can be used to specify the retry policy for failed
   * executions. Each element in the array represents the number of
   * milliseconds to wait before retrying the execution. For example,
   * `[1000, 5000, 10000]` means that a failed execution will be retried at
   * most 3 times, with 1 second, 5 seconds, and 10 seconds delay between each
   * retry.
   */
  backoffSchedule?: number[];
}

/**
 * EnqueuedKvCronJob is a cron job that has been enqueued.
 */
export type EnqueuedJob = Omit<
  EnqueueJobOpOptions,
  "kv" | "kvKeyPrefix" | "date" | "amount" | "signal"
>;

/**
 * isEnqueuedJob returns true if the given value is an enqueued job.
 */
function isEnqueuedJob(value: unknown): value is EnqueuedJob {
  const job = value as EnqueuedJob;
  return (
    "nonce" in job && typeof job.nonce === "string" &&
    "name" in job && typeof job.name === "string"
  );
}

/**
 * enqueueJobOp enqueues a data message to be processed by a cron job.
 */
async function enqueueJobOp(
  options: EnqueueJobOpOptions,
  retryCount = 0,
): Promise<Deno.KvCommitResult> {
  const makeKvCronKey = makeKvCronKeyMaker(
    options.kvKeyPrefix ?? KV_CRON_KEY_PREFIX,
  );
  const date = options.date ?? new Date();
  const delayedDate = fromSchedule(options.schedule).getNextDate(options.date);
  const delay = delayedDate.getTime() - date.getTime();
  const result = await options.kv
    .atomic()
    .enqueue(
      {
        nonce: options.nonce,
        name: options.name,
        schedule: options.schedule,
        backoffSchedule: options.backoffSchedule,
      } satisfies EnqueuedJob,
      { delay },
    )
    .set(
      makeKvCronKey(KV_CRON_KEY_PART_JOBS, options.nonce),
      { amount: options.amount },
    )
    .sum(makeKvCronKey(KV_CRON_KEY_PART_ENQUEUED_COUNT), 1n)
    .commit();
  if (!result.ok) {
    if (
      options.backoffSchedule !== undefined &&
      options.backoffSchedule.length > retryCount
    ) {
      const delay = options.backoffSchedule[retryCount];
      await new Promise((resolve) => setTimeout(resolve, delay));
      return await enqueueJobOp(options, retryCount + 1);
    }

    throw new Error("Failed to enqueue cron job.");
  }

  return result;
}

/**
 * AbortOpOptions defines the options for aborting a cron job.
 */
export type AbortOpOptions = Pick<
  EnqueueJobOpOptions,
  "kv" | "kvKeyPrefix" | "nonce"
>;

/**
 * abortOp aborts a cron job.
 */
async function abortOp(options: AbortOpOptions) {
  const makeKvCronKey = makeKvCronKeyMaker(
    options.kvKeyPrefix ?? KV_CRON_KEY_PREFIX,
  );
  const enqueuedCountResult = await options.kv.get<Deno.KvU64>(
    makeKvCronKey(KV_CRON_KEY_PART_ENQUEUED_COUNT),
  );
  const nextEnqueuedCount = new Deno.KvU64(
    enqueuedCountResult.value ? enqueuedCountResult.value.value - 1n : 0n,
  );
  const result = await options.kv
    .atomic()
    .delete(makeKvCronKey(KV_CRON_KEY_PART_JOBS, options.nonce))
    .set(
      makeKvCronKey(KV_CRON_KEY_PART_ENQUEUED_COUNT),
      nextEnqueuedCount,
    )
    .commit();
  if (!result.ok) {
    throw new Error("Failed to abort cron job.");
  }

  return result;
}

/**
 * getJobOp gets an enqueued job.
 */
async function getJobOp(
  options: Pick<EnqueueJobOpOptions, "kv" | "kvKeyPrefix" | "nonce">,
) {
  const makeKvCronKey = makeKvCronKeyMaker(
    options.kvKeyPrefix ?? KV_CRON_KEY_PREFIX,
  );
  return await options.kv.get<{ amount?: Deno.KvU64 }>(
    makeKvCronKey(KV_CRON_KEY_PART_JOBS, options.nonce),
  );
}

/**
 * EnqueueOptions defines the options for enqueuing a cron job.
 */
export interface EnqueueOptions extends
  Pick<
    EnqueueJobOpOptions,
    "schedule" | "date" | "amount" | "backoffSchedule"
  > {
  /**
   * signal is the signal that can be used to abort the cron job.
   */
  signal?: AbortSignal;
}

/**
 * makeKvCron creates a cron job manager.
 */
export function makeKvCron<T extends JobsSchema>(options: KvCronOptions<T>) {
  return {
    async enqueue(name: keyof T, enqueueOptions: EnqueueOptions) {
      const nonce = options.generateNonce
        ? options.generateNonce()
        : crypto.randomUUID();
      if (enqueueOptions?.signal) {
        enqueueOptions.signal.addEventListener(
          "abort",
          async () => {
            await abortOp({
              kv: options.kv,
              kvKeyPrefix: options.kvKeyPrefix,
              nonce,
            });
          },
          { once: true },
        );
      }

      const result = await enqueueJobOp({
        kv: options.kv,
        kvKeyPrefix: options.kvKeyPrefix,
        nonce,
        name,
        schedule: enqueueOptions.schedule,
        date: enqueueOptions.date,
        amount: enqueueOptions.amount,
        backoffSchedule: enqueueOptions.backoffSchedule,
      });
      if (!result.ok) {
        throw new Error("Failed to enqueue cron job.");
      }

      return result;
    },

    async process(data: unknown, date = new Date()) {
      if (!isEnqueuedJob(data)) {
        return;
      }

      const jobResult = await getJobOp({
        kv: options.kv,
        kvKeyPrefix: options.kvKeyPrefix,
        nonce: data.nonce,
      });
      if (!jobResult.value) {
        return;
      }

      const handle = options.jobs[data.name];
      if (!handle) {
        throw new Error(`Unknown cron job: ${data.name.toString()}`);
      }
      await handle();

      const makeKvCronKey = makeKvCronKeyMaker(
        options.kvKeyPrefix ?? KV_CRON_KEY_PREFIX,
      );
      const postprocessOp = options.kv.atomic(); // .check(jobResult);
      if (!jobResult.value.amount || jobResult.value.amount?.value > 1n) {
        const result = await enqueueJobOp({
          kv: options.kv,
          kvKeyPrefix: options.kvKeyPrefix,
          nonce: data.nonce,
          name: data.name,
          schedule: data.schedule,
          date,
          amount: jobResult.value.amount
            ? new Deno.KvU64(jobResult.value.amount.value - 1n)
            : undefined,
          backoffSchedule: data.backoffSchedule,
        });
        if (!result.ok) {
          throw new Error("Failed to enqueue cron job.");
        }
      } else {
        postprocessOp.delete(jobResult.key);
      }

      return await options.kv.atomic()
        .sum(makeKvCronKey(KV_CRON_KEY_PART_PROCESSED_COUNT), 1n)
        .commit();
    },
    // TODO: Implement `getAll` method. `getAll` should return all enqueued
    // jobs.
    // https://discord.com/channels/684898665143206084/1174052112536195102/1174412447692628081
  };
}
