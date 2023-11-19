import { fromSchedule, type JSONSchedule } from "./schedule.ts";

const KV_CRON_KEY_PREFIX: Deno.KvKey = ["kv_cron"];
const KV_CRON_NONCES_KEY_PREFIX: Deno.KvKey = [...KV_CRON_KEY_PREFIX, "nonces"];
const KV_CRON_ENQUEUED_COUNT_KEY: Deno.KvKey = [
  ...KV_CRON_KEY_PREFIX,
  "enqueued_count",
];
const KV_CRON_PROCESSED_COUNT_KEY: Deno.KvKey = [
  ...KV_CRON_KEY_PREFIX,
  "processed_count",
];

/**
 * enqueueAndSetNonce enqueues a data message to be processed by a cron job.
 */
async function enqueueAndSetNonce<T>(
  kv: Deno.Kv,
  data: T,
  nonce: string,
) {
return await kv
    .atomic()
    .check({ key: [...KV_CRON_NONCES_KEY_PREFIX, nonce], versionstamp: null })
    .enqueue({ nonce, data })
    .set([...KV_CRON_NONCES_KEY_PREFIX, nonce], true)
    .sum(KV_CRON_ENQUEUED_COUNT_KEY, 1n)
    .commit();
}

/**
 * getNonce retrieves a data message from the queue to be processed by a cron
 * job.
 */
async function getNonce(
  kv: Deno.Kv,
  nonce: string,
) {
  const nonceResult = await kv.get([...KV_CRON_NONCES_KEY_PREFIX, nonce]);
  if (nonceResult.value === null) {
    // This messaged was already processed.
    return;
  }

  // Ensure this message was not yet processed.
  return await kv.atomic()
    .check(nonceResult)
    .delete(nonceResult.key)
    .sum(KV_CRON_PROCESSED_COUNT_KEY, 1n)
.commit();
}

/**
 * KvCronOptions defines the options for creating a cron job manager.
 */
export interface KvCronOptions {
  /**
   * kv is the Deno.Kv store to use for the cron job manager.
   */
  kv: Deno.Kv;

  /**
   * schedule is the cron expression that defines when the cron job is triggered.
   */
  schedule: string | JSONSchedule;

  /**
   * generateNonce is a function that generates a nonce for a given data message.
   */
  generateNonce: () => string;

  /**
   * handlersMap is a list of handlers for a given cron job.
   */
  handlersMap: Map<string, Function>;
}

/**
 * kvCron creates a cron job manager for a given Deno.Kv store.
 */
export async function kvCron<T>(
  options: KvCronOptions,
  handle: (data: T) => void,
) {
  // Find next time the cron job should be triggered.
  const cron = fromSchedule(options.schedule);
  
  // Enqueue a message to be processed at that time.
  await enqueueAndSetNonce(
 
  // Set a nonce for the message.
  // When the message is processed, delete the nonce.
  // If the nonce is already deleted, then the message was already processed.
  // Handle the message.
}

/**
 * createKvCron creates a cron job manager for a given Deno.Kv store.
 */
export function createKvCron(kv: Deno.Kv) {
  const handlers: Map<string, (data: string) => void> = new Map();

  return {
    async register(cronExpression: string, fn: (data: string) => void) {
      // const cron = parseCronExpression(cronExpression);
      // const channel = `${CHANNEL_PREFIX}${cronExpression}`;
      handlers.set(cronExpression, fn);

      // If there already a cron job registered for this expression, delete it
      // and recreate it.
      // TODO: Design a data flow that allows us to update the cron job.
    },

    async handleMessage(message: { channel: string; nonce: string }) {
      if (
        typeof message.channel !== "string" ||
        !message.channel.startsWith(CHANNEL_PREFIX)
      ) {
        return;
      }

      // Parse the cron expression from the channel name.
      const cronExpression = message.channel.slice(CHANNEL_PREFIX.length);
      const handler = handlers.get(cronExpression);
      if (handler === undefined) {
        throw new Error(`No handler registered for channel ${message.channel}`);
      }

      // Ensure this message was not yet processed.
      const nonceKey = [`${message.channel}:nonces`, message.nonce];
      const nonceResult = await kv.get(nonceKey);
      if (nonceResult === null) {
        // This message was already handled.
        return;
      }

      // Invoke the handler one time.
      await handler(cronExpression);

      // Delete the nonce key to prevent this message from being handled again.
      await kv.atomic()
        .check(nonceResult)
        .delete(nonceResult.key)
        .commit();
    },
  };
}

const CHANNEL_PREFIX = "cron-channel:";
