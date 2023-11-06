// import { parseCronExpression } from "./deps.ts";

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
