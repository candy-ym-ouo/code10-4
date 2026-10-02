import { Queue } from "bullmq";
import { getConfig } from "../config/env.js";
import { getRedis } from "./redis.js";

let queue: Queue | undefined;

export function getMediaQueue(): Queue {
  if (!queue) {
    queue = new Queue("media-processing", { connection: getRedis().duplicate() });
  }
  return queue;
}

/**
 * 投递音频探测任务。同一物理对象使用稳定 jobId，
 * BullMQ 自动去重，避免同摘要并发确认触发重复探测。
 * 已完成/失败但尚未过期的同 ID 任务会先移除再投递，保证 retry-probe 可用。
 */
export async function enqueueProbe(audioObjectId: string, extra: { legacyMediaId?: string } = {}): Promise<void> {
  const queue = getMediaQueue();
  if (extra.legacyMediaId) {
    // 兼容回填前的历史任务，不做去重
    await queue.add(
      "probe-media",
      { mediaId: extra.legacyMediaId },
      {
        jobId: `probe-legacy:${extra.legacyMediaId}:${Date.now()}`,
        attempts: 3,
        backoff: { type: "exponential", delay: 3000 },
        removeOnComplete: 100,
        removeOnFail: 500,
      },
    );
    return;
  }

  const jobId = `probe:${audioObjectId}`;
  const existing = await queue.getJob(jobId);
  if (existing) {
    const state = await existing.getState();
    if (state === "active" || state === "waiting" || state === "waiting-children" || state === "delayed" || state === "prioritized") {
      return; // 已有探测在排队或运行，依赖幂等，不重复投递
    }
    await existing.remove();
  }
  await queue.add(
    "probe-media",
    { audioObjectId },
    {
      jobId,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueCleanup(sessionId: string): Promise<void> {
  await getMediaQueue().add(
    "cleanup-session",
    { sessionId },
    {
      jobId: `cleanup:${sessionId}:${Date.now()}`,
      attempts: 5,
      backoff: { type: "exponential", delay: 5000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueExport(exportId: string): Promise<void> {
  await getMediaQueue().add(
    "export-data",
    { exportId },
    {
      jobId: `export:${exportId}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close();
    queue = undefined;
  }
}
