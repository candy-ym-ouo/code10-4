import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Worker, type Job } from "bullmq";
import { Prisma } from "@prisma/client";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { abortMultipartUpload, deleteObject, getObjectStream, objectExists, putObject } from "./lib/s3.js";
import { generatePeaks, probeAudio } from "./lib/media.js";
import { buildUserExport } from "./lib/export.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

async function processMediaObject(objectId: string, job: Job): Promise<void> {
  const object = await prisma.mediaObject.findUnique({ where: { id: objectId } });
  if (!object) return;
  // 已就绪对象（例如重复任务）无需再次探测。
  if (object.status === "READY") {
    // 兜住历史状态漂移：对象已 READY 但引用尚未同步时补齐。
    await prisma.mediaAsset.updateMany({
      where: { objectId, status: { not: "READY" } },
      data: {
        status: "READY",
        durationMs: object.durationMs,
        codec: object.codec,
        sampleRate: object.sampleRate,
        channels: object.channels,
        peaks: object.peaks === null ? Prisma.JsonNull : (object.peaks as Prisma.InputJsonValue),
        processedAt: object.processedAt,
        failureCode: null,
        failureMessage: null,
      },
    });
    return;
  }
  await prisma.mediaObject.update({
    where: { id: objectId },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(object.objectKey).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension || ".bin"}`);
  try {
    const stream = await getObjectStream(object.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);
    const now = new Date();

    // 物理对象只探测一次，结果同步到全部引用，所有引用共享同一份元数据。
    await prisma.$transaction([
      prisma.mediaObject.update({
        where: { id: objectId },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: now,
          failureCode: null,
          failureMessage: null,
        },
      }),
      prisma.mediaAsset.updateMany({
        where: { objectId },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: now,
          failureCode: null,
          failureMessage: null,
        },
      }),
      // 对象可用后，引用它的草稿练习进入复盘态（仅一次，幂等）。
      prisma.practiceSession.updateMany({
        where: {
          status: "DRAFT",
          mediaAssets: { some: { objectId } },
        },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      }),
    ]);
    log("info", { objectId, durationMs: Number(probe.durationMs) }, "media object probe completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const code =
      message === "NO_AUDIO_STREAM"
        ? "NO_AUDIO_STREAM"
        : message === "INVALID_DURATION"
          ? "INVALID_DURATION"
          : "MEDIA_PROBE_FAILED";
    const friendly =
      message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试";

    // 最后一次尝试仍失败才落地 FAILED；BullMQ 重试期间保持 PROCESSING。
    if (job.attemptsMade + 1 >= (job.opts.attempts ?? 1)) {
      await prisma.$transaction([
        prisma.mediaObject.update({
          where: { id: objectId },
          data: { status: "FAILED", failureCode: code, failureMessage: friendly, processedAt: new Date() },
        }),
        prisma.mediaAsset.updateMany({
          where: { objectId },
          data: { status: "FAILED", failureCode: code, failureMessage: friendly, processedAt: new Date() },
        }),
      ]);
    }
    log("error", { objectId, err: message, attempt: job.attemptsMade + 1 }, "media object probe failed");
    throw error;
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

async function cleanupSession(sessionId: string): Promise<void> {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    include: { mediaAssets: { select: { id: true, objectId: true, objectKey: true } } },
  });
  if (!session) return;

  try {
    // 以物理对象为单位清理：仍被其它练习引用的对象保留，最后引用消失才删除内容。
    const objects = new Map<string, string>();
    for (const media of session.mediaAssets) {
      if (media.objectId) objects.set(media.objectId, media.objectKey);
    }

    // 会话及其引用行先在数据库删除；随后才删除失去最后引用的物理对象。
    // S3 删除失败会抛出并把会话标记回 DELETE_FAILED，media_objects 中
    // 零引用的登记由 reconcileOrphanObjects 周期补删，整个过程可追溯。
    await prisma.practiceSession.delete({ where: { id: sessionId } });

    const garbage: Array<{ objectId: string; objectKey: string; userId: string }> = [];
    for (const [objectId, objectKey] of objects) {
      const remaining = await prisma.mediaAsset.count({
        where: { OR: [{ objectId }, { objectKey }] },
      });
      if (remaining > 0) continue;
      await deleteObject(objectKey);
      const object = await prisma.mediaObject.findUnique({
        where: { id: objectId },
        select: { id: true, userId: true },
      });
      await prisma.mediaObject.delete({ where: { id: objectId } });
      if (object) garbage.push({ objectId, objectKey, userId: object.userId });
    }

    if (garbage.length > 0) {
      await prisma.auditLog.createMany({
        data: garbage.map((item) => ({
          userId: item.userId,
          action: "MEDIA_OBJECT_GARBAGE_COLLECTED",
          resource: "MEDIA_OBJECT",
          resourceId: item.objectId,
          result: "SUCCESS",
          metadata: { objectKey: item.objectKey, sessionId } as never,
        })),
      });
    }
    log("info", { sessionId, removedObjects: garbage.length }, "session cleanup completed");
  } catch (error) {
    await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    throw error;
  }
}

async function exportData(exportId: string): Promise<void> {
  const task = await prisma.dataExport.findUnique({ where: { id: exportId } });
  if (!task || !task.objectKey) return;
  await prisma.dataExport.update({ where: { id: exportId }, data: { status: "PROCESSING" } });
  try {
    const output = await buildUserExport(task.userId, task.format);
    await putObject(task.objectKey, output.body, output.contentType);
    await prisma.dataExport.update({ where: { id: exportId }, data: { status: "READY", failure: null } });
  } catch (error) {
    await prisma.dataExport.update({
      where: { id: exportId },
      data: { status: "FAILED", failure: error instanceof Error ? error.message.slice(0, 500) : "EXPORT_FAILED" },
    });
    throw error;
  }
}

async function scanOverdueGoals(): Promise<void> {
  const startOfToday = new Date();
  startOfToday.setUTCHours(0, 0, 0, 0);
  const result = await prisma.goal.updateMany({
    where: {
      dueDate: { lt: startOfToday },
      status: { in: ["OPEN", "IN_PROGRESS"] },
    },
    data: { status: "MISSED" },
  });
  if (result.count > 0) log("info", { count: result.count }, "overdue goals marked missed");
}

/**
 * 清理零引用物理对象：会话删除时 S3 删除可能失败并留下登记，
 * 这里周期性补删，确保引用清理最终一致且不产生孤儿存储。
 */
async function reconcileOrphanObjects(): Promise<void> {
  const orphans = await prisma.mediaObject.findMany({
    where: { assets: { none: {} } },
    take: 50,
    select: { id: true, userId: true, objectKey: true },
  });
  for (const object of orphans) {
    try {
      if (await objectExists(object.objectKey)) await deleteObject(object.objectKey);
      await prisma.mediaObject.delete({ where: { id: object.id } });
      await prisma.auditLog.create({
        data: {
          userId: object.userId,
          action: "MEDIA_OBJECT_GARBAGE_COLLECTED",
          resource: "MEDIA_OBJECT",
          resourceId: object.id,
          result: "SUCCESS",
          metadata: { objectKey: object.objectKey, reconcile: true } as never,
        },
      });
      log("info", { objectId: object.id }, "orphan media object reclaimed");
    } catch (error) {
      log("error", { objectId: object.id, err: error instanceof Error ? error.message : String(error) }, "orphan reconcile failed");
    }
  }
}

/**
 * 回收过期上传：中止 S3 分片会话并标记引用，避免网络中断后
 * 已传分片长期占用存储。仅处理超过 expiresAt 仍未完成的记录。
 */
async function reclaimStaleUploads(): Promise<void> {
  const stale = await prisma.mediaAsset.findMany({
    where: {
      status: { in: ["PENDING_UPLOAD", "UPLOADING", "FAILED"] },
      uploadId: { not: null },
      expiresAt: { lt: new Date() },
    },
    take: 100,
    select: { id: true, objectKey: true, uploadId: true },
  });
  for (const media of stale) {
    if (!media.uploadId) continue;
    try {
      await abortMultipartUpload(media.objectKey, media.uploadId);
    } catch (error) {
      log("error", { mediaId: media.id, err: error instanceof Error ? error.message : String(error) }, "stale upload abort failed");
      continue;
    }
    await prisma.mediaAsset.update({
      where: { id: media.id },
      data: { status: "CANCELLED", failureCode: "UPLOAD_SESSION_EXPIRED", uploadId: null, uploadPartSize: null },
    });
  }
  if (stale.length > 0) log("info", { count: stale.length }, "stale uploads reclaimed");
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-object") return processMediaObject(String(job.data.objectId), job);
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId));
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    throw new Error(`Unknown job: ${job.name}`);
  },
  { connection: redis, concurrency: config.WORKER_CONCURRENCY },
);

worker.on("failed", (job, error) => log("error", { jobId: job?.id, jobName: job?.name, err: error.message }, "job failed"));
worker.on("error", (error) => log("error", { err: error.message }, "worker error"));

const heartbeat = setInterval(async () => {
  await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
}, 10_000);
await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
await scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
const overdueInterval = setInterval(() => {
  void scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
}, 24 * 60 * 60_000);

await reconcileOrphanObjects().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "orphan reconcile failed"));
const orphanInterval = setInterval(() => {
  void reconcileOrphanObjects().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "orphan reconcile failed"));
}, 60 * 60_000);

await reclaimStaleUploads().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "stale upload reclaim failed"));
const staleUploadInterval = setInterval(() => {
  void reclaimStaleUploads().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "stale upload reclaim failed"));
}, 30 * 60_000);

async function shutdown(signal: string): Promise<void> {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  clearInterval(orphanInterval);
  clearInterval(staleUploadInterval);
  await worker.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
