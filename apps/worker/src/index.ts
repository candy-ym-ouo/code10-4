import { randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Worker } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import {
  abortMultipartUpload,
  deleteObject,
  getObjectStream,
  listInProgressUploads,
  putObject,
} from "./lib/s3.js";
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

async function recordEvent(input: {
  action: string;
  result: "SUCCESS" | "FAILURE";
  audioObjectId?: string | null;
  mediaAssetId?: string | null;
  sessionId?: string | null;
  userId?: string | null;
  objectKey?: string | null;
  sha256?: string | null;
  detail?: Record<string, unknown>;
}): Promise<void> {
  await prisma.mediaObjectEvent.create({
    data: {
      id: randomUUID(),
      action: input.action,
      result: input.result,
      audioObjectId: input.audioObjectId ?? null,
      mediaAssetId: input.mediaAssetId ?? null,
      sessionId: input.sessionId ?? null,
      userId: input.userId ?? null,
      objectKey: input.objectKey ?? null,
      sha256: input.sha256 ?? null,
      detail: (input.detail ?? undefined) as never,
    },
  });
}

async function processAudioObject(audioObjectId: string) {
  const object = await prisma.audioObject.findUnique({ where: { id: audioObjectId } });
  if (!object) return;
  await prisma.audioObject.update({
    where: { id: audioObjectId },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  // 内容寻址对象 Key 不含扩展名；ffprobe/ffmpeg 按文件内容探测，不依赖后缀
  const localPath = path.join(workDir, "audio.bin");
  try {
    const stream = await getObjectStream(object.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);

    // 物理对象只解析一次，所有引用同步到 READY；涉及的练习进入复盘状态
    const attachedMedia = await prisma.mediaAsset.findMany({
      where: { audioObjectId },
      distinct: ["sessionId"],
      select: { sessionId: true, userId: true },
    });
    await prisma.$transaction(async (tx) => {
      const now = new Date();
      await tx.audioObject.update({
        where: { id: audioObjectId },
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
      });
      await tx.mediaAsset.updateMany({
        where: { audioObjectId },
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
      });
      for (const media of attachedMedia) {
        await tx.practiceSession.updateMany({
          where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
          data: { status: "IN_REVIEW", version: { increment: 1 } },
        });
      }
      await tx.mediaObjectEvent.create({
        data: {
          id: randomUUID(),
          action: "MEDIA_OBJECT_PROBED",
          result: "SUCCESS",
          audioObjectId,
          userId: object.userId,
          objectKey: object.objectKey,
          sha256: object.sha256,
          detail: { referenceCount: attachedMedia.length, durationMs: Number(probe.durationMs) } as never,
        },
      });
    });
    log("info", { audioObjectId, durationMs: Number(probe.durationMs) }, "audio object probe completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const code = message === "NO_AUDIO_STREAM" ? "NO_AUDIO_STREAM" : message === "INVALID_DURATION" ? "INVALID_DURATION" : "MEDIA_PROBE_FAILED";
    const now = new Date();
    await prisma.audioObject.update({
      where: { id: audioObjectId },
      data: {
        status: "FAILED",
        failureCode: code,
        failureMessage: message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试",
        processedAt: now,
      },
    });
    await prisma.mediaAsset.updateMany({
      where: { audioObjectId },
      data: {
        status: "FAILED",
        failureCode: code,
        failureMessage: message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试",
        processedAt: now,
      },
    });
    await recordEvent({
      action: "MEDIA_OBJECT_PROBE_FAILED",
      result: "FAILURE",
      audioObjectId,
      userId: object.userId,
      objectKey: object.objectKey,
      sha256: object.sha256,
      detail: { code, error: message },
    }).catch(() => undefined);
    log("error", { audioObjectId, err: message }, "audio object probe failed");
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

/** 兼容回填前按 mediaId 投递的历史任务。 */
async function processLegacyMedia(mediaId: string) {
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;
  if (media.audioObjectId) return processAudioObject(media.audioObjectId);
  await prisma.mediaAsset.update({
    where: { id: mediaId },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });
  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-legacy-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    const stream = await getObjectStream(media.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);
    await prisma.$transaction(async (tx) => {
      await tx.mediaAsset.update({
        where: { id: mediaId },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: new Date(),
          failureCode: null,
          failureMessage: null,
        },
      });
      await tx.practiceSession.updateMany({
        where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      });
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    await prisma.mediaAsset.update({
      where: { id: mediaId },
      data: { status: "FAILED", failureCode: "MEDIA_PROBE_FAILED", failureMessage: message.slice(0, 500) },
    });
    log("error", { mediaId, err: message }, "legacy media probe failed");
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

async function cleanupSession(sessionId: string) {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    include: { mediaAssets: { select: { id: true, objectKey: true, audioObjectId: true, sha256: true } } },
  });
  if (!session) return;
  try {
    const keys = new Set(session.mediaAssets.map((media) => media.objectKey));
    for (const objectKey of keys) {
      // 引用计数：其他练习仍引用同一物理对象时绝不删除
      const otherReferences = await prisma.mediaAsset.count({
        where: { objectKey, sessionId: { not: sessionId } },
      });
      const sameSessionReferences = session.mediaAssets.filter((media) => media.objectKey === objectKey);
      if (otherReferences === 0) {
        const representative = sameSessionReferences[0];
        try {
          await deleteObject(objectKey);
          await recordEvent({
            action: "MEDIA_OBJECT_DELETED",
            result: "SUCCESS",
            audioObjectId: representative?.audioObjectId ?? null,
            sessionId,
            objectKey,
            sha256: representative?.sha256 ?? null,
            detail: { source: "SESSION_CLEANUP" },
          });
        } catch (error) {
          await recordEvent({
            action: "MEDIA_OBJECT_DELETE_FAILED",
            result: "FAILURE",
            audioObjectId: representative?.audioObjectId ?? null,
            sessionId,
            objectKey,
            sha256: representative?.sha256 ?? null,
            detail: { source: "SESSION_CLEANUP", error: error instanceof Error ? error.message : "UNKNOWN" },
          }).catch(() => undefined);
          throw error;
        }
      } else {
        const representative = sameSessionReferences[0];
        await recordEvent({
          action: "MEDIA_REFERENCE_REMOVED",
          result: "SUCCESS",
          audioObjectId: representative?.audioObjectId ?? null,
          sessionId,
          objectKey,
          sha256: representative?.sha256 ?? null,
          detail: { source: "SESSION_CLEANUP", remainingReferences: otherReferences },
        });
      }
    }
    // 物理对象已在上一步按引用计数删除；会话级联删除 media_assets 后，
    // 清理失去任何引用的 audio_objects / upload_attempts 行
    const orphanObjectIds = [
      ...new Set(session.mediaAssets.map((media) => media.audioObjectId).filter((id): id is string => Boolean(id))),
    ];
    await prisma.practiceSession.delete({ where: { id: sessionId } });
    if (orphanObjectIds.length > 0) {
      const stillReferenced = await prisma.mediaAsset.findMany({
        where: { audioObjectId: { in: orphanObjectIds } },
        select: { audioObjectId: true },
        distinct: ["audioObjectId"],
      });
      const referencedIds = new Set(stillReferenced.map((item) => item.audioObjectId));
      const removable = orphanObjectIds.filter((id) => !referencedIds.has(id));
      if (removable.length > 0) {
        await prisma.uploadAttempt.deleteMany({ where: { audioObjectId: { in: removable } } });
        await prisma.audioObject.deleteMany({ where: { id: { in: removable } } });
      }
    }
    log("info", { sessionId }, "session cleanup completed");
  } catch (error) {
    await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    throw error;
  }
}

async function exportData(exportId: string) {
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

async function scanOverdueGoals() {
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

/** 回收数据库已过期但 S3 端仍挂起的分片上传，避免未完成对象长期占用存储。 */
async function sweepStaleUploads() {
  const cutoff = new Date(Date.now() - config.MULTIPART_TTL_HOURS * 3_600_000);
  const staleAttempts = await prisma.uploadAttempt.findMany({
    where: { status: "ACTIVE", expiresAt: { lt: new Date() } },
    take: 200,
  });
  let aborted = 0;
  for (const attempt of staleAttempts) {
    const object = await prisma.audioObject.findUnique({ where: { id: attempt.audioObjectId } });
    if (object) await abortMultipartUpload(object.objectKey, attempt.uploadId).catch(() => undefined);
    await prisma.uploadAttempt.update({
      where: { id: attempt.id },
      data: { status: "ABORTED", failureCode: "SWEEP_EXPIRED" },
    });
    aborted += 1;
  }

  // 兜底：S3 中存在、数据库已无记录的孤儿分片（超过 TTL）
  if (aborted === 0) {
    try {
      const inflight = await listInProgressUploads();
      for (const upload of inflight) {
        if (upload.initiated && upload.initiated < cutoff) {
          const known = await prisma.uploadAttempt.findUnique({ where: { uploadId: upload.uploadId } });
          if (!known) {
            await abortMultipartUpload(upload.key, upload.uploadId).catch(() => undefined);
            aborted += 1;
          }
        }
      }
    } catch (error) {
      log("warn", { err: error instanceof Error ? error.message : String(error) }, "orphan multipart sweep failed");
    }
  }
  if (aborted > 0) log("info", { aborted }, "stale multipart uploads swept");
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") {
      if (typeof job.data.audioObjectId === "string") return processAudioObject(String(job.data.audioObjectId));
      if (typeof job.data.mediaId === "string") return processLegacyMedia(String(job.data.mediaId));
      throw new Error("probe-media job missing audioObjectId/mediaId");
    }
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId));
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    if (job.name === "sweep-multipart") return sweepStaleUploads();
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

const sweepInterval = setInterval(() => {
  void sweepStaleUploads().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "multipart sweep failed"));
}, config.MULTIPART_SWEEP_CRON_MS);
void sweepStaleUploads().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "initial multipart sweep failed"));

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  clearInterval(sweepInterval);
  await worker.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
