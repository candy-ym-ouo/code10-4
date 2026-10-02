import { randomUUID } from "node:crypto";
import path from "node:path";
import type { FastifyPluginAsync } from "fastify";
import { z } from "zod";
import { getConfig } from "../config/env.js";
import { AppError, notFound } from "../lib/errors.js";
import { prisma } from "../lib/prisma.js";
import { enqueueProbe } from "../lib/queue.js";
import {
  abortMultipartUpload,
  createMultipartUpload,
  createPartUploadUrl,
  deleteObject,
  createPlaybackUrl,
  listUploadedParts,
} from "../lib/s3.js";
import { parseOrThrow } from "../lib/validation.js";
import { audit } from "../lib/audit.js";
import {
  computePartPlan,
  ensureAudioObject,
  mediaSelect,
  mergeAndVerifyObject,
  recordObjectEvent,
} from "../lib/media-object.js";

const ALLOWED_MIME_TYPES = new Set([
  "audio/mpeg",
  "audio/mp4",
  "audio/m4a",
  "audio/x-m4a",
  "audio/aac",
  "audio/wav",
  "audio/x-wav",
  "audio/ogg",
  "audio/flac",
  "audio/webm",
]);
const uploadSessionSchema = z.object({
  originalName: z.string().trim().min(1).max(255),
  mimeType: z.string().trim().min(1).max(100),
  sizeBytes: z.coerce.bigint().positive(),
  sha256: z.string().regex(/^[a-fA-F0-9]{64}$/, "SHA-256 摘要格式不正确"),
});
const partNumbersSchema = z.object({ partNumbers: z.array(z.coerce.number().int().min(1).max(10_000)).min(1).max(100) });

function safeFileName(input: string): string {
  const base = path.basename(input).replace(/[^\p{L}\p{N}._-]+/gu, "_").slice(0, 180);
  return base || "audio";
}

const mediaRoutes: FastifyPluginAsync = async (app) => {
  app.addHook("preHandler", app.authenticate);

  app.post("/sessions/:sessionId/media/uploads", async (request, reply) => {
    const config = getConfig();
    const { sessionId } = request.params as { sessionId: string };
    const input = parseOrThrow(uploadSessionSchema, request.body);
    if (!ALLOWED_MIME_TYPES.has(input.mimeType.toLowerCase())) {
      throw new AppError(415, "UNSUPPORTED_MEDIA", "仅支持常见音频格式");
    }
    const maxBytes = BigInt(config.MAX_MEDIA_SIZE_MB) * 1024n * 1024n;
    if (input.sizeBytes > maxBytes) {
      throw new AppError(413, "FILE_TOO_LARGE", `单个音频不能超过 ${config.MAX_MEDIA_SIZE_MB} MB`);
    }

    const session = await prisma.practiceSession.findFirst({
      where: { id: sessionId, userId: request.authUser!.id },
      include: {
        _count: { select: { mediaAssets: true } },
        mediaAssets: { select: { sizeBytes: true } },
      },
    });
    if (!session) throw notFound();
    if (!["DRAFT", "IN_REVIEW"].includes(session.status)) {
      throw new AppError(409, "INVALID_SESSION_STATE", "当前练习状态不能继续上传音频");
    }
    if (session._count.mediaAssets >= config.MAX_MEDIA_PER_SESSION) {
      throw new AppError(400, "MEDIA_LIMIT_REACHED", `每个练习最多 ${config.MAX_MEDIA_PER_SESSION} 个音频`);
    }
    const total = session.mediaAssets.reduce((sum, item) => sum + item.sizeBytes, 0n);
    const maxTotal = BigInt(config.MAX_SESSION_TOTAL_MB) * 1024n * 1024n;
    if (total + input.sizeBytes > maxTotal) {
      throw new AppError(413, "SESSION_SIZE_LIMIT_REACHED", `单次练习音频总量不能超过 ${config.MAX_SESSION_TOTAL_MB} MB`);
    }

    const digest = input.sha256.toLowerCase();
    const partPlan = computePartPlan(Number(input.sizeBytes));
    const now = new Date();
    const attemptExpiresAt = new Date(now.getTime() + config.MULTIPART_TTL_HOURS * 3_600_000);

    const result = await prisma.$transaction(async (tx) => {
      const audioObject = await ensureAudioObject(tx, {
        userId: request.authUser!.id,
        sha256: digest,
        sizeBytes: input.sizeBytes,
        mimeType: input.mimeType,
      });

      // 物理对象已完成或正在处理：只新增引用，不重复占用存储。
      // FAILED 是已成功合并但探测/校验失败的隔离对象，不能当作可信内容复用，
      // 需要走下方重新合并上传的流程覆盖该内容寻址 Key。
      if (["UPLOADED", "PROCESSING", "READY"].includes(audioObject.status)) {
        const media = await tx.mediaAsset.create({
          data: {
            userId: request.authUser!.id,
            sessionId,
            audioObjectId: audioObject.id,
            status: audioObject.status === "READY" ? "READY" : audioObject.status === "PROCESSING" ? "PROCESSING" : "UPLOADED",
            objectKey: audioObject.objectKey,
            originalName: safeFileName(input.originalName),
            mimeType: input.mimeType,
            sizeBytes: input.sizeBytes,
            sha256: digest,
            durationMs: audioObject.durationMs,
            codec: audioObject.codec,
            sampleRate: audioObject.sampleRate,
            channels: audioObject.channels,
            peaks: audioObject.peaks ?? undefined,
            uploadedAt: audioObject.uploadedAt ?? now,
            processedAt: audioObject.processedAt ?? null,
          },
          select: mediaSelect,
        });
        await recordObjectEvent(tx, {
          action: "MEDIA_REFERENCE_CREATED",
          result: "SUCCESS",
          audioObjectId: audioObject.id,
          mediaAssetId: media.id,
          sessionId,
          userId: request.authUser!.id,
          objectKey: audioObject.objectKey,
          sha256: digest,
          detail: { sourceStatus: audioObject.status },
        });
        return { kind: "reused" as const, media, upload: null as null };
      }

      // 仅在事务内挑出候选会话；是否真的可续传要在事务外向 S3 确认
      const candidate = await tx.uploadAttempt.findFirst({
        where: { audioObjectId: audioObject.id, status: "ACTIVE" },
        orderBy: { createdAt: "desc" },
      });
      return { kind: "pending" as const, audioObject, candidate };
    });

    if (result.kind === "reused") {
      await audit(
        request,
        "MEDIA_CONTENT_REUSED",
        "MEDIA_ASSET",
        result.media.id,
        "SUCCESS",
        { audioObjectId: result.media.audioObjectId },
      );
      return reply.status(201).send(result);
    }

    const { audioObject, candidate } = result;

    // 候选会话必须在 S3 端仍然存活（ListParts 非 404）且未过期，否则作废后新建。
    // 这样网络恢复/页面重开时只会续传真实存在的分片。
    let attempt: { id: string; uploadId: string; partSizeBytes: bigint; partCount: number } | null = null;
    const invalidateCandidate = async (reason: string, abortRemote: boolean): Promise<void> => {
      if (!candidate) return;
      await prisma.uploadAttempt
        .update({ where: { id: candidate.id }, data: { status: "ABORTED", failureCode: reason.slice(0, 64) } })
        .catch(() => undefined);
      if (abortRemote) await abortMultipartUpload(audioObject.objectKey, candidate.uploadId).catch(() => undefined);
      await recordObjectEvent(prisma, {
        action: "MEDIA_UPLOAD_ABORTED",
        result: "FAILURE",
        audioObjectId: audioObject.id,
        userId: request.authUser!.id,
        objectKey: audioObject.objectKey,
        sha256: digest,
        detail: { uploadId: candidate.uploadId, reason },
      }).catch(() => undefined);
    };

    if (candidate && candidate.expiresAt < now) {
      await invalidateCandidate("UPLOAD_SESSION_EXPIRED", true);
    } else if (
      candidate &&
      (candidate.partSizeBytes !== BigInt(partPlan.partSizeBytes) || candidate.partCount !== partPlan.partCount)
    ) {
      // 分片规划变化时旧分片不能再用于合并，必须中止后重建
      await invalidateCandidate("UPLOAD_PLAN_CHANGED", true);
    } else if (candidate) {
      const remoteParts = await listUploadedParts(audioObject.objectKey, candidate.uploadId).catch(() => null);
      if (remoteParts !== null) {
        attempt = {
          id: candidate.id,
          uploadId: candidate.uploadId,
          partSizeBytes: candidate.partSizeBytes,
          partCount: candidate.partCount,
        };
      } else {
        await invalidateCandidate("UPLOAD_SESSION_MISSING", false);
      }
    }

    if (!attempt) {
      const uploadId = await createMultipartUpload(audioObject.objectKey, input.mimeType, digest);
      const attemptId = randomUUID();
      await prisma.uploadAttempt.create({
        data: {
          id: attemptId,
          audioObjectId: audioObject.id,
          userId: request.authUser!.id,
          uploadId,
          status: "ACTIVE",
          partSizeBytes: BigInt(partPlan.partSizeBytes),
          partCount: partPlan.partCount,
          parts: [],
          expiresAt: attemptExpiresAt,
        },
      });
      attempt = { id: attemptId, uploadId, partSizeBytes: BigInt(partPlan.partSizeBytes), partCount: partPlan.partCount };
    }

    const pendingResult = await prisma.$transaction(async (tx) => {
      const media = await tx.mediaAsset.create({
        data: {
          userId: request.authUser!.id,
          sessionId,
          audioObjectId: audioObject.id,
          status: "PENDING_UPLOAD",
          objectKey: audioObject.objectKey,
          originalName: safeFileName(input.originalName),
          mimeType: input.mimeType,
          sizeBytes: input.sizeBytes,
          sha256: digest,
          expiresAt: attemptExpiresAt,
        },
        select: mediaSelect,
      });
      await recordObjectEvent(tx, {
        action: "MEDIA_UPLOAD_INITIATED",
        result: "SUCCESS",
        audioObjectId: audioObject.id,
        mediaAssetId: media.id,
        sessionId,
        userId: request.authUser!.id,
        objectKey: audioObject.objectKey,
        sha256: digest,
        detail: { uploadId: attempt!.uploadId, attemptId: attempt!.id, resumed: candidate?.id === attempt!.id },
      });
      return {
        media,
        reused: false,
        upload: {
          audioObjectId: audioObject.id,
          uploadId: attempt!.uploadId,
          attemptId: attempt!.id,
          partSizeBytes: Number(attempt!.partSizeBytes),
          partCount: attempt!.partCount,
          expiresAt: attemptExpiresAt,
          partUrlsEndpoint: `/api/v1/media/${media.id}/upload-parts`,
        },
      };
    });

    await audit(
      request,
      "MEDIA_UPLOAD_INITIATED",
      "MEDIA_ASSET",
      pendingResult.media.id,
      "SUCCESS",
      { audioObjectId: audioObject.id, resumed: candidate?.id === attempt?.id },
    );
    return reply.status(201).send(pendingResult);
  });

  app.get("/media/:mediaId/upload-state", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      include: { audioObject: { include: { uploadAttempts: { where: { status: "ACTIVE" }, orderBy: { createdAt: "desc" }, take: 1 } } } },
    });
    if (!media) throw notFound();
    const attempt = media.audioObject?.uploadAttempts[0];
    if (!attempt || attempt.expiresAt < new Date()) {
      return { status: media.status, upload: null };
    }
    if (!attempt || attempt.expiresAt < new Date()) {
      if (attempt) {
        await prisma.uploadAttempt
          .update({ where: { id: attempt.id }, data: { status: "ABORTED", failureCode: "UPLOAD_SESSION_EXPIRED" } })
          .catch(() => undefined);
      }
      return { status: media.status, upload: null };
    }
    const serverParts = await listUploadedParts(media.objectKey, attempt.uploadId);
    if (serverParts === null) {
      // DB 记录滞后（例如已被 Worker 回收）：通知客户端重新创建上传会话
      await prisma.uploadAttempt
        .update({ where: { id: attempt.id }, data: { status: "ABORTED", failureCode: "UPLOAD_SESSION_MISSING" } })
        .catch(() => undefined);
      return { status: media.status, upload: null };
    }
    return {
      status: media.status,
      upload: {
        audioObjectId: media.audioObjectId,
        uploadId: attempt.uploadId,
        attemptId: attempt.id,
        partSizeBytes: Number(attempt.partSizeBytes),
        partCount: attempt.partCount,
        expiresAt: attempt.expiresAt,
        completedParts: (serverParts ?? []).map((part) => ({ partNumber: part.partNumber, etag: part.etag, sizeBytes: part.sizeBytes })),
      },
    };
  });

  app.post("/media/:mediaId/upload-parts", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const { partNumbers } = parseOrThrow(partNumbersSchema, request.body ?? {});
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      include: { audioObject: { include: { uploadAttempts: { where: { status: "ACTIVE" }, orderBy: { createdAt: "desc" }, take: 1 } } } },
    });
    if (!media) throw notFound();
    const attempt = media.audioObject?.uploadAttempts[0];
    if (!attempt || attempt.expiresAt < new Date()) {
      throw new AppError(409, "UPLOAD_SESSION_EXPIRED", "分片上传会话已过期，请重新创建");
    }
    const uniqueParts = [...new Set(partNumbers)];
    if (uniqueParts.some((number) => number > attempt.partCount)) {
      throw new AppError(400, "UPLOAD_PART_INVALID", `分片编号必须在 1..${attempt.partCount} 之间`);
    }
    const liveParts = await listUploadedParts(media.objectKey, attempt.uploadId);
    if (liveParts === null) {
      await prisma.uploadAttempt
        .update({ where: { id: attempt.id }, data: { status: "ABORTED", failureCode: "UPLOAD_SESSION_MISSING" } })
        .catch(() => undefined);
      throw new AppError(409, "UPLOAD_SESSION_MISSING", "分片上传会话不存在，请重新创建上传");
    }
    const urls: Array<{ partNumber: number; url: string }> = [];
    for (const partNumber of uniqueParts) {
      urls.push({ partNumber, url: await createPartUploadUrl(media.objectKey, attempt.uploadId, partNumber) });
    }
    return {
      uploadId: attempt.uploadId,
      partSizeBytes: Number(attempt.partSizeBytes),
      partCount: attempt.partCount,
      urls,
    };
  });

  app.post("/media/:mediaId/complete-upload", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      include: { audioObject: { include: { uploadAttempts: { where: { status: "ACTIVE" }, orderBy: { createdAt: "desc" }, take: 1 } } } },
    });
    if (!media) throw notFound();
    if (media.status === "READY") return { media, reused: true, probeQueued: false };
    if (!["PENDING_UPLOAD", "UPLOADING", "UPLOADED", "FAILED"].includes(media.status)) {
      throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频状态不能确认上传");
    }
    if (!media.audioObject || !media.audioObject.uploadAttempts[0]) {
      throw new AppError(409, "UPLOAD_SESSION_MISSING", "上传会话不存在，请重新创建上传");
    }
    const attempt = media.audioObject.uploadAttempts[0];
    if (attempt.expiresAt < new Date()) {
      await prisma.uploadAttempt.update({
        where: { id: attempt.id },
        data: { status: "ABORTED", failureCode: "UPLOAD_SESSION_EXPIRED" },
      });
      throw new AppError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已过期，请重新创建");
    }

    // 同摘要并发确认：数据库唯一约束保证只有一个物理对象；
    // 若另一请求已完成合并/探测，本次确认直接复用结果，不再触碰对象存储。
    const currentObject = await prisma.audioObject.findUniqueOrThrow({ where: { id: media.audioObject.id } });
    if (currentObject.status === "READY") {
      const updated = await prisma.mediaAsset.update({
        where: { id: media.id },
        data: {
          status: "READY",
          durationMs: currentObject.durationMs,
          codec: currentObject.codec,
          sampleRate: currentObject.sampleRate,
          channels: currentObject.channels,
          peaks: currentObject.peaks ?? undefined,
          uploadedAt: currentObject.uploadedAt ?? undefined,
          processedAt: currentObject.processedAt ?? undefined,
        },
        select: mediaSelect,
      });
      return { media: updated, reused: true, probeQueued: false };
    }
    if (currentObject.status === "UPLOADED" || currentObject.status === "PROCESSING") {
      await prisma.mediaAsset.update({ where: { id: media.id }, data: { status: currentObject.status } });
      throw new AppError(409, "MEDIA_FINALIZING", "另一请求正在确认相同内容，请稍后查询状态");
    }

    // 仅一个确认者走到这里：合并分片 + 服务端全量 SHA-256 校验
    const claimed = await prisma.audioObject.updateMany({
      where: { id: currentObject.id, status: { in: ["UPLOADING", "FAILED"] } },
      data: { status: "PROCESSING" },
    });
    if (claimed.count === 0) {
      throw new AppError(409, "MEDIA_FINALIZING", "另一请求正在确认相同内容，请稍后查询状态");
    }

    const partSizeBytes = Number(attempt.partSizeBytes);
    let mergedParts: Array<{ partNumber: number; etag: string }>;
    try {
      mergedParts = await mergeAndVerifyObject({
        objectKey: media.objectKey,
        uploadId: attempt.uploadId,
        partCount: attempt.partCount,
        partSizeBytes,
        sizeBytes: media.sizeBytes,
        sha256: media.sha256,
      });
    } catch (error) {
      if (error instanceof AppError && error.code === "UPLOAD_PARTS_INCOMPLETE") {
        // 缺片：对象仍等待补齐分片，回到 UPLOADING
        await prisma.audioObject.update({ where: { id: currentObject.id }, data: { status: "UPLOADING" } });
        throw error;
      }
      if (error instanceof AppError && error.code === "UPLOAD_SESSION_MISSING") {
        // S3 会话已不存在：作废记录，客户端重新发起上传时会创建新会话
        await prisma.$transaction(async (tx) => {
          await tx.audioObject.update({ where: { id: currentObject.id }, data: { status: "UPLOADING" } });
          await tx.uploadAttempt.updateMany({
            where: { audioObjectId: currentObject.id, status: "ACTIVE" },
            data: { status: "ABORTED", failureCode: "UPLOAD_SESSION_MISSING" },
          });
        });
        throw error;
      }
      if (error instanceof AppError && error.code === "UPLOAD_HASH_MISMATCH") {
        // 摘要不匹配：物理对象内容不可信，标记 FAILED 隔离；需要重新发起上传覆盖该内容寻址 Key
        await prisma.$transaction(async (tx) => {
          await tx.audioObject.update({
            where: { id: currentObject.id },
            data: { status: "FAILED", failureCode: "UPLOAD_HASH_MISMATCH", failureMessage: "合并后摘要不一致" },
          });
          await tx.uploadAttempt.update({
            where: { id: attempt.id },
            data: { status: "FAILED", failureCode: "UPLOAD_HASH_MISMATCH", failureMessage: "合并后摘要不一致" },
          });
          await tx.mediaAsset.updateMany({
            where: { audioObjectId: currentObject.id, status: { in: ["PENDING_UPLOAD", "UPLOADING"] } },
            data: { status: "FAILED", failureCode: "UPLOAD_HASH_MISMATCH", failureMessage: "合并后摘要不一致，请重新上传" },
          });
          await recordObjectEvent(tx, {
            action: "MEDIA_UPLOAD_VERIFY_FAILED",
            result: "FAILURE",
            audioObjectId: currentObject.id,
            mediaAssetId: media.id,
            sessionId: media.sessionId,
            userId: media.userId,
            objectKey: media.objectKey,
            sha256: media.sha256,
            detail: { code: error.code },
          });
        });
        throw error;
      }
      await prisma.audioObject.update({ where: { id: currentObject.id }, data: { status: "UPLOADING" } });
      throw error;
    }

    const updatedMedia = await prisma.$transaction(async (tx) => {
      const now = new Date();
      await tx.audioObject.update({
        where: { id: currentObject.id },
        data: { status: "UPLOADED", uploadedAt: now, failureCode: null, failureMessage: null },
      });
      await tx.uploadAttempt.update({
        where: { id: attempt.id },
        data: {
          status: "COMPLETED",
          completedAt: now,
          parts: mergedParts as never,
          failureCode: null,
          failureMessage: null,
        },
      });
      const updated = await tx.mediaAsset.update({
        where: { id: media.id },
        data: { status: "UPLOADED", uploadedAt: now, failureCode: null, failureMessage: null },
        select: mediaSelect,
      });
      // 同一物理对象的其他待确认引用一并转为 UPLOADED
      await tx.mediaAsset.updateMany({
        where: { audioObjectId: currentObject.id, status: { in: ["PENDING_UPLOAD", "UPLOADING", "FAILED"] } },
        data: { status: "UPLOADED", uploadedAt: now },
      });
      await recordObjectEvent(tx, {
        action: "MEDIA_UPLOAD_CONFIRMED",
        result: "SUCCESS",
        audioObjectId: currentObject.id,
        mediaAssetId: media.id,
        sessionId: media.sessionId,
        userId: media.userId,
        objectKey: media.objectKey,
        sha256: media.sha256,
        detail: { partCount: mergedParts.length, partSizeBytes },
      });
      return updated;
    });

    // 对象已确认，队列短暂不可用时不回滚：媒体保持 UPLOADED，
    // 前端轮询期间可通过 retry-probe 重新投递，Worker 侧按对象幂等。
    let probeQueued = true;
    try {
      await enqueueProbe(currentObject.id);
    } catch {
      probeQueued = false;
    }
    await audit(request, "MEDIA_UPLOADED", "MEDIA_ASSET", media.id, "SUCCESS", { audioObjectId: currentObject.id, probeQueued });
    return { media: updatedMedia, reused: false, probeQueued };
  });

  app.get("/media/:mediaId", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id }, select: mediaSelect });
    if (!media) throw notFound();
    return { media };
  });

  app.get("/media/:mediaId/playback-url", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    if (media.status !== "READY") throw new AppError(409, "MEDIA_NOT_READY", "音频尚未完成校验");
    const url = await createPlaybackUrl(media.objectKey, media.originalName, media.mimeType);
    return { url, expiresIn: getConfig().PLAYBACK_URL_TTL_SECONDS };
  });

  app.post("/media/:mediaId/retry-probe", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      include: { audioObject: true },
    });
    if (!media) throw notFound();
    if (media.audioObject) {
      if (!["FAILED", "UPLOADED"].includes(media.audioObject.status)) {
        throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频不需要重试解析");
      }
      await prisma.audioObject.update({
        where: { id: media.audioObject.id },
        data: { status: "UPLOADED", failureCode: null, failureMessage: null },
      });
      await prisma.mediaAsset.updateMany({
        where: { audioObjectId: media.audioObject.id, status: "FAILED" },
        data: { status: "UPLOADED", failureCode: null, failureMessage: null },
      });
      await enqueueProbe(media.audioObject.id);
      return { success: true, status: "UPLOADED" };
    }
    // 兼容回填前的历史数据
    if (!["FAILED", "UPLOADED"].includes(media.status)) {
      throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频不需要重试解析");
    }
    await prisma.mediaAsset.update({
      where: { id: media.id },
      data: { status: "UPLOADED", failureCode: null, failureMessage: null },
    });
    await enqueueProbe(media.id, { legacyMediaId: media.id });
    return { success: true, status: "UPLOADED" };
  });

  app.delete("/media/:mediaId", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();

    // 引用清理：只有最后一个引用被移除时才真正删除物理对象，全程写事件链。
    // 对象存储删除失败时保留媒体引用与对象行，事件链记录失败，客户端可重试。
    const referenceCount = await prisma.mediaAsset.count({ where: { objectKey: media.objectKey } });
    if (referenceCount === 1) {
      const activeAttempts = media.audioObjectId
        ? await prisma.uploadAttempt.findMany({
            where: { audioObjectId: media.audioObjectId, status: "ACTIVE" },
          })
        : [];
      for (const attempt of activeAttempts) {
        await abortMultipartUpload(media.objectKey, attempt.uploadId).catch(() => undefined);
        await prisma.uploadAttempt.update({
          where: { id: attempt.id },
          data: { status: "ABORTED", failureCode: "REFERENCE_DELETED" },
        });
      }
      try {
        await deleteObject(media.objectKey);
      } catch (error) {
        const message = error instanceof Error ? error.message : "OBJECT_DELETE_FAILED";
        await recordObjectEvent(prisma, {
          action: "MEDIA_OBJECT_DELETE_FAILED",
          result: "FAILURE",
          audioObjectId: media.audioObjectId,
          mediaAssetId: media.id,
          sessionId: media.sessionId,
          userId: media.userId,
          objectKey: media.objectKey,
          sha256: media.sha256,
          detail: { referenceCountBefore: referenceCount, error: message },
        }).catch(() => undefined);
        await audit(request, "MEDIA_DELETE_FAILED", "MEDIA_ASSET", media.id, "FAILURE", { objectKey: media.objectKey });
        throw new AppError(500, "OBJECT_DELETE_FAILED", "物理对象删除失败，引用已保留，请稍后重试");
      }

      await prisma.$transaction(async (tx) => {
        await tx.mediaAsset.delete({ where: { id: media.id } });
        if (media.audioObjectId) {
          await tx.uploadAttempt.deleteMany({ where: { audioObjectId: media.audioObjectId } });
          await tx.audioObject.delete({ where: { id: media.audioObjectId } });
        }
        await recordObjectEvent(tx, {
          action: "MEDIA_OBJECT_DELETED",
          result: "SUCCESS",
          audioObjectId: media.audioObjectId,
          mediaAssetId: media.id,
          sessionId: media.sessionId,
          userId: media.userId,
          objectKey: media.objectKey,
          sha256: media.sha256,
          detail: { referenceCountBefore: referenceCount, objectDeleted: true },
        });
      });
      await audit(request, "MEDIA_DELETED", "MEDIA_ASSET", media.id, "SUCCESS", { objectDeleted: true, referenceCount });
      return { success: true, objectDeleted: true, referenceCountAfter: 0 };
    }

    await prisma.$transaction(async (tx) => {
      await tx.mediaAsset.delete({ where: { id: media.id } });
      await recordObjectEvent(tx, {
        action: "MEDIA_REFERENCE_REMOVED",
        result: "SUCCESS",
        audioObjectId: media.audioObjectId,
        mediaAssetId: media.id,
        sessionId: media.sessionId,
        userId: media.userId,
        objectKey: media.objectKey,
        sha256: media.sha256,
        detail: { referenceCountBefore: referenceCount, objectDeleted: false },
      });
    });
    await audit(request, "MEDIA_DELETED", "MEDIA_ASSET", media.id, "SUCCESS", { objectDeleted: false, referenceCount });
    return { success: true, objectDeleted: false, referenceCountAfter: referenceCount - 1 };
  });

  // 引用清理追溯：查看某条音频背后的物理对象及完整引用事件链
  app.get("/media/:mediaId/object-history", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    const [events, referenceCount, object] = await Promise.all([
      prisma.mediaObjectEvent.findMany({
        where: {
          OR: [
            { mediaAssetId: mediaId },
            ...(media.audioObjectId ? [{ audioObjectId: media.audioObjectId }] : []),
            { objectKey: media.objectKey },
          ],
        },
        orderBy: { createdAt: "desc" },
        take: 100,
      }),
      prisma.mediaAsset.count({ where: { objectKey: media.objectKey } }),
      media.audioObjectId ? prisma.audioObject.findUnique({ where: { id: media.audioObjectId } }) : Promise.resolve(null),
    ]);
    return { object, referenceCount, events };
  });
};

export default mediaRoutes;
