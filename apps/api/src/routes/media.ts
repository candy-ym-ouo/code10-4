import type { FastifyPluginAsync } from "fastify";
import { uploadInitSchema, uploadPartsQuerySchema } from "@practice/contracts";
import { getConfig } from "../config/env.js";
import { AppError, notFound } from "../lib/errors.js";
import { prisma } from "../lib/prisma.js";
import { abortUpload, completeUpload, enqueueProbeForObject, getUploadState, initUpload, signParts } from "../lib/media-service.js";
import { createPlaybackUrl, deleteObject } from "../lib/s3.js";
import { parseOrThrow } from "../lib/validation.js";
import { audit } from "../lib/audit.js";

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

const mediaSelect = {
  id: true,
  sessionId: true,
  objectId: true,
  status: true,
  originalName: true,
  mimeType: true,
  sizeBytes: true,
  sha256: true,
  durationMs: true,
  codec: true,
  sampleRate: true,
  channels: true,
  peaks: true,
  failureCode: true,
  failureMessage: true,
  uploadedAt: true,
  processedAt: true,
  createdAt: true,
  updatedAt: true,
} as const;

const mediaRoutes: FastifyPluginAsync = async (app) => {
  app.addHook("preHandler", app.authenticate);

  app.post("/sessions/:sessionId/media/uploads", async (request, reply) => {
    const config = getConfig();
    const { sessionId } = request.params as { sessionId: string };
    const input = parseOrThrow(uploadInitSchema, request.body);
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
        _count: { select: { mediaAssets: { where: { status: { not: "CANCELLED" } } } } },
        mediaAssets: { select: { status: true, sizeBytes: true } },
      },
    });
    if (!session) throw notFound();
    if (!["DRAFT", "IN_REVIEW"].includes(session.status)) {
      throw new AppError(409, "INVALID_SESSION_STATE", "当前练习状态不能继续上传音频");
    }
    if (session._count.mediaAssets >= config.MAX_MEDIA_PER_SESSION) {
      throw new AppError(400, "MEDIA_LIMIT_REACHED", `每个练习最多 ${config.MAX_MEDIA_PER_SESSION} 个音频`);
    }
    const total = session.mediaAssets
      .filter((item) => item.status !== "CANCELLED")
      .reduce((sum, item) => sum + item.sizeBytes, 0n);
    const maxTotal = BigInt(config.MAX_SESSION_TOTAL_MB) * 1024n * 1024n;
    if (total + input.sizeBytes > maxTotal) {
      throw new AppError(413, "SESSION_SIZE_LIMIT_REACHED", `单次练习音频总量不能超过 ${config.MAX_SESSION_TOTAL_MB} MB`);
    }

    const result = await initUpload(request.authUser!.id, sessionId, {
      originalName: input.originalName,
      mimeType: input.mimeType,
      sizeBytes: input.sizeBytes,
      sha256: input.sha256,
      ...(input.partSize ? { partSize: input.partSize } : {}),
    });

    // upload 为 null 表示内容复用：对象已存在（READY 直接可用，或探测中等待结果），
    // 客户端零传输，只需按返回的媒体状态展示或轮询。
    if (!result.upload) {
      await audit(request, "MEDIA_REUSED", "MEDIA_ASSET", result.media.id, "SUCCESS", {
        status: result.media.status,
      });
      return reply.status(201).send({
        media: result.media,
        reused: true,
        upload: null,
        partUrls: [],
        expiresAt: null,
      });
    }

    // 首批预签名只签缺失分片：续传场景下已落盘分片不重复签发、不重传。
    const upload = result.upload;
    const alreadyUploaded = new Set(upload.uploadedPartNumbers);
    const missingPartNumbers = upload.parts.map((part) => part.partNumber).filter((partNumber) => !alreadyUploaded.has(partNumber));
    const partUrls = await signParts(request.authUser!.id, upload.mediaId, missingPartNumbers);
    await audit(request, "MEDIA_UPLOAD_INITIATED", "MEDIA_ASSET", upload.mediaId, "SUCCESS", {
      totalParts: upload.totalParts,
      partSize: upload.partSize,
      resumedParts: upload.uploadedPartNumbers.length,
    });
    return reply.status(201).send({
      media: result.media,
      reused: false,
      upload: {
        mediaId: upload.mediaId,
        uploadId: upload.uploadId,
        partSize: upload.partSize,
        totalParts: upload.totalParts,
        parts: upload.parts,
        uploadedPartNumbers: upload.uploadedPartNumbers,
      },
      partUrls,
      expiresAt: new Date(Date.now() + config.UPLOAD_URL_TTL_SECONDS * 1000),
    });
  });

  // 网络恢复/页面重试时查询服务端已收到的分片，只续传缺失对象。
  app.get("/media/:mediaId/upload-state", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const upload = await getUploadState(request.authUser!.id, mediaId);
    return { upload };
  });

  // 为指定分片签发预签名 PUT URL（批量，逗号分隔）。
  app.get("/media/:mediaId/upload-parts", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const { partNumbers } = parseOrThrow(uploadPartsQuerySchema, request.query);
    const partUrls = await signParts(request.authUser!.id, mediaId, partNumbers);
    return { partUrls };
  });

  app.post("/media/:mediaId/complete-upload", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const result = await completeUpload(request.authUser!.id, mediaId);
    const media = await prisma.mediaAsset.findUniqueOrThrow({ where: { id: mediaId }, select: mediaSelect });
    await audit(request, "MEDIA_UPLOADED", "MEDIA_ASSET", mediaId, "SUCCESS", {
      objectId: result.objectId,
      deduplicated: !result.owner,
    });
    return { media, probeQueued: result.owner };
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
      include: { object: true },
    });
    if (!media) throw notFound();
    // 历史遗留：迁移前的 UPLOADED 记录可能还没有 object_id，补登记后再投递探测。
    if (!media.objectId) {
      if (!["UPLOADED", "FAILED"].includes(media.status)) {
        throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频不需要重试解析");
      }
      const object = await prisma.mediaObject.upsert({
        where: { media_objects_user_sha256_key: { userId: media.userId, sha256: media.sha256 } },
        update: {},
        create: {
          userId: media.userId,
          objectKey: media.objectKey,
          sha256: media.sha256,
          status: "UPLOADED",
          sizeBytes: media.sizeBytes,
          mimeType: media.mimeType,
          uploadedAt: media.uploadedAt ?? new Date(),
        },
      });
      await prisma.mediaAsset.update({ where: { id: media.id }, data: { objectId: object.id } });
      await enqueueProbeForObject(object.id);
      return { success: true, status: "UPLOADED" };
    }
    const object = media.object;
    if (!object || object.status !== "FAILED") {
      throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频不需要重试解析");
    }
    await prisma.$transaction([
      prisma.mediaObject.update({
        where: { id: object.id },
        data: { status: "UPLOADED", failureCode: null, failureMessage: null },
      }),
      prisma.mediaAsset.updateMany({
        where: { objectId: object.id, status: "FAILED" },
        data: { status: "UPLOADED", failureCode: null, failureMessage: null },
      }),
    ]);
    await enqueueProbeForObject(object.id);
    return { success: true, status: "UPLOADED" };
  });

  // 删除单个引用：仍有其它引用时只解除关联；最后一个引用删除时连带删除物理对象。
  // 引用计数以 media_objects 为准，全程写审计日志，引用清理可追溯。
  app.delete("/media/:mediaId", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      include: { object: { select: { id: true } } },
    });
    if (!media) throw notFound();

    const otherRefs = await prisma.mediaAsset.count({
      where: { id: { not: media.id }, OR: [{ objectId: media.objectId }, { objectKey: media.objectKey }] },
    });

    if (otherRefs > 0) {
      // 共享对象：只删除引用，物理内容保留给其它练习。
      await prisma.mediaAsset.delete({ where: { id: media.id } });
      await audit(request, "MEDIA_REFERENCE_REMOVED", "MEDIA_OBJECT", media.objectId, "SUCCESS", {
        assetId: media.id,
        remainingReferences: otherRefs,
      });
      return { success: true };
    }

    // 最后一个引用：先删 S3 内容；删除失败时保留数据库记录以便重试。
    await deleteObject(media.objectKey);
    await prisma.$transaction([
      prisma.mediaAsset.delete({ where: { id: media.id } }),
      ...(media.objectId ? [prisma.mediaObject.delete({ where: { id: media.objectId } })] : []),
    ]);
    await audit(request, "MEDIA_OBJECT_DELETED", "MEDIA_OBJECT", media.objectId, "SUCCESS", {
      objectKey: media.objectKey,
      assetId: media.id,
    });
    return { success: true };
  });

  // 主动放弃上传：中止 S3 分片上传，避免产生孤儿分片持续计费。
  app.post("/media/:mediaId/abort-upload", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    if (["READY", "CANCELLED"].includes(media.status)) return { success: true };
    await abortUpload(media);
    await prisma.mediaAsset.update({
      where: { id: media.id },
      data: { status: "CANCELLED", uploadId: null, uploadPartSize: null, failureCode: "UPLOAD_ABORTED" },
    });
    await audit(request, "MEDIA_UPLOAD_ABORTED", "MEDIA_ASSET", media.id, "SUCCESS");
    return { success: true };
  });
};

export default mediaRoutes;
