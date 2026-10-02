import path from "node:path";
import { Prisma } from "@prisma/client";
import { planMultipartUpload, MIN_UPLOAD_PART_BYTES } from "@practice/contracts";
import {
  abortMultipartUpload,
  completeMultipartUpload,
  createMultipartUpload,
  listUploadedParts,
  signUploadPart,
  verifyObject,
  type UploadedPart,
} from "./s3.js";
import { AppError } from "./errors.js";
import { getMediaQueue } from "./queue.js";
import { prisma } from "./prisma.js";

export interface UploadInitInput {
  originalName: string;
  mimeType: string;
  sizeBytes: bigint;
  sha256: string;
  partSize?: number;
}

export interface UploadDescriptor {
  mediaId: string;
  uploadId: string;
  partSize: number;
  totalParts: number;
  parts: Array<{ partNumber: number; start: number; end: number }>;
  uploadedPartNumbers: number[];
}

/**
 * 创建（或复用）上传会话。同一会话内重复选择同一文件时复用原记录；
 * 同摘要的不同并发上传在确认阶段才收敛到唯一物理对象。
 */
export async function initUpload(userId: string, sessionId: string, input: UploadInitInput): Promise<{
  media: { id: string; status: string; originalName: string; sizeBytes: bigint; createdAt: Date };
  upload: UploadDescriptor | null;
}> {
  const digest = input.sha256.toLowerCase();

  // 1. 已有就绪物理对象：直接创建引用，零网络传输。
  //    PROCESSING/UPLOADED 的对象意味着同摘要探测正在进行，直接等待并复用结果，
  //    避免第二个上传者重复传完整文件；FAILED 则继续走全新上传以便替换坏对象。
  const reusableObject = await prisma.mediaObject.findUnique({
    where: { media_objects_user_sha256_key: { userId, sha256: digest } },
  });
  if (reusableObject && reusableObject.status !== "FAILED") {
    const alreadyReady = reusableObject.status === "READY";
    const media = await prisma.mediaAsset.create({
      data: {
        userId,
        sessionId,
        objectId: reusableObject.id,
        status: alreadyReady ? "READY" : "UPLOADED",
        objectKey: reusableObject.objectKey,
        originalName: input.originalName,
        mimeType: input.mimeType,
        sizeBytes: reusableObject.sizeBytes,
        sha256: digest,
        durationMs: reusableObject.durationMs,
        codec: reusableObject.codec,
        sampleRate: reusableObject.sampleRate,
        channels: reusableObject.channels,
        peaks: reusableObject.peaks ?? undefined,
        uploadedAt: new Date(),
        processedAt: reusableObject.processedAt,
      },
      select: { id: true, status: true, originalName: true, sizeBytes: true, createdAt: true },
    });
    // READY 时客户端零传输直接展示；探测中时复用引用但让客户端轮询等结果。
    return { media, upload: null };
  }

  // 2. 同一会话内已有进行中的同摘要上传：断点续传，不重复创建记录。
  //    FAILED（合并/校验失败，UploadId 已被消费）与 UPLOADED 不在续传范围内。
  const existing = await prisma.mediaAsset.findFirst({
    where: {
      userId,
      sessionId,
      sha256: digest,
      status: { in: ["PENDING_UPLOAD", "UPLOADING"] },
      uploadId: { not: null },
    },
    orderBy: { createdAt: "desc" },
  });
  if (existing && existing.uploadId && existing.uploadPartSize) {
    if (existing.expiresAt && existing.expiresAt < new Date() && existing.status !== "UPLOADED") {
      // 上传会话已过期：中止旧会话后走新建流程。
      await abortMultipartUpload(existing.objectKey, existing.uploadId).catch(() => undefined);
      await prisma.mediaAsset.update({
        where: { id: existing.id },
        data: { status: "CANCELLED", failureCode: "UPLOAD_SESSION_EXPIRED", uploadId: null, uploadPartSize: null },
      });
    } else {
      return {
        media: {
          id: existing.id,
          status: existing.status,
          originalName: existing.originalName,
          sizeBytes: existing.sizeBytes,
          createdAt: existing.createdAt,
        },
        upload: await buildDescriptor(existing.id, existing.objectKey, existing.uploadId, Number(input.sizeBytes), existing.uploadPartSize),
      };
    }
  }

  // 3. 全新分片上传会话。
  const mediaId = crypto.randomUUID();
  const objectKey = `users/${userId}/sessions/${sessionId}/${mediaId}/${safeFileName(input.originalName)}`;
  const size = Number(input.sizeBytes);
  const plan = planMultipartUpload(size, input.partSize);
  const uploadId = await createMultipartUpload(objectKey, input.mimeType, digest);
  const media = await prisma.mediaAsset.create({
    data: {
      id: mediaId,
      userId,
      sessionId,
      status: "PENDING_UPLOAD",
      objectKey,
      uploadId,
      uploadPartSize: plan.partSize,
      originalName: input.originalName,
      mimeType: input.mimeType,
      sizeBytes: input.sizeBytes,
      sha256: digest,
      expiresAt: new Date(Date.now() + 24 * 60 * 60_000),
    },
    select: { id: true, status: true, originalName: true, sizeBytes: true, createdAt: true },
  });

  return {
    media,
    upload: {
      mediaId,
      uploadId,
      partSize: plan.partSize,
      totalParts: plan.parts.length,
      parts: plan.parts,
      uploadedPartNumbers: [],
    },
  };
}

async function buildDescriptor(
  mediaId: string,
  objectKey: string,
  uploadId: string,
  sizeBytes: number,
  partSize: number,
): Promise<UploadDescriptor> {
  const [plan, remoteParts] = await Promise.all([
    Promise.resolve(planMultipartUpload(sizeBytes, partSize)),
    // ListParts 失败（如对象存储短暂不可达或会话已被服务端回收）不阻断初始化，
    // 返回空列表让客户端走续传协商，真正缺片会在确认时暴露。
    listUploadedParts(objectKey, uploadId).catch(() => [] as UploadedPart[]),
  ]);  return {
    mediaId,
    uploadId,
    partSize,
    totalParts: plan.parts.length,
    parts: plan.parts,
    uploadedPartNumbers: remoteParts.map((part) => part.partNumber),
  };
}

/** 查询某个媒体资源的续传状态，只返回 S3 端已落盘的分片。 */
export async function getUploadState(userId: string, mediaId: string): Promise<UploadDescriptor> {
  const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId } });
  if (!media) throw new AppError(404, "RESOURCE_NOT_FOUND", "音频不存在或无权访问");
  if (!media.uploadId || !media.uploadPartSize) {
    throw new AppError(409, "INVALID_MEDIA_STATE", "该音频不是分片上传会话");
  }
  return buildDescriptor(media.id, media.objectKey, media.uploadId, Number(media.sizeBytes), media.uploadPartSize);
}

/** 为缺失分片批量签发预签名 PUT URL。 */
export async function signParts(
  userId: string,
  mediaId: string,
  partNumbers: number[],
): Promise<Array<{ partNumber: number; url: string }>> {
  const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId } });
  if (!media) throw new AppError(404, "RESOURCE_NOT_FOUND", "音频不存在或无权访问");
  if (!media.uploadId) throw new AppError(409, "INVALID_MEDIA_STATE", "该音频没有进行中的上传会话");
  if (["READY", "UPLOADED", "CANCELLED"].includes(media.status)) {
    throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频状态不能继续上传分片");
  }
  if (media.expiresAt && media.expiresAt < new Date()) {
    throw new AppError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已过期，请重新创建");
  }
  const totalParts = Math.ceil(Number(media.sizeBytes) / (media.uploadPartSize ?? MIN_UPLOAD_PART_BYTES));
  const urls = await Promise.all(
    [...new Set(partNumbers)]
      .filter((partNumber) => partNumber >= 1 && partNumber <= totalParts)
      .map(async (partNumber) => ({
        partNumber,
        url: await signUploadPart(media.objectKey, media.uploadId!, partNumber),
      })),
  );
  return urls;
}

/**
 * 确认上传：在事务内用 advisory lock 串行化同 (user, sha) 的所有确认，
 * 保证 ListParts→CompleteMultipartUpload→登记物理对象的流程只执行一次；
 * 同摘要并发确认只会生成一个物理对象，其余请求成为引用。
 */
export async function completeUpload(userId: string, mediaId: string): Promise<{ objectId: string; owner: boolean }> {
  const before = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId } });
  if (!before) throw new AppError(404, "RESOURCE_NOT_FOUND", "音频不存在或无权访问");
  if (before.status === "READY" && before.objectId) {
    return { objectId: before.objectId, owner: false };
  }
  if (before.status === "UPLOADED" && before.objectId) {
    // 物理对象已登记（可能是重复确认或探测中）：幂等返回，不重复合并对象。
    const object = await prisma.mediaObject.findUnique({ where: { id: before.objectId } });
    if (object) return { objectId: object.id, owner: object.objectKey === before.objectKey };
  }
  if (!["PENDING_UPLOAD", "UPLOADING", "FAILED"].includes(before.status) || !before.uploadId || !before.uploadPartSize) {
    throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频状态不能确认上传");
  }
  if (before.expiresAt && before.expiresAt < new Date()) {
    await prisma.mediaAsset.update({
      where: { id: before.id },
      data: { status: "FAILED", failureCode: "UPLOAD_SESSION_EXPIRED", failureMessage: "上传会话已过期，请重新创建" },
    });
    throw new AppError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已过期，请重新创建");
  }

  const { key1, key2 } = shaToLockKey(userId, before.sha256);
  let owner = false;
  let objectId = "";
  let objectKey = before.objectKey;
  let needsVerify = false;

  // ListParts 在事务外完成，避免慢网络占用数据库连接；它是分片是否真实落盘的
  // 唯一依据，网络恢复后缺失分片在这里暴露。
  const remoteParts = await listUploadedParts(before.objectKey, before.uploadId);
  const plan = planMultipartUpload(Number(before.sizeBytes), before.uploadPartSize);
  const missing = plan.parts
    .filter((part) => !remoteParts.some((remote) => remote.partNumber === part.partNumber))
    .map((part) => part.partNumber);
  if (missing.length > 0) {
    throw new AppError(409, "UPLOAD_PARTS_MISSING", "仍有分片未上传完成，请在网络恢复后续传", { missingParts: missing });
  }
  const orderedParts = [...remoteParts].sort((a, b) => a.partNumber - b.partNumber);

  await prisma.$transaction(
    async (tx) => {
      await tx.$executeRaw`SELECT pg_advisory_xact_lock(${key1}, ${key2})`;

      // 拿锁后重新读取：可能已有并发确认完成了同一摘要的物理对象登记与合并。
      const media = await tx.mediaAsset.findUniqueOrThrow({ where: { id: mediaId } });
      if (media.status === "UPLOADED" || media.status === "READY") {
        if (!media.objectId) throw new AppError(500, "UPLOAD_STATE_INCONSISTENT", "上传状态不一致，请稍后重试");
        const linked = await tx.mediaObject.findUniqueOrThrow({ where: { id: media.objectId } });
        objectId = linked.id;
        owner = linked.objectKey === media.objectKey && linked.status !== "FAILED";
        objectKey = linked.objectKey;
        return;
      }

      const existing = await tx.mediaObject.findUnique({
        where: { media_objects_user_sha256_key: { userId, sha256: media.sha256 } },
      });
      if (existing && existing.status !== "FAILED") {
        // 同摘要并发确认：只生成一个物理对象，本资源成为引用，无需再次合并分片。
        objectId = existing.id;
        owner = false;
        objectKey = existing.objectKey;
        await linkAssetToObject(tx, media.id, existing.id, existing.status === "READY", existing);
        return;
      }

      const now = new Date();
      const object = existing
        ? await tx.mediaObject.update({
            where: { id: existing.id },
            data: {
              objectKey: media.objectKey,
              status: "UPLOADED",
              sizeBytes: media.sizeBytes,
              mimeType: media.mimeType,
              durationMs: null,
              codec: null,
              sampleRate: null,
              channels: null,
              peaks: Prisma.JsonNull,
              processedAt: null,
              uploadedAt: now,
              failureCode: null,
              failureMessage: null,
            },
          })
        : await tx.mediaObject.create({
            data: {
              userId,
              objectKey: media.objectKey,
              sha256: media.sha256,
              status: "UPLOADED",
              sizeBytes: media.sizeBytes,
              mimeType: media.mimeType,
              uploadedAt: now,
            },
          });
      objectId = object.id;
      owner = true;
      objectKey = object.objectKey;
      needsVerify = true;
      await linkAssetToObject(tx, media.id, object.id, false, null, now);
    },
    { timeout: 30_000, maxWait: 30_000 },
  );

  if (!needsVerify) return { objectId, owner };

  // 合并 S3 分片放在事务提交之后：只有唯一的 owner 执行一次，
  // 避免并发确认对同一 UploadId 重复 CompleteMultipartUpload。
  try {
    await completeMultipartUpload(objectKey, before.uploadId, orderedParts);
  } catch (error) {
    await prisma.$transaction([
      prisma.mediaObject.update({
        where: { id: objectId },
        data: { status: "FAILED", failureCode: "UPLOAD_MERGE_FAILED", failureMessage: error instanceof Error ? error.message.slice(0, 500) : "分片合并失败" },
      }),
      prisma.mediaAsset.update({
        where: { id: mediaId },
        data: { status: "FAILED", failureCode: "UPLOAD_MERGE_FAILED", failureMessage: "分片合并失败，请重新上传" },
      }),
    ]);
    throw error;
  }

  // 对象内容校验只在所有者路径执行一次；引用直接复用校验结果。
  try {
    await verifyObject(objectKey, before.sizeBytes, before.sha256);
  } catch (error) {
    const code =
      error instanceof AppError && error.code === "UPLOAD_HASH_MISMATCH"
        ? "UPLOAD_HASH_MISMATCH"
        : "UPLOAD_VERIFY_FAILED";
    await prisma.$transaction([
      prisma.mediaObject.update({
        where: { id: objectId },
        data: { status: "FAILED", failureCode: code, failureMessage: error instanceof Error ? error.message.slice(0, 500) : "校验失败" },
      }),
      prisma.mediaAsset.update({
        where: { id: mediaId },
        data: { status: "FAILED", failureCode: code, failureMessage: "对象校验失败，请重新上传" },
      }),
    ]);
    throw error;
  }
  await enqueueProbeForObject(objectId);

  return { objectId, owner };
}

async function linkAssetToObject(
  tx: Prisma.TransactionClient,
  mediaId: string,
  objectId: string,
  ready: boolean,
  object: { durationMs: bigint | null; codec: string | null; sampleRate: number | null; channels: number | null; peaks: Prisma.JsonValue | null; processedAt: Date | null } | null,
  now = new Date(),
): Promise<void> {
  await tx.mediaAsset.update({
    where: { id: mediaId },
    data: {
      objectId,
      status: ready ? "READY" : "UPLOADED",
      uploadedAt: now,
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60_000),
      failureCode: null,
      failureMessage: null,
      ...(ready && object
        ? {
            durationMs: object.durationMs,
            codec: object.codec,
            sampleRate: object.sampleRate,
            channels: object.channels,
            peaks: object.peaks === null ? Prisma.JsonNull : (object.peaks as Prisma.InputJsonValue),
            processedAt: object.processedAt,
          }
        : {}),
    },
  });
}

/**
 * 投递物理对象探测任务。同一对象已存在活跃任务时不重复投递，
 * 避免并发确认或重复确认触发重复 ffmpeg 探测。
 */
export async function enqueueProbeForObject(objectId: string): Promise<void> {
  const queue = getMediaQueue();
  const active = await queue.getJobs(["active", "waiting", "delayed"]);
  if (active.some((job) => job.name === "probe-object" && job.data.objectId === objectId)) return;
  await queue.add(
    "probe-object",
    { objectId },
    {
      jobId: `probe:${objectId}:${Date.now()}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

/** 中止并清理一个未完成的分片上传。 */
export async function abortUpload(media: { objectKey: string; uploadId: string | null }): Promise<void> {
  if (media.uploadId) await abortMultipartUpload(media.objectKey, media.uploadId).catch(() => undefined);
}

function safeFileName(input: string): string {
  const base = path.basename(input).replace(/[^\p{L}\p{N}._-]+/gu, "_").slice(0, 180);
  return base || "audio";
}

function shaToLockKey(userId: string, sha256: string): { key1: bigint; key2: bigint } {
  // pg_advisory_xact_lock(bigint, bigint)：两个 64 位键组合标识同一摘要槽位。
  const userHex = userId.replace(/-/g, "");
  const key1 = BigInt(`0x${userHex.slice(0, 16)}`) ^ BigInt(`0x${userHex.slice(16, 32)}`);
  const key2 = BigInt(`0x${sha256.slice(0, 16)}`) ^ BigInt(`0x${sha256.slice(16, 32)}`);
  return { key1, key2 };
}
