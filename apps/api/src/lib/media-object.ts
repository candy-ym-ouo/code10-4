import { randomUUID } from "node:crypto";
import type { Prisma } from "@prisma/client";
import { PrismaClient } from "@prisma/client";
import { planMultipart, MULTIPART_MAX_PART_COUNT } from "@practice/contracts";
import { getConfig } from "../config/env.js";
import { AppError } from "./errors.js";
import { completeMultipartUpload, listUploadedParts, verifyObject } from "./s3.js";

export type TxClient = Prisma.TransactionClient;

export const mediaSelect = {
  id: true,
  sessionId: true,
  audioObjectId: true,
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
  audioObject: { select: { id: true, status: true } },
} satisfies Prisma.MediaAssetSelect;

export interface MediaEventInput {
  action: string;
  result: "SUCCESS" | "FAILURE";
  audioObjectId?: string | null;
  mediaAssetId?: string | null;
  sessionId?: string | null;
  userId?: string | null;
  objectKey?: string | null;
  sha256?: string | null;
  detail?: Prisma.InputJsonValue;
}

/** 写入引用事件链；事件本身不带外键，物理对象删除后仍可追溯。 */
export async function recordObjectEvent(client: PrismaClient | TxClient, input: MediaEventInput): Promise<void> {
  await client.mediaObjectEvent.create({
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
      detail: input.detail ?? undefined,
    },
  });
}

/** 内容寻址对象 Key：同一用户同一摘要永远指向同一个物理对象。 */
export function contentObjectKey(userId: string, sha256: string): string {
  return `users/${userId}/audio/${sha256.slice(0, 2)}/${sha256}`;
}

export function computePartPlan(sizeBytes: number): { partSizeBytes: number; partCount: number } {
  const partSizeMb = getConfig().MULTIPART_PART_SIZE_MB;
  const plan = planMultipart(sizeBytes, partSizeMb * 1024 * 1024);
  if (plan.partCount > MULTIPART_MAX_PART_COUNT) {
    throw new AppError(413, "FILE_TOO_LARGE", "文件过大，超出分片上传上限");
  }
  return plan;
}

/**
 * 确保存在内容寻址的 AudioObject（并发安全）。
 * 同摘要并发创建时依赖 (user_id, sha256) 唯一约束，只有一方插入成功。
 */
export async function ensureAudioObject(
  client: TxClient,
  input: { userId: string; sha256: string; sizeBytes: bigint; mimeType: string },
) {
  const existing = await client.audioObject.findUnique({ where: { userId_sha256: { userId: input.userId, sha256: input.sha256 } } });
  if (existing) return existing;
  try {
    return await client.audioObject.create({
      data: {
        id: randomUUID(),
        userId: input.userId,
        status: "UPLOADING",
        objectKey: contentObjectKey(input.userId, input.sha256),
        sha256: input.sha256,
        sizeBytes: input.sizeBytes,
        mimeType: input.mimeType,
      },
    });
  } catch (error) {
    if ((error as { code?: string }).code === "P2002") {
      const raced = await client.audioObject.findUniqueOrThrow({
        where: { userId_sha256: { userId: input.userId, sha256: input.sha256 } },
      });
      return raced;
    }
    throw error;
  }
}

/**
 * 用 S3 服务端视角校验分片清单：必须 1..N 连续、除最后一片外均不小于 5 MiB。
 * 客户端声称的 ETag 不可信，最终以服务端 ListParts 结果合并。
 */
export function validateServerParts(
  serverParts: Array<{ partNumber: number; sizeBytes: number; etag: string }>,
  partCount: number,
  partSizeBytes: number,
): void {
  if (serverParts.length !== partCount) {
    const missing = Array.from({ length: partCount }, (_, index) => index + 1).filter(
      (number) => !serverParts.some((part) => part.partNumber === number),
    );
    throw new AppError(409, "UPLOAD_PARTS_INCOMPLETE", "仍有分片未上传，请续传后再确认", missing.slice(0, 100));
  }
  const sorted = [...serverParts].sort((a, b) => a.partNumber - b.partNumber);
  sorted.forEach((part, index) => {
    if (part.partNumber !== index + 1) {
      throw new AppError(409, "UPLOAD_PARTS_INCOMPLETE", "分片编号不连续，请续传缺失分片");
    }
    if (index < partCount - 1 && part.sizeBytes !== partSizeBytes) {
      throw new AppError(400, "UPLOAD_PART_SIZE_INVALID", `分片 ${part.partNumber} 大小异常，请重新上传该分片`);
    }
  });
  const lastPart = sorted[partCount - 1];
  if (!lastPart || lastPart.sizeBytes < 1 || lastPart.sizeBytes > partSizeBytes) {
    throw new AppError(400, "UPLOAD_PART_SIZE_INVALID", "最后一个分片大小异常，请重新上传");
  }
}

/** 事务外完成分片合并，然后服务端完整校验大小与 SHA-256。 */
export async function mergeAndVerifyObject(input: {
  objectKey: string;
  uploadId: string;
  partCount: number;
  partSizeBytes: number;
  sizeBytes: bigint;
  sha256: string;
}): Promise<Array<{ partNumber: number; etag: string }>> {
  const serverParts = await listUploadedParts(input.objectKey, input.uploadId);
  if (serverParts === null) {
    throw new AppError(409, "UPLOAD_SESSION_MISSING", "分片上传会话不存在或已过期，请重新创建上传");
  }
  validateServerParts(serverParts, input.partCount, input.partSizeBytes);
  await completeMultipartUpload(
    input.objectKey,
    input.uploadId,
    serverParts.map((part) => ({ partNumber: part.partNumber, etag: part.etag })),
  );
  await verifyObject(input.objectKey, input.sizeBytes, input.sha256);
  return serverParts.map((part) => ({ partNumber: part.partNumber, etag: part.etag }));
}
