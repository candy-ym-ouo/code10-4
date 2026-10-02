import { partByteRange, planMultipart, MULTIPART_DEFAULT_PART_BYTES } from "@practice/contracts";
import { apiFetch, ApiError } from "./client.js";

export interface UploadDescriptor {
  audioObjectId: string;
  uploadId: string;
  attemptId: string;
  partSizeBytes: number;
  partCount: number;
  expiresAt: string | Date;
  partUrlsEndpoint?: string;
  completedParts?: Array<{ partNumber: number; etag: string; sizeBytes: number }>;
}

export interface PartEtag {
  partNumber: number;
  etag: string;
}

const MAX_PART_ATTEMPTS = 5;
const STORAGE_PREFIX = "practice-upload:";

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** 等待浏览器网络恢复（online 事件），最多等 60 秒后也放行重试一次。 */
function waitForNetwork(): Promise<void> {
  if (typeof navigator === "undefined" || navigator.onLine !== false) return Promise.resolve();
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      window.removeEventListener("online", finish);
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(finish, 60_000);
    window.addEventListener("online", finish, { once: true });
  });
}

/** 查询服务端分片会话状态，网络恢复后用它判断哪些分片缺失，只续传缺失对象。 */
export async function fetchUploadState(mediaId: string): Promise<UploadDescriptor | null> {
  const result = await apiFetch<{ status: string; upload: UploadDescriptor | null }>(
    `/api/v1/media/${mediaId}/upload-state`,
  );
  return result.upload;
}

async function signParts(mediaId: string, partNumbers: number[]): Promise<Array<{ partNumber: number; url: string }>> {
  const result = await apiFetch<{ urls: Array<{ partNumber: number; url: string }> }>(
    `/api/v1/media/${mediaId}/upload-parts`,
    { method: "POST", body: JSON.stringify({ partNumbers }) },
  );
  return result.urls;
}

function putPart(url: string, blob: Blob, onProgress?: (loaded: number) => void): Promise<string> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("PUT", url);
    request.upload.onprogress = (event) => onProgress?.(event.loaded);
    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        const etag = request.getResponseHeader("ETag") ?? request.getResponseHeader("etag");
        if (!etag) {
          reject(new Error("对象存储未返回分片 ETag"));
          return;
        }
        resolve(etag.replace(/"/g, ""));
      } else {
        reject(new Error(`对象存储返回 ${request.status}`));
      }
    };
    request.onerror = () => reject(new Error("上传连接中断"));
    request.ontimeout = () => reject(new Error("分片上传超时"));
    request.send(blob);
  });
}

export interface ResumableUploadOptions {
  mediaId: string;
  file: File;
  descriptor: UploadDescriptor;
  /** 已知已完成的分片号（来自之前的会话或 upload-state 查询）。 */
  knownParts?: PartEtag[];
  onProgress?: (uploadedBytes: number, totalBytes: number) => void;
  shouldAbort?: () => boolean;
}

/**
 * 执行可断点续传的分片上传：
 * - 先以服务端 ListParts 为准确认已存在分片，网络恢复后只补传缺失分片；
 * - 每个分片独立指数退避重试，断网时等待 online 事件；
 * - 全部完成后由调用方请求 complete-upload，服务端做全量 SHA-256 校验。
 */
export async function uploadResumable(options: ResumableUploadOptions): Promise<PartEtag[]> {
  const { mediaId, file, descriptor } = options;
  const partSize = descriptor.partSizeBytes;
  const plan = planMultipart(file.size, partSize === MULTIPART_DEFAULT_PART_BYTES ? partSize : partSize);
  if (plan.partSizeBytes !== partSize || plan.partCount !== descriptor.partCount) {
    throw new Error("分片规划与服务端不一致，请重新创建上传");
  }

  // 服务端视角的已传分片优先；其次使用本地缓存的 ETag
  const serverState = await fetchUploadState(mediaId);
  const completed = new Map<number, string>();
  for (const part of serverState?.completedParts ?? options.knownParts ?? []) {
    completed.set(part.partNumber, part.etag);
  }

  const totalBytes = file.size;
  const uploadedBytesOf = (partNumber: number) =>
    partNumber === descriptor.partCount ? totalBytes - (partNumber - 1) * partSize : partSize;
  const reportProgress = () => {
    let uploaded = 0;
    for (const partNumber of completed.keys()) uploaded += uploadedBytesOf(partNumber);
    options.onProgress?.(uploaded, totalBytes);
  };
  reportProgress();

  for (let partNumber = 1; partNumber <= descriptor.partCount; partNumber += 1) {
    if (completed.has(partNumber)) continue;
    const { start, end } = partByteRange(partNumber, partSize, totalBytes);
    const blob = file.slice(start, end);

    let lastError: unknown;
    for (let attempt = 0; attempt < MAX_PART_ATTEMPTS; attempt += 1) {
      if (options.shouldAbort?.()) throw new Error("上传已取消");
      try {
        const urls = await signParts(mediaId, [partNumber]);
        const url = urls[0]?.url;
        if (!url) throw new Error("未获取到分片上传地址");
        const etag = await putPart(url, blob, () => reportProgress());
        completed.set(partNumber, etag);
        saveProgress(mediaId, descriptor, [...completed.entries()].map(([number, etag]) => ({ partNumber: number, etag })));
        reportProgress();
        lastError = undefined;
        break;
      } catch (error) {
        lastError = error;
        await waitForNetwork();
        await wait(Math.min(2_000 * 2 ** attempt, 30_000));
        // 重试前向服务端确认：该分片可能其实已上传成功，只是响应丢失
        const refreshed = await fetchUploadState(mediaId).catch(() => null);
        if (!refreshed || refreshed.uploadId !== descriptor.uploadId) {
          throw new ApiError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已失效，请重新创建");
        }
        const found = refreshed.completedParts?.find((part) => part.partNumber === partNumber);
        if (found) {
          completed.set(partNumber, found.etag);
          lastError = undefined;
          break;
        }
      }
    }
    if (lastError) throw lastError;
  }

  clearProgress(mediaId);
  return [...completed.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([partNumber, etag]) => ({ partNumber, etag }));
}

/** 批量预签名 + 并发上传，用于稳定网络下的首传，保持速度。 */
export async function uploadResumableParallel(options: ResumableUploadOptions, concurrency = 3): Promise<PartEtag[]> {
  const { mediaId, file, descriptor } = options;
  const partSize = descriptor.partSizeBytes;
  const serverState = await fetchUploadState(mediaId);
  const completed = new Map<number, string>();
  for (const part of serverState?.completedParts ?? options.knownParts ?? []) {
    completed.set(part.partNumber, part.etag);
  }

  const totalBytes = file.size;
  const uploadedBytesOf = (partNumber: number) =>
    partNumber === descriptor.partCount ? totalBytes - (partNumber - 1) * partSize : partSize;
  const reportProgress = () => {
    let uploaded = 0;
    for (const partNumber of completed.keys()) uploaded += uploadedBytesOf(partNumber);
    options.onProgress?.(uploaded, totalBytes);
  };
  reportProgress();

  const missing = Array.from({ length: descriptor.partCount }, (_, index) => index + 1).filter(
    (partNumber) => !completed.has(partNumber),
  );

  let cursor = 0;
  const workers = Array.from({ length: Math.min(concurrency, Math.max(1, missing.length)) }, async () => {
    for (;;) {
      const partNumber = missing[cursor];
      cursor += 1;
      if (partNumber == null) return;
      if (options.shouldAbort?.()) throw new Error("上传已取消");
      const { start, end } = partByteRange(partNumber, partSize, totalBytes);
      const blob = file.slice(start, end);

      let lastError: unknown;
      for (let attempt = 0; attempt < MAX_PART_ATTEMPTS; attempt += 1) {
        try {
          const urls = await signParts(mediaId, [partNumber]);
          const url = urls[0]?.url;
          if (!url) throw new Error("未获取到分片上传地址");
          const etag = await putPart(url, blob, reportProgress);
          completed.set(partNumber, etag);
          saveProgress(mediaId, descriptor, [...completed.entries()].map(([number, e]) => ({ partNumber: number, etag: e })));
          reportProgress();
          lastError = undefined;
          break;
        } catch (error) {
          lastError = error;
          await waitForNetwork();
          await wait(Math.min(2_000 * 2 ** attempt, 30_000));
          const refreshed = await fetchUploadState(mediaId).catch(() => null);
          if (!refreshed || refreshed.uploadId !== descriptor.uploadId) {
            throw new ApiError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已失效，请重新创建");
          }
          const found = refreshed.completedParts?.find((part) => part.partNumber === partNumber);
          if (found) {
            completed.set(partNumber, found.etag);
            lastError = undefined;
            break;
          }
        }
      }
      if (lastError) throw lastError;
    }
  });
  await Promise.all(workers);

  clearProgress(mediaId);
  return [...completed.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([partNumber, etag]) => ({ partNumber, etag }));
}

interface StoredUpload {
  mediaId: string;
  fileName: string;
  fileSize: number;
  sha256: string;
  descriptor: UploadDescriptor;
  parts: PartEtag[];
  updatedAt: string;
}

export function saveProgress(mediaId: string, descriptor: UploadDescriptor, parts: PartEtag[]): void {
  try {
    const key = `${STORAGE_PREFIX}${mediaId}`;
    const previous = readStored(mediaId);
    const stored: StoredUpload = {
      mediaId,
      fileName: previous?.fileName ?? "",
      fileSize: previous?.fileSize ?? 0,
      sha256: previous?.sha256 ?? "",
      descriptor,
      parts,
      updatedAt: new Date().toISOString(),
    };
    localStorage.setItem(key, JSON.stringify(stored));
  } catch {
    // localStorage 不可用时仅失去本地续传能力，服务端 ListParts 仍可兜底
  }
}

export function rememberUpload(mediaId: string, file: File, sha256: string, descriptor: UploadDescriptor): void {
  try {
    const stored: StoredUpload = {
      mediaId,
      fileName: file.name,
      fileSize: file.size,
      sha256,
      descriptor,
      parts: [],
      updatedAt: new Date().toISOString(),
    };
    localStorage.setItem(`${STORAGE_PREFIX}${mediaId}`, JSON.stringify(stored));
  } catch {
    // 忽略持久化失败
  }
}

function readStored(mediaId: string): StoredUpload | null {
  try {
    const raw = localStorage.getItem(`${STORAGE_PREFIX}${mediaId}`);
    return raw ? (JSON.parse(raw) as StoredUpload) : null;
  } catch {
    return null;
  }
}

export function readProgress(mediaId: string): StoredUpload | null {
  return readStored(mediaId);
}

export function clearProgress(mediaId: string): void {
  try {
    localStorage.removeItem(`${STORAGE_PREFIX}${mediaId}`);
  } catch {
    // 忽略
  }
}

/** 匹配本地缓存中与待传文件同摘要的未完成会话，用于页面重开后续传。 */
export function findStoredUpload(file: File, sha256: string): StoredUpload | null {
  try {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (!key?.startsWith(STORAGE_PREFIX)) continue;
      try {
        const stored = JSON.parse(localStorage.getItem(key) ?? "null") as StoredUpload | null;
        if (stored && stored.fileSize === file.size && stored.sha256 === sha256) return stored;
      } catch {
        // 跳过损坏记录
      }
    }
  } catch {
    // localStorage 不可用
  }
  return null;
}
