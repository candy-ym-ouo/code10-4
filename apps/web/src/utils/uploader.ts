import { reactive } from "vue";
import { apiFetch, ApiError } from "../api/client.js";
import { sha256Hex } from "../utils/sha256.js";

export type UploadStatus =
  | "计算摘要"
  | "初始化"
  | "上传中"
  | "等待系统校验"
  | "READY"
  | "FAILED"
  | "CANCELLED";

export interface UploadItem {
  id: string;
  file: File;
  status: UploadStatus;
  progress: number;
  mediaId?: string;
  error?: string;
  /** 已确认上传到对象存储的分片序号（断点续传依据）。 */
  uploadedParts: Set<number>;
  abortController?: AbortController;
  /** 分片上传尝试次数，用于失败重试时的指数退避。 */
  attempts: number;
}

interface PartRange {
  partNumber: number;
  start: number;
  end: number;
}

interface InitResponse {
  media: { id: string; status: string };
  reused: boolean;
  upload: {
    mediaId: string;
    uploadId: string;
    partSize: number;
    totalParts: number;
    parts: PartRange[];
    uploadedPartNumbers: number[];
  } | null;
  partUrls: Array<{ partNumber: number; url: string }>;
}

const PART_RETRY_LIMIT = 5;
const PART_BACKOFF_MS = 1500;

function isNetworkError(error: unknown): boolean {
  return error instanceof TypeError || (error instanceof ApiError && (error.status >= 500 || error.code === "UPLOAD_PARTS_MISSING"));
}

/**
 * 分片上传单个分片：网络抖动时自动指数退避重试，
 * 重试次数用尽后抛出，由上层走断点续传恢复流程。
 */
function uploadPartWithRetry(
  url: string,
  blob: Blob,
  signal: AbortSignal,
  onProgress: (loaded: number) => void,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("PUT", url);
    request.responseType = "";
    request.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded);
    };
    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        resolve((request.getResponseHeader("ETag") ?? "").replace(/^"|"$/g, ""));
      } else {
        reject(new Error(`对象存储返回 ${request.status}`));
      }
    };
    request.onerror = () => reject(new Error("上传连接中断"));
    request.ontimeout = () => reject(new Error("上传超时"));
    request.onabort = () => reject(new DOMException("aborted", "AbortError"));
    signal.addEventListener("abort", () => request.abort());
    request.send(blob);
  });
}

async function withRetry<T>(label: string, run: () => Promise<T>): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < PART_RETRY_LIMIT; attempt += 1) {
    try {
      return await run();
    } catch (error) {
      lastError = error;
      if (error instanceof DOMException && error.name === "AbortError") throw error;
      if (!isNetworkError(error) && attempt >= 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, PART_BACKOFF_MS * 2 ** attempt));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(`${label} 失败`);
}

export interface UploadCallbacks {
  /** 对象已确认合并（或直接复用），由回调负责轮询探测结果并更新 item 终态。 */
  onReady: (item: UploadItem, mediaId: string, reused: boolean) => Promise<void> | void;
}

export function createUploader(sessionId: string) {
  const uploads = reactive<UploadItem[]>([]);

  function mediaType(file: File): string {
    if (file.type.startsWith("audio/")) return file.type;
    const extension = file.name.split(".").pop()?.toLowerCase();
    return (
      ({ mp3: "audio/mpeg", m4a: "audio/mp4", wav: "audio/wav", ogg: "audio/ogg", flac: "audio/flac", webm: "audio/webm" } as Record<string, string>)[
        extension ?? ""
      ] ?? "audio/mpeg"
    );
  }

  async function initSession(file: File, digest: string, mimeType: string): Promise<InitResponse> {
    return apiFetch<InitResponse>(`/api/v1/sessions/${sessionId}/media/uploads`, {
      method: "POST",
      body: JSON.stringify({ originalName: file.name, mimeType, sizeBytes: file.size, sha256: digest }),
    });
  }

  /**
   * 断点恢复：以服务端 ListParts 结果为准，只续传缺失对象。
   * 返回缺失分片的最新预签名 URL。
   */
  async function fetchMissingParts(mediaId: string, plan: PartRange[]): Promise<{
    missing: PartRange[];
    urls: Map<number, string>;
    uploaded: Set<number>;
  }> {
    const state = await apiFetch<{ upload: { parts: PartRange[]; uploadedPartNumbers: number[] } }>(
      `/api/v1/media/${mediaId}/upload-state`,
    );
    const uploaded = new Set(state.upload.uploadedPartNumbers);
    const missing = plan.filter((part) => !uploaded.has(part.partNumber));
    if (missing.length === 0) return { missing, urls: new Map(), uploaded };
    const query = missing.map((part) => part.partNumber).join(",");
    const signed = await apiFetch<{ partUrls: Array<{ partNumber: number; url: string }> }>(
      `/api/v1/media/${mediaId}/upload-parts?partNumbers=${encodeURIComponent(query)}`,
    );
    return { missing, urls: new Map(signed.partUrls.map((item) => [item.partNumber, item.url])), uploaded };
  }

  async function runUpload(item: UploadItem, callbacks: UploadCallbacks): Promise<void> {
    try {
      item.status = "计算摘要";
      const digest = await sha256Hex(item.file);
      const mimeType = mediaType(item.file);

      item.status = "初始化";
      // 已有 mediaId（断点重试）：先以服务端 ListParts 为准同步状态，只补缺失分片，
      // 不重新计算摘要、不重建会话、不重复创建媒体记录。
      if (item.mediaId) {
        const descriptor = await apiFetch<{ upload: { parts: PartRange[] } }>(
          `/api/v1/media/${item.mediaId}/upload-state`,
        );
        const resumedPlan = descriptor.upload.parts;
        const refreshed = await fetchMissingParts(item.mediaId, resumedPlan);
        const resumedUploaded = new Set(refreshed.uploaded);
        item.uploadedParts = resumedUploaded;
        item.status = "上传中";
        const controller = new AbortController();
        item.abortController = controller;
        await uploadMissingParts(item, resumedPlan, refreshed.urls, resumedUploaded, controller);
        item.status = "等待系统校验";
        await apiFetch(`/api/v1/media/${item.mediaId}/complete-upload`, { method: "POST", body: "{}" });
        item.progress = 100;
        await callbacks.onReady(item, item.mediaId, false);
        return;
      }

      const creation = await initSession(item.file, digest, mimeType);
      item.mediaId = creation.media.id;

      if (creation.reused || !creation.upload) {
        item.progress = 100;
        item.uploadedParts = new Set();
        // READY 对象零传输直接完成；探测中的复用引用走轮询等待结果。
        if (creation.media.status === "READY") {
          await callbacks.onReady(item, creation.media.id, true);
        } else {
          item.status = "等待系统校验";
          await callbacks.onReady(item, creation.media.id, false);
        }
        return;
      }

      const plan = creation.upload.parts;
      const urlByPart = new Map(creation.partUrls.map((entry) => [entry.partNumber, entry.url]));
      item.uploadedParts = new Set(creation.upload.uploadedPartNumbers);
      item.status = "上传中";

      const controller = new AbortController();
      item.abortController = controller;
      await uploadMissingParts(item, plan, urlByPart, item.uploadedParts, controller);

      item.status = "等待系统校验";
      await apiFetch(`/api/v1/media/${item.mediaId}/complete-upload`, { method: "POST", body: "{}" });
      item.progress = 100;
      await callbacks.onReady(item, item.mediaId!, false);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        item.status = "CANCELLED";
        return;
      }
      item.status = "FAILED";
      item.error = error instanceof ApiError ? error.message : error instanceof Error ? error.message : "上传失败";
      item.attempts += 1;
    }
  }

  /**
   * 顺序上传缺失分片。单片网络错误自动指数退避重试；
   * 重试前调用服务端 upload-state 校正进度，保证网络恢复只续传缺失对象。
   */
  async function uploadMissingParts(
    item: UploadItem,
    plan: PartRange[],
    initialUrls: Map<number, string>,
    uploaded: Set<number>,
    controller: AbortController,
  ): Promise<void> {
    const totalSize = item.file.size;
    let urls = initialUrls;
    const uploadedBytes = () =>
      plan.reduce((sum, part) => sum + (uploaded.has(part.partNumber) ? part.end - part.start : 0), 0);
    const refreshProgress = () => {
      item.progress = Math.min(99, Math.round((uploadedBytes() / totalSize) * 100));
    };
    refreshProgress();

    for (const part of plan) {
      if (controller.signal.aborted) throw new DOMException("aborted", "AbortError");
      if (uploaded.has(part.partNumber)) continue;

      let url = urls.get(part.partNumber);
      if (!url) {
        const refreshed = await fetchMissingParts(item.mediaId!, plan);
        refreshed.uploaded.forEach((number) => uploaded.add(number));
        urls = refreshed.urls;
        if (uploaded.has(part.partNumber)) {
          refreshProgress();
          continue;
        }
        url = urls.get(part.partNumber);
        if (!url) throw new Error(`分片 ${part.partNumber} 的上传地址不可用`);
      }

      const blob = item.file.slice(part.start, part.end);
      await withRetry(`分片 ${part.partNumber}`, async () => {
        if (uploaded.has(part.partNumber)) return;
        await uploadPartWithRetry(url!, blob, controller.signal, (loaded) => {
          item.progress = Math.min(99, Math.round(((uploadedBytes() + Math.min(loaded, blob.size)) / totalSize) * 100));
        });
        uploaded.add(part.partNumber);
      });
      refreshProgress();
    }
  }

  function addFiles(files: FileList | File[], callbacks: UploadCallbacks): void {
    for (const file of Array.from(files)) {
      const item = reactive<UploadItem>({
        id: crypto.randomUUID(),
        file,
        status: "计算摘要",
        progress: 0,
        uploadedParts: new Set<number>(),
        attempts: 0,
      });
      uploads.push(item);
      void runUpload(item, callbacks);
    }
  }

  /** 断点重试：复用同一 UploadItem（含 mediaId 与已传分片），网络恢复只补缺失分片。 */
  function retry(item: UploadItem, callbacks: UploadCallbacks): void {
    item.error = undefined;
    void runUpload(item, callbacks);
  }

  async function abort(item: UploadItem): Promise<void> {
    item.abortController?.abort();
    if (item.mediaId) {
      await apiFetch(`/api/v1/media/${item.mediaId}/abort-upload`, { method: "POST" }).catch(() => undefined);
    }
    item.status = "CANCELLED";
  }

  return { uploads, addFiles, retry, abort };
}
