import { createSHA256 } from "hash-wasm";

/** 分片流式计算文件 SHA-256，避免把整个大文件读入内存。 */
export async function sha256Hex(file: Blob): Promise<string> {
  const hasher = await createSHA256();
  hasher.init();
  const chunkSize = 4 * 1024 * 1024;
  for (let offset = 0; offset < file.size; offset += chunkSize) {
    const chunk = new Uint8Array(await file.slice(offset, offset + chunkSize).arrayBuffer());
    hasher.update(chunk);
  }
  return hasher.digest("hex");
}
