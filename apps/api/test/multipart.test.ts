import { describe, expect, it, vi } from "vitest";

// env 必须在导入 s3/config 之前提供，且避免整个 app 模块链加载 argon2
vi.stubEnv("DATABASE_URL", "postgresql://u:p@localhost:5432/db");
vi.stubEnv("REDIS_URL", "redis://localhost:6379");
vi.stubEnv("JWT_ACCESS_SECRET", "test-secret-test-secret-test-secret-1234");
vi.stubEnv("REFRESH_TOKEN_PEPPER", "test-pepper-test-pepper-test-pepper-1234");
vi.stubEnv("S3_ENDPOINT", "http://localhost:9000");
vi.stubEnv("S3_ACCESS_KEY", "x");
vi.stubEnv("S3_SECRET_KEY", "x");
vi.stubEnv("PUBLIC_API_ORIGIN", "http://localhost:3000");
vi.stubEnv("WEB_ORIGIN", "http://localhost:5173");

const { validateServerParts } = await import("../src/lib/media-object.js");

const parts = (numbers: number[], size: number) =>
  numbers.map((partNumber) => ({ partNumber, sizeBytes: size, etag: `etag-${partNumber}` }));

describe("validateServerParts", () => {
  const partSize = 8 * 1024 * 1024;

  it("accepts a complete contiguous upload with a shorter last part", () => {
    const complete = [...parts([1, 2], partSize), { partNumber: 3, sizeBytes: 4 * 1024 * 1024, etag: "etag-3" }];
    expect(() => validateServerParts(complete, 3, partSize)).not.toThrow();
  });

  it("rejects when a part is missing and lists the missing numbers", () => {
    try {
      validateServerParts(parts([1, 3], partSize), 3, partSize);
      throw new Error("should have thrown");
    } catch (error) {
      expect((error as { code?: string }).code).toBe("UPLOAD_PARTS_INCOMPLETE");
      expect((error as { details?: unknown }).details).toEqual([2]);
    }
  });

  it("rejects non-contiguous numbering even when the count matches", () => {
    expect(() => validateServerParts(parts([1, 3], partSize), 2, partSize)).toThrow(/不连续/);
  });

  it("rejects an oversized last part", () => {
    const bad = [...parts([1, 2], partSize), { partNumber: 3, sizeBytes: partSize + 1, etag: "x" }];
    expect(() => validateServerParts(bad, 3, partSize)).toThrow(/最后一个分片/);
  });

  it("rejects an empty part list", () => {
    expect(() => validateServerParts([], 1, partSize)).toThrow();
  });
});
