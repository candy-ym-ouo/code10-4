import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().url(),
  S3_ENDPOINT: z.string().url(),
  S3_REGION: z.string().min(1).default("us-east-1"),
  S3_BUCKET: z.string().min(1).default("practice-audio"),
  S3_ACCESS_KEY: z.string().min(1),
  S3_SECRET_KEY: z.string().min(1),
  S3_FORCE_PATH_STYLE: z.string().optional().transform((value) => value == null || value.toLowerCase() === "true"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(3),
  MULTIPART_TTL_HOURS: z.coerce.number().int().min(1).max(168).default(24),
  MULTIPART_SWEEP_CRON_MS: z.coerce.number().int().min(60_000).default(3_600_000),
});

let cached: z.infer<typeof envSchema> | undefined;

export function getConfig() {
  if (cached) return cached;
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    throw new Error(result.error.issues.map((issue) => `${issue.path.join(".")}: ${issue.message}`).join("; "));
  }
  cached = result.data;
  return cached;
}
