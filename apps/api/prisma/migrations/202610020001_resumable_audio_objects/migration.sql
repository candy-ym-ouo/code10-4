-- 物理音频对象、分片上传会话与引用事件链

-- CreateEnum
CREATE TYPE "AudioObjectStatus" AS ENUM ('UPLOADING', 'UPLOADED', 'PROCESSING', 'READY', 'FAILED');

-- CreateEnum
CREATE TYPE "UploadAttemptStatus" AS ENUM ('ACTIVE', 'COMPLETED', 'ABORTED', 'FAILED');

-- CreateTable
CREATE TABLE "audio_objects" (
    "id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "status" "AudioObjectStatus" NOT NULL DEFAULT 'UPLOADING',
    "object_key" TEXT NOT NULL,
    "sha256" CHAR(64) NOT NULL,
    "size_bytes" BIGINT NOT NULL,
    "mime_type" VARCHAR(100),
    "duration_ms" BIGINT,
    "codec" VARCHAR(64),
    "sample_rate" INTEGER,
    "channels" SMALLINT,
    "peaks" JSONB,
    "failure_code" VARCHAR(64),
    "failure_message" VARCHAR(500),
    "uploaded_at" TIMESTAMPTZ(6),
    "processed_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "audio_objects_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "upload_attempts" (
    "id" UUID NOT NULL,
    "audio_object_id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "upload_id" VARCHAR(255) NOT NULL,
    "status" "UploadAttemptStatus" NOT NULL DEFAULT 'ACTIVE',
    "part_size_bytes" BIGINT NOT NULL,
    "part_count" INTEGER NOT NULL,
    "parts" JSONB NOT NULL,
    "expires_at" TIMESTAMPTZ(6) NOT NULL,
    "completed_at" TIMESTAMPTZ(6),
    "failure_code" VARCHAR(64),
    "failure_message" VARCHAR(500),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "upload_attempts_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "media_object_events" (
    "id" UUID NOT NULL,
    "audio_object_id" UUID,
    "user_id" UUID,
    "media_asset_id" UUID,
    "session_id" UUID,
    "action" VARCHAR(64) NOT NULL,
    "result" VARCHAR(32) NOT NULL,
    "object_key" TEXT,
    "sha256" CHAR(64),
    "detail" JSONB,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "media_object_events_pkey" PRIMARY KEY ("id")
);

-- AlterTable
ALTER TABLE "media_assets" ADD COLUMN "audio_object_id" UUID;

-- CreateIndex
CREATE UNIQUE INDEX "audio_objects_object_key_key" ON "audio_objects"("object_key");
CREATE UNIQUE INDEX "audio_objects_user_id_sha256_key" ON "audio_objects"("user_id", "sha256");
CREATE INDEX "audio_objects_user_id_status_idx" ON "audio_objects"("user_id", "status");
CREATE INDEX "upload_attempts_upload_id_key" ON "upload_attempts"("upload_id");
CREATE INDEX "upload_attempts_audio_object_id_status_idx" ON "upload_attempts"("audio_object_id", "status");
CREATE INDEX "upload_attempts_status_expires_at_idx" ON "upload_attempts"("status", "expires_at");
CREATE INDEX "media_object_events_audio_object_id_created_at_idx" ON "media_object_events"("audio_object_id", "created_at");
CREATE INDEX "media_object_events_media_asset_id_created_at_idx" ON "media_object_events"("media_asset_id", "created_at");
CREATE INDEX "media_object_events_user_id_created_at_idx" ON "media_object_events"("user_id", "created_at");
CREATE INDEX "media_assets_audio_object_id_idx" ON "media_assets"("audio_object_id");

-- AddForeignKey
ALTER TABLE "audio_objects" ADD CONSTRAINT "audio_objects_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "upload_attempts" ADD CONSTRAINT "upload_attempts_audio_object_id_fkey" FOREIGN KEY ("audio_object_id") REFERENCES "audio_objects"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "upload_attempts" ADD CONSTRAINT "upload_attempts_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "media_assets" ADD CONSTRAINT "media_assets_audio_object_id_fkey" FOREIGN KEY ("audio_object_id") REFERENCES "audio_objects"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- 回填：每个 (user_id, sha256) 归并为一个物理对象，优先选取已就绪/最近的记录
INSERT INTO "audio_objects" (
    "id", "user_id", "status", "object_key", "sha256", "size_bytes", "mime_type",
    "duration_ms", "codec", "sample_rate", "channels", "peaks",
    "failure_code", "failure_message", "uploaded_at", "processed_at",
    "created_at", "updated_at"
)
SELECT
    gen_random_uuid(),
    m."user_id",
    CASE
        WHEN bool_or(m."status" = 'READY') THEN 'READY'::"AudioObjectStatus"
        WHEN bool_or(m."status" IN ('UPLOADED', 'PROCESSING')) THEN 'UPLOADED'::"AudioObjectStatus"
        WHEN bool_or(m."status" = 'FAILED') THEN 'FAILED'::"AudioObjectStatus"
        ELSE 'UPLOADING'::"AudioObjectStatus"
    END,
    (array_agg(m."object_key" ORDER BY
        CASE m."status" WHEN 'READY' THEN 0 WHEN 'PROCESSING' THEN 1 WHEN 'UPLOADED' THEN 2 ELSE 3 END,
        m."created_at" DESC))[1],
    m."sha256",
    MAX(m."size_bytes"),
    (array_agg(m."mime_type" ORDER BY
        CASE m."status" WHEN 'READY' THEN 0 WHEN 'PROCESSING' THEN 1 WHEN 'UPLOADED' THEN 2 ELSE 3 END,
        m."created_at" DESC))[1],
    (array_agg(m."duration_ms" ORDER BY m."created_at" DESC) FILTER (WHERE m."duration_ms" IS NOT NULL))[1],
    (array_agg(m."codec" ORDER BY m."created_at" DESC) FILTER (WHERE m."codec" IS NOT NULL))[1],
    (array_agg(m."sample_rate" ORDER BY m."created_at" DESC) FILTER (WHERE m."sample_rate" IS NOT NULL))[1],
    (array_agg(m."channels" ORDER BY m."created_at" DESC) FILTER (WHERE m."channels" IS NOT NULL))[1],
    (array_agg(m."peaks" ORDER BY m."created_at" DESC) FILTER (WHERE m."peaks" IS NOT NULL))[1],
    (array_agg(m."failure_code" ORDER BY m."created_at" DESC) FILTER (WHERE m."failure_code" IS NOT NULL))[1],
    (array_agg(m."failure_message" ORDER BY m."created_at" DESC) FILTER (WHERE m."failure_message" IS NOT NULL))[1],
    MIN(m."uploaded_at"),
    MAX(m."processed_at"),
    MIN(m."created_at"),
    MAX(m."updated_at")
FROM "media_assets" m
GROUP BY m."user_id", m."sha256";

UPDATE "media_assets" m
SET "audio_object_id" = o."id"
FROM "audio_objects" o
WHERE o."user_id" = m."user_id" AND o."sha256" = m."sha256";

-- 回填后仍未指向物理对象表的旧记录（理论上不会出现）保留 NULL，不影响后续上传流程
