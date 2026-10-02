-- Physical audio objects are decoupled from MediaAsset references:
-- one verified object (sha256) can be referenced by many assets/sessions.
CREATE TYPE "MediaObjectStatus" AS ENUM ('UPLOADED', 'PROCESSING', 'READY', 'FAILED');

CREATE TABLE "media_objects" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "user_id" UUID NOT NULL,
    "object_key" TEXT NOT NULL,
    "sha256" CHAR(64) NOT NULL,
    "status" "MediaObjectStatus" NOT NULL DEFAULT 'UPLOADED',
    "size_bytes" BIGINT NOT NULL,
    "mime_type" VARCHAR(100) NOT NULL,
    "duration_ms" BIGINT,
    "codec" VARCHAR(64),
    "sample_rate" INTEGER,
    "channels" SMALLINT,
    "peaks" JSONB,
    "failure_code" VARCHAR(64),
    "failure_message" VARCHAR(500),
    "uploaded_at" TIMESTAMPTZ(6),
    "processed_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    CONSTRAINT "media_objects_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "media_objects_object_key_key" ON "media_objects"("object_key");
CREATE UNIQUE INDEX "media_objects_user_id_sha256_key" ON "media_objects"("user_id", "sha256");
CREATE INDEX "media_objects_user_id_status_idx" ON "media_objects"("user_id", "status");

ALTER TABLE "media_assets" ADD COLUMN "object_id" UUID;
ALTER TABLE "media_assets" ADD COLUMN "upload_id" VARCHAR(256);
ALTER TABLE "media_assets" ADD COLUMN "upload_part_size" INTEGER;

-- Backfill one physical object per distinct (user, object_key). Before this
-- migration reused assets already shared object keys, so the canonical row is
-- simply the earliest asset; every co-keyed asset becomes a reference.
INSERT INTO "media_objects" (
    "user_id", "object_key", "sha256", "status", "size_bytes", "mime_type",
    "duration_ms", "codec", "sample_rate", "channels", "peaks",
    "uploaded_at", "processed_at", "created_at", "updated_at"
)
SELECT DISTINCT ON ("user_id", "object_key")
    "user_id",
    "object_key",
    "sha256",
    CASE
        WHEN "status" = 'READY' THEN 'READY'::"MediaObjectStatus"
        WHEN "status" = 'FAILED' THEN 'FAILED'::"MediaObjectStatus"
        WHEN "status" IN ('UPLOADED', 'PROCESSING') THEN 'UPLOADED'::"MediaObjectStatus"
        ELSE 'UPLOADED'::"MediaObjectStatus"
    END,
    "size_bytes",
    "mime_type",
    "duration_ms",
    "codec",
    "sample_rate",
    "channels",
    "peaks",
    "uploaded_at",
    "processed_at",
    "created_at",
    "updated_at"
FROM "media_assets"
WHERE "status" IN ('READY', 'UPLOADED', 'PROCESSING', 'FAILED')
ORDER BY "user_id", "object_key",
    CASE "status" WHEN 'READY' THEN 0 WHEN 'PROCESSING' THEN 1 WHEN 'UPLOADED' THEN 2 ELSE 3 END,
    "created_at" ASC;

UPDATE "media_assets" asset
SET "object_id" = object."id"
FROM "media_objects" object
WHERE asset."user_id" = object."user_id"
  AND asset."object_key" = object."object_key";

ALTER TABLE "media_assets"
ADD CONSTRAINT "media_assets_object_id_fkey"
FOREIGN KEY ("object_id") REFERENCES "media_objects"("id")
ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX "media_assets_object_id_idx" ON "media_assets"("object_id");
CREATE INDEX "media_assets_upload_id_idx" ON "media_assets"("upload_id") WHERE "upload_id" IS NOT NULL;
