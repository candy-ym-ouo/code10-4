import {
  AbortMultipartUploadCommand,
  GetObjectCommand,
  ListMultipartUploadsCommand,
  PutObjectCommand,
  S3Client,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import type { Readable } from "node:stream";
import { getConfig } from "../config/env.js";

let client: S3Client | undefined;

export function getS3(): S3Client {
  if (!client) {
    const config = getConfig();
    client = new S3Client({
      endpoint: config.S3_ENDPOINT,
      region: config.S3_REGION,
      forcePathStyle: config.S3_FORCE_PATH_STYLE,
      credentials: { accessKeyId: config.S3_ACCESS_KEY, secretAccessKey: config.S3_SECRET_KEY },
    });
  }
  return client;
}

export async function getObjectStream(objectKey: string): Promise<Readable> {
  const object = await getS3().send(new GetObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }));
  if (!object.Body) throw new Error("object body is empty");
  return object.Body as Readable;
}

export async function putObject(objectKey: string, body: string, contentType: string): Promise<void> {
  await getS3().send(
    new PutObjectCommand({
      Bucket: getConfig().S3_BUCKET,
      Key: objectKey,
      Body: body,
      ContentType: contentType,
    }),
  );
}

export async function deleteObject(objectKey: string): Promise<void> {
  await getS3().send(new DeleteObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }));
}

export interface InProgressUpload {
  key: string;
  uploadId: string;
  initiated: Date | undefined;
}

/** 列出 Bucket 中所有进行中的分片上传（用于回收过期会话）。 */
export async function listInProgressUploads(): Promise<InProgressUpload[]> {
  const bucket = getConfig().S3_BUCKET;
  const uploads: InProgressUpload[] = [];
  let marker: string | undefined;
  for (;;) {
    const result = await getS3().send(new ListMultipartUploadsCommand({ Bucket: bucket, KeyMarker: marker }));
    for (const upload of result.Uploads ?? []) {
      if (upload.Key && upload.UploadId) {
        uploads.push({ key: upload.Key, uploadId: upload.UploadId, initiated: upload.Initiated });
      }
    }
    if (!result.IsTruncated) break;
    marker = result.NextKeyMarker;
    if (!marker) break;
  }
  return uploads;
}

export async function abortMultipartUpload(objectKey: string, uploadId: string): Promise<boolean> {
  try {
    await getS3().send(
      new AbortMultipartUploadCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey, UploadId: uploadId }),
    );
    return true;
  } catch (error) {
    if ((error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404) return false;
    throw error;
  }
}
