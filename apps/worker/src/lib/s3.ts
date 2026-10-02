import {
  AbortMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
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

/** 中止分片上传，释放已上传但未合并的分片占用的存储。 */
export async function abortMultipartUpload(objectKey: string, uploadId: string): Promise<void> {
  await getS3().send(
    new AbortMultipartUploadCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey, UploadId: uploadId }),
  );
}

/** 探测对象是否存在；404 视为不存在，其余错误向上抛出。 */
export async function objectExists(objectKey: string): Promise<boolean> {
  try {
    await getS3().send(new HeadObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }));
    return true;
  } catch (error) {
    if ((error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404) return false;
    throw error;
  }
}
