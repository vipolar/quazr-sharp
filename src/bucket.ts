import { S3Client, GetObjectCommand, ChecksumAlgorithm } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Upload } from '@aws-sdk/lib-storage';
import { TaskError } from './errors.js';
import { Readable } from 'stream';
import pQueue from 'p-queue';

export class ImageBucket {
    private cache: Map<string, Buffer<ArrayBufferLike>>;
    private readonly queueTimeoutMs: number;
    private readonly maxCacheSize: number;
    private readonly s3Bucket: string;
    private cacheSize: number;
    private queue: pQueue;
    private s3: S3Client;

    constructor(maxCacheSize?: number, queueTimeoutMs?: number, s3?: {s3BucketHost?: string, s3BucketPort?: string, s3Bucket?: string, s3Password?: string, s3User?: string}) {
        const S3_BUCKET_HOST = s3?.s3BucketHost || process.env['BE_BUCKET_HOST'];
        const S3_BUCKET_PORT = s3?.s3BucketPort || process.env['BE_BUCKET_PORT'];
        const S3_PASSWORD = s3?.s3Password || process.env['BE_BUCKET_PASS'];
        const S3_USER = s3?.s3User || process.env['BE_BUCKET_KEY'];

        if (!S3_BUCKET_HOST || !S3_BUCKET_PORT || !S3_PASSWORD || !S3_USER) {
            throw new Error("Invalid S3 credentials or parameters");
        }

        const QUEUE_TIMEOUT_MS = queueTimeoutMs || parseInt(process.env['SHARP_QUEUE_TIMEOUT_MS'] || '30000', 10);
        const SHARP_CACHE_SIZE = maxCacheSize || parseInt(process.env['SHARP_CACHE_SIZE'] || '500000000', 10);

        if (QUEUE_TIMEOUT_MS <= 0) {
            throw new Error("Optional env variable QUEUE_TIMEOUT_MS must be a positive integer if set");
        }

        if (SHARP_CACHE_SIZE <= 0) {
            throw new Error("Optional env variable SHARP_CACHE_SIZE must be a positive integer if set");
        }

        this.s3Bucket = s3?.s3Bucket || process.env['BE_IMAGES_BUCKET'] || 'quazr-images';
        this.queue = new pQueue({ concurrency: 1 });
        this.queueTimeoutMs = QUEUE_TIMEOUT_MS;
        this.maxCacheSize = SHARP_CACHE_SIZE;
        this.cache = new Map();
        this.cacheSize = 0;

        this.s3 = new S3Client({
            endpoint: `http://${S3_BUCKET_HOST}:${S3_BUCKET_PORT}`,
            region: 'us-east-1',
            credentials: {
                secretAccessKey: S3_PASSWORD,
                accessKeyId: S3_USER,
            },
            requestHandler: new NodeHttpHandler({
                connectionTimeout: 15000,
                requestTimeout: 90000,
            }),
            forcePathStyle: true,
            maxAttempts: 3
        });
    };

    async uploadImageToBucket(outputKey: string, processedImage: Buffer<ArrayBufferLike>, format: string) {
        try {
            const upload = new Upload({
                client: this.s3,
                params: {
                    Key: outputKey,
                    Body: processedImage,
                    Bucket: this.s3Bucket,
                    ContentType: `image/${format}`,
                    ChecksumAlgorithm: ChecksumAlgorithm.CRC32,
                }
            });

            await upload.done();
        } catch (error: unknown) {
            if (error instanceof Error && error.name) {
                switch (error.name) {
                    case "NoSuchBucket":
                        throw new TaskError("No such bucket is present in the S3 instance", error, 40501);
                    case "TimeoutError":
                        throw new TaskError("Operation failed due to a network timeout error", error, 40408);
                    case "AccessDenied":
                        throw new TaskError("Permission denied while uploading the image to S3", error, 40403);
                    case "EntityTooLarge":
                        throw new TaskError("Operation failed due to image file being too large", error, 40413);
                    case "InvalidObjectState":
                        throw new TaskError("S3 client rejected due to being in an invalid state", error, 40409);
                    case "CredentialsProviderError":
                        throw new TaskError("Operation failed due to storage authentication error", error, 40423);
                    case "S3ServiceException":
                        throw new TaskError("Unexpected error while transmitting image to S3", error, 40500);
                    default:
                        throw new TaskError("Unknown error while transmitting image to S3", error, 40000);
                }
            }

            throw new TaskError("Unknown error while transmitting image to S3", error, 40000);
        }
    };

    async cacheImageBuffer(imageKey: string, imageBuffer: Buffer<ArrayBufferLike>): Promise<void> {
        return this.queue.add(
            async () => {
                try {
                    this.cache.delete(imageKey);
                    const imageBufferLength = imageBuffer.byteLength;
                    
                    if (imageBufferLength >= this.maxCacheSize) {
                        throw new Error("Image buffer size exceeds cache size limit");
                    }

                    this.cache.set(imageKey, imageBuffer);
                    this.cacheSize += imageBufferLength;

                    while (this.cacheSize > this.maxCacheSize) {
                        const oldestKey = this.cache.keys().next().value;
                        
                        if (oldestKey) {
                            const oldestValue = this.cache.get(oldestKey);
                            if (oldestValue) {
                                this.cacheSize -= oldestValue.byteLength;
                                this.cache.delete(oldestKey);
                            }
                        } else {
                            this.cacheSize = 0;
                        }
                    }
                } catch (error: unknown) {
                    throw new TaskError("Unexpected error while caching image buffer", error, 31001);
                }
            }, { timeout: this.queueTimeoutMs }
        );
    };

    async downloadImageFromBucket(imageKey: string): Promise<Readable> {
        let readStream: Readable | undefined;

        try {
            const getCommand = new GetObjectCommand({
                ChecksumMode: "ENABLED",
                Bucket: this.s3Bucket,
                Key: imageKey
            });

            const { Body } = await this.s3.send(getCommand);
            readStream = Body as Readable | undefined;
        } catch (error: unknown) {
            if (error instanceof Error && error.name) {
                switch (error.name) {
                    case "NoSuchKey":
                        throw new TaskError("No such image is present in the S3 bucket", error, 10404);
                    case "NoSuchBucket":
                        throw new TaskError("No such bucket is present in the S3 instance", error, 10501);
                    case "TimeoutError":
                        throw new TaskError("Operation failed due to a network timeout error", error, 10408);
                    case "AccessDenied":
                        throw new TaskError("Permission denied while trying to access S3 image", error, 10403);
                    case "InvalidObjectState":
                        throw new TaskError("S3 client rejected due to being in an invalid state", error, 10409);
                    case "CredentialsProviderError":
                        throw new TaskError("Operation failed due to storage authentication error", error, 10423);
                    case "S3ServiceException":
                        throw new TaskError("Unexpected error while retrieving image from S3", error, 10500);
                    default:
                        throw new TaskError("Unknown error while retrieving image from S3", error, 10000);
                }
            }

            throw new TaskError('Unknown error while retrieving image from S3', error, 10000);
        }

        if (!readStream || !(readStream instanceof Readable)) {
            throw new TaskError('Failed to retrieve image stream from S3 bucket', 10500);
        }

        return readStream;
    };

    getImageBuffer(imageKey: string): Buffer<ArrayBufferLike> | undefined {
        return this.cache.get(imageKey);
    };

    getCacheStats(): { size: number; maxSize: number; entryCount: number } {
        return {
            size: this.cacheSize,
            maxSize: this.maxCacheSize,
            entryCount: this.cache.size,
        };
    };

    clearCache(): void {
        this.cache.clear();
        this.cacheSize = 0;
    };
};
