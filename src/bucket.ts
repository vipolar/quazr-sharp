import { S3ServiceException, InvalidObjectState, AccessDenied, NoSuchKey } from '@aws-sdk/client-s3';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import type { Readable } from 'stream';
import { TaskError } from './errors.js';
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
            throw new Error("QUEUE_TIMEOUT_MS must be a positive integer");
        }

        if (SHARP_CACHE_SIZE <= 0) {
            throw new Error("SHARP_CACHE_SIZE must be a positive integer");
        }

        this.s3Bucket = s3?.s3Bucket || process.env['BE_BUCKET_NAME'] || 'test';
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
            forcePathStyle: true
        });
    };

    async uploadImageToBucket(outputKey: string, processedImage: Buffer<ArrayBufferLike>, format: string) {
        //TODO: check if upload was actually successful
        const upload = new Upload({
            client: this.s3,
            params: {
                Key: outputKey,
                Body: processedImage,
                Bucket: this.s3Bucket,
                ContentType: `image/${format}`
            }
        });

        await upload.done();
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
                    console.error("Unexpected error during cache operation:", error);
                    //throw error;
                }
            },
            { timeout: this.queueTimeoutMs }
        );
    };

    async downloadImageFromBucket(imageKey: string, ignoreCached: boolean = false): Promise<Readable | Buffer<ArrayBufferLike>> {
        let cachedImageBuffer: Buffer<ArrayBufferLike> | undefined = this.cache.get(imageKey);
        let readStream: Readable | undefined;

        if (!cachedImageBuffer || ignoreCached) {
            const getCommand = new GetObjectCommand({ Bucket: this.s3Bucket, Key: imageKey });

            try {
                const { Body } = await this.s3.send(getCommand);
                readStream = Body as Readable | undefined;
            } catch (error: unknown) {
                if (error instanceof NoSuchKey) {
                    throw new TaskError('No such image is present in the S3 bucket', error, 404);
                } else if (error instanceof AccessDenied) {
                    throw new TaskError('Permission denied while trying to access S3 object', error, 403);
                } else if (error instanceof InvalidObjectState) {
                    throw new TaskError('S3 object is in an invalid state and cannot be retrieved', error, 409);
                } else if (error instanceof S3ServiceException) {
                    throw new TaskError('Unexpected error while retrieving image from S3', error, 500);
                } else {
                    throw new TaskError('Unknown error while retrieving image from S3', error, 500);
                }
            }

            if (!readStream) {
                throw new TaskError('Failed to retrieve image stream from S3 bucket', 500);
            }

            return readStream as Readable;
        }

        return cachedImageBuffer;
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
