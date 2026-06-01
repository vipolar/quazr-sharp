import type { ImageProcessingTask, ImageFormattingTask } from './task';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import type { Readable } from 'stream';
import amqp from 'amqplib';
import sharp from 'sharp';

const RABBIT_ENDPOINT = `amqp://${process.env['BE_BROKER_USER']}:${process.env['BE_BROKER_PASSWORD']}@${process.env['BE_BROKER_HOST']}:${process.env['BE_BROKER_PORT']}`;
const RUSTFS_ENDPOINT = `http://${process.env['BE_BUCKET_HOST']}:${process.env['BE_BUCKET_PORT']}`;
const SHARP_CACHE_SIZE = parseInt(process.env['SHARP_CACHE_SIZE'] || '100', 10);
const RUSTFS_BUCKET_NAME = process.env['BE_BUCKET_NAME'] || 'test';
const RUSTFS_PASSWORD = process.env['BE_BUCKET_PASS'] || '';
const RUSTFS_USER = process.env['BE_BUCKET_KEY'] || '';

const QUEUE_NAME = 'quazr-images-queue';

const processedImageCache = new Map<string, Buffer>();
const s3 = new S3Client({
    endpoint: RUSTFS_ENDPOINT,
    region: 'us-east-1',
    credentials: {
        secretAccessKey: RUSTFS_PASSWORD,
        accessKeyId: RUSTFS_USER,
    },
    forcePathStyle: true
});

function cacheImage(imageKey: string, imageBuffer: Buffer) {
    processedImageCache.set(imageKey, imageBuffer);

    if (processedImageCache.size > SHARP_CACHE_SIZE) {
        const oldestKey = processedImageCache.keys().next().value;
        if (oldestKey) {
            processedImageCache.delete(oldestKey);
        }
    }
};

async function processImage(task: ImageProcessingTask) {
    const { imageKey, scaledWidth, scaledHeight, angleInRadians, extractLeft, extractTop, extractWidth, extractHeight, quality, format } = task;
    const getCommand = new GetObjectCommand({ Bucket: RUSTFS_BUCKET_NAME, Key: imageKey });
    const { Body: readStream } = await s3.send(getCommand);

    if (!readStream) {
        throw new Error('Failed to retrieve image stream from S3 bucket');
    }

    const rotationInDegrees = (angleInRadians * (180 / Math.PI));
    const angleInRadiansCosine = Math.abs(Math.cos(angleInRadians));
    const angleInRadiansSine = Math.abs(Math.sin(angleInRadians));

    const expandedCanvasWidth = (scaledWidth * angleInRadiansCosine) + (scaledHeight * angleInRadiansSine);
    const expandedCanvasHeight = (scaledWidth * angleInRadiansSine) + (scaledHeight * angleInRadiansCosine);
    const adjustedExtractLeft = Math.round((expandedCanvasWidth / 2) - extractLeft - (extractWidth / 2));
    const adjustedExtractTop = Math.round((expandedCanvasHeight / 2) - extractTop - (extractHeight / 2));

    const pipeline = sharp()
        .resize({ width: scaledWidth, height: scaledHeight, fit: 'inside', withoutEnlargement: true })
        .rotate(rotationInDegrees)
        .extract({ left: adjustedExtractLeft, top: adjustedExtractTop, width: extractWidth, height: extractHeight })
        .toFormat(format, { quality: quality });

    const processedImage = await (readStream as Readable).pipe(pipeline).toBuffer();

    if (!processedImage) {
        throw new Error('Failed to process the image and write it to buffer');
    }

    const outputKey = `${imageKey}_processed.${format}`;
    cacheImage(imageKey, processedImage);
    const upload = new Upload({
        client: s3,
        params: {
            Key: outputKey,
            Body: processedImage,
            Bucket: RUSTFS_BUCKET_NAME,
            ContentType: `image/${format}`
        }
    });

    await upload.done();
};

async function formatImage(task: ImageFormattingTask) {
    const { imageKey, scaledWidth, scaledHeight, quality, format } = task;
    let imageBuffer: Buffer<ArrayBufferLike> | undefined = processedImageCache.get(imageKey);

    if (!imageBuffer) {
        const getCommand = new GetObjectCommand({ Bucket: RUSTFS_BUCKET_NAME, Key: imageKey });
        const { Body: readStream } = await s3.send(getCommand);

        if (!readStream) {
            throw new Error('Failed to retrieve image stream from S3 bucket');
        }

        const byteArray = await readStream.transformToByteArray();
        if (!byteArray || byteArray.length === 0) {
            throw new Error('Failed to read image data from S3 stream');
        }

        imageBuffer = Buffer.from(byteArray);
        cacheImage(imageKey, imageBuffer);
    }

    const formattedImage = await sharp(imageBuffer)
        .resize({ width: scaledWidth, height: scaledHeight, fit: 'inside', withoutEnlargement: true })
        .toFormat(format, { quality })
        .toBuffer();

    if (!formattedImage) {
        throw new Error('Failed to format the image and write it to buffer');
    }

    const outputKey = `${imageKey}_processed_w${scaledWidth}_h${scaledHeight}_q${quality}.${format}`;
    const upload = new Upload({
        client: s3,
        params: {
            Key: outputKey,
            Body: formattedImage,
            Bucket: RUSTFS_BUCKET_NAME,
            ContentType: `image/${format}`
        }
    });

    await upload.done();
};

async function startWorker() {
    try {
        const conn = await amqp.connect(RABBIT_ENDPOINT);
        const channel = await conn.createChannel();
        
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        await channel.prefetch(5);

        console.log(`Worker listening on ${QUEUE_NAME}...`);

        channel.consume(QUEUE_NAME, async (msg) => {
            if (msg !== null) {
                try {
                    const task = JSON.parse(msg.content.toString());

                    if (task && typeof task === 'object') {
                        if ('angleInRadians' in task) {
                            await processImage(task as ImageProcessingTask);
                        } else if ('format' in task) {
                            await formatImage(task as ImageFormattingTask);
                        } else {
                            throw new Error('Invalid image task format');
                        }
                    } else {
                        throw new Error('Received invalid task data');
                    }
                    
                    channel.ack(msg);
                } catch (error) {
                    console.error('Processing failed:', error);
                    channel.nack(msg, false, false); 
                }
            }
        });
    } catch (error) {
        console.error('Failed to start worker:', error);
        process.exit(1);
    }
};

startWorker();