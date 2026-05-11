import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import amqp from 'amqplib';
import sharp from 'sharp';

const RABBIT_ENDPOINT = `amqp://${process.env.BE_BROKER_USER}:${process.env.BE_BROKER_PASSWORD}@${process.env.BE_BROKER_HOST}:${process.env.BE_BROKER_PORT}`;
const RUSTFS_ENDPOINT = `http://${process.env.BE_BUCKET_HOST}:${process.env.BE_BUCKET_PORT}`;
const RUSTFS_BUCKET_NAME = process.env.BE_BUCKET_NAME || 'test';
const RUSTFS_PASSWORD = process.env.BE_BUCKET_PASS;
const RUSTFS_USER = process.env.BE_BUCKET_KEY;

const QUEUE_NAME = 'quazr-images-queue';

const s3 = new S3Client({
    endpoint: RUSTFS_ENDPOINT,
    region: 'us-east-1',
    credentials: {
        secretAccessKey: RUSTFS_PASSWORD,
        accessKeyId: RUSTFS_USER,
    },
    forcePathStyle: true
});

async function processImage(task) {
    console.log('Received task:', task);
    const { imageKey, width, height, quality, format} = task;
    const outputKey = `processed/${imageKey}.${format}`;

    const getCommand = new GetObjectCommand({ Bucket: RUSTFS_BUCKET_NAME, Key: imageKey });
    const { Body: readStream } = await s3.send(getCommand);

    const pipeline = sharp()
        .resize({ width: width, height: height, fit: 'cover' })
        .toFormat(format, { quality: quality });

    const upload = new Upload({
        client: s3,
        params: {
            Key: outputKey,
            Bucket: RUSTFS_BUCKET_NAME,
            Body: readStream.pipe(pipeline),
            ContentType: `image/${format}`
        }
    });

    await upload.done();
    console.log(`Processed and uploaded: ${outputKey}`);
}

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
                    await processImage(task);
                    
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
}

startWorker();