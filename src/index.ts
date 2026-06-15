import { ImageVerificationTask, ImageDownscalingTask, ImageFormattingTask, ImageProcessingTask } from './task.js';
import { ProgrammingError, TaskError } from './errors.js';
import { ImageBucket } from './bucket.js';
import amqp from 'amqplib';

class ImageProcessingWorker {
    private connection: amqp.ChannelModel | null = null;
    private channel: amqp.Channel | null = null;
    private readonly ampqEndpoint: string;

    private readonly imagesExchange: string;

    //private readonly imagesTasksRoutingKey: string;
    private readonly imagesTasksQueue: string;

    private readonly imagesTasksSuccessesRoutingKey: string;
    //private readonly imagesTasksSuccessesQueue: string;

    private readonly imagesTasksFailuresRoutingKey: string;
    //private readonly imagesTasksFailuresQueue: string;

    private readonly imagesDLQRoutingKey: string;
    private readonly imagesDLQExchange: string;
    //private readonly imagesDLQQueue: string;

    private readonly bucket: ImageBucket;
    
    public constructor() {
        const AMPQ_USER = process.env['BE_BROKER_USER'];
        const AMPQ_PASS = process.env['BE_BROKER_PASS'];
        const AMPQ_HOST = process.env['BE_BROKER_HOST'];
        const AMPQ_PORT = process.env['BE_BROKER_PORT'];

        if (!AMPQ_USER || !AMPQ_PASS || !AMPQ_HOST || !AMPQ_PORT) {
            throw new Error("Invalid AMPQ credentials or parameters");
        }

        this.ampqEndpoint = `amqp://${AMPQ_USER}:${AMPQ_PASS}@${AMPQ_HOST}:${AMPQ_PORT}`;

        this.imagesExchange = process.env['BE_IMAGES_EXCHANGE'] || 'quazr-images-exchange';

        //this.imagesTasksRoutingKey = process.env['BE_IMAGES_TASKS_ROUTING_KEY'] || 'quazr-images-tasks-routing-key';
        this.imagesTasksQueue = process.env['BE_IMAGES_TASKS_QUEUE'] || 'quazr-images-tasks-queue';

        this.imagesTasksSuccessesRoutingKey = process.env['BE_IMAGES_TASKS_SUCCESSES_ROUTING_KEY'] || 'quazr-images-tasks-successes-routing-key';
        //this.imagesTasksSuccessesQueue = process.env['BE_IMAGES_TASKS_SUCCESSES_QUEUE'] || 'quazr-images-tasks-successes-queue';

        this.imagesTasksFailuresRoutingKey = process.env['BE_IMAGES_TASKS_FAILURES_ROUTING_KEY'] || 'quazr-images-tasks-failures-routing-key'
        //this.imagesTasksFailuresQueue = process.env['BE_IMAGES_TASKS_FAILURES_QUEUE'] || 'quazr-images-tasks-failures-queue';

        this.imagesDLQRoutingKey = process.env['BE_IMAGES_DLQ_ROUTING_KEY'] || 'quazr-images-dlq-routing-key';
        this.imagesDLQExchange = process.env['BE_IMAGES_DLQ_EXCHANGE'] || 'quazr-images-dlq-exchange';
        //this.imagesDLQQueue = process.env['BE_IMAGES_DLQ_QUEUE'] || 'quazr-images-dlq-queue';

        this.bucket = new ImageBucket();
    }

    private publishResults(exchange: string, routingKey: string, payload: any) {
        const messageBuffer = Buffer.from(JSON.stringify(payload));
        const publishingOptions = {
            contentType: 'application/json',
            persistent: true
        };

        this.channel!.publish(exchange, routingKey, messageBuffer, publishingOptions);
    };

    private async processMessage(msg: amqp.ConsumeMessage): Promise<void> {
        let imageTask: ImageDownscalingTask | ImageProcessingTask | ImageFormattingTask | ImageVerificationTask | undefined;

        try {
            const parsedMessage = JSON.parse(msg.content.toString());

            if (!(parsedMessage && typeof parsedMessage === 'object' && 'task' in parsedMessage)) {
                throw new Error("Parsed message does not contain any valid task data");
            }

            switch (parsedMessage.task) {
                case 'ImageDownscalingTask':
                    imageTask = new ImageDownscalingTask(parsedMessage, this.bucket);
                    break;
                case 'ImageProcessingTask':
                    imageTask = new ImageProcessingTask(parsedMessage, this.bucket);
                    break;
                case 'ImageFormattingTask':
                    imageTask = new ImageFormattingTask(parsedMessage, this.bucket);
                    break;
                case 'ImageVerificationTask':
                    imageTask = new ImageVerificationTask(parsedMessage, this.bucket);
                    break;
                default:
                    throw new ProgrammingError("Received task is of unrecognized type");
            }

            const taskSuccess = await imageTask.processTask();

            this.publishResults(this.imagesExchange, this.imagesTasksSuccessesRoutingKey, taskSuccess);
            console.log(taskSuccess);
            this.channel!.ack(msg);
        } catch (error: unknown) {
            if (imageTask && error instanceof TaskError) {
                try {
                    const failure = imageTask.generateFailureResponse(error);
                    this.publishResults(this.imagesExchange, this.imagesTasksFailuresRoutingKey, failure);
                } catch (publishingError: unknown) {
                    error = publishingError;
                }
            } else {
                try {
                    this.publishResults(this.imagesDLQExchange, this.imagesDLQRoutingKey, error);
                } catch (publishingError: unknown) {
                    error = publishingError;
                }
            }

            try {
                const errorType = error instanceof Error ? error.name : "Error";
                console.error(`Unexpected ${errorType}`, error);
                this.channel!.nack(msg, false, false);
            } catch (error: unknown) {
                console.error("NackError", error);
            }
        }
    }

    async start(): Promise<void> {
        try {
            this.connection = await amqp.connect(this.ampqEndpoint);
            this.channel = await this.connection.createChannel();

            await this.channel.assertQueue(this.imagesTasksQueue, {
                durable: true,
                arguments: {
                    'x-dead-letter-exchange': this.imagesDLQExchange,
                    'x-dead-letter-routing-key': this.imagesDLQRoutingKey
                }
            });

            await this.channel.prefetch(5);
            this.channel.consume(this.imagesTasksQueue, (msg) => {
                if (msg) {
                    this.processMessage(msg);
                }
            });
        } catch (error) {
            console.error('Failed to start worker:', error);
            process.exit(1);
        }
    }

    async stop(): Promise<void> {
        try {
            if (this.channel) {
                await this.channel.close();
            }
            if (this.connection) {
                await this.connection.close();
            }
        } catch (error) {
            console.error('Error stopping worker:', error);
        }
    }
}

async function main() {
    const worker = new ImageProcessingWorker();
    await worker.start();

    process.on('SIGTERM', async () => {
        console.log('SIGTERM received, shutting down gracefully...');
        await worker.stop();
        process.exit(0);
    });

    process.on('SIGINT', async () => {
        console.log('SIGINT received, shutting down gracefully...');
        await worker.stop();
        process.exit(0);
    });
}

main().catch(error => {
    console.error('Fatal error: ', error);
    process.exit(1);
});
