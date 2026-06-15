import { ProgrammingError, TaskError } from './errors.js';
import { ImageBucket } from './bucket.js';
import type { Sharp } from "sharp";
import { Readable } from "stream";
import sharp from "sharp";

class ImageTaskTemplate {
	protected readonly format: string;
	protected readonly imageUUID: string;
	protected readonly sourceFile: string;
	protected readonly bucket: ImageBucket;
	protected readonly quality: number;
	protected readonly task: string;

	protected processorInstance: Sharp | undefined;
	protected sourceIsStream: boolean = false;
	protected readonly outputFile: string;

	constructor(message: any, bucket: ImageBucket) {
		this.outputFile = message.outputFile;
		this.sourceFile = message.sourceFile;
		this.imageUUID = message.imageUUID;
		this.quality = message.quality;
		this.format = message.format;
		this.task = message.task;

		this.bucket = bucket;
	};

	protected applyImageProcessorParameters() {
		this.processorInstance = this.processorInstance!;
	};

	protected applyFormattingToImageProcessor() {
		switch (this.format) {
			case 'png':
				this.processorInstance =
					this.processorInstance!.toFormat(this.format, { quality: this.quality });
				break;
			case 'webp':
				this.processorInstance =
					this.processorInstance!.toFormat(this.format, { quality: this.quality });
				break;
			case 'avif':
				this.processorInstance =
					this.processorInstance!.toFormat(this.format, { quality: this.quality });
				break;
			default:
				throw new ProgrammingError("Unexpected target format to process the image to");
		}
	};

	public async processTask(): Promise<Object> {
		const outputKey = `${this.imageUUID}/${this.outputFile}`;
		let outputBuffer = this.bucket.getImageBuffer(outputKey);

		if (!outputBuffer || !Buffer.isBuffer(outputBuffer)) {
			const sourceKey = `${this.imageUUID}/${this.sourceFile}`;
			const source = await this.bucket.downloadImageFromBucket(sourceKey);

			if (Buffer.isBuffer(source)) {
				try {
					this.processorInstance = sharp(source);
					this.sourceIsStream = false;
				} catch (error: unknown) {
					throw new TaskError("Failed to initalize sharp instance for buffered image", error, 10001);
				}
			} else if (source instanceof Readable) {
				try {
					this.processorInstance = sharp();
					this.sourceIsStream = true;
				} catch (error: unknown) {
					throw new TaskError("Failed to initalize sharp instance for input stream", error, 10002);
				}
			} else {
				throw new ProgrammingError("Invalid source image format to initialize sharp instance from");
			}

			try {
				this.applyImageProcessorParameters();
			} catch (error: unknown) {
				throw new TaskError("Failed to append processing parameters to processor", error, 20001);
			}

			try {
				this.applyFormattingToImageProcessor();
			} catch (error: unknown) {
				throw new TaskError("Failed to append formatting parameters to processor", error, 30001);
			}

			if (this.sourceIsStream) {
				try {
					outputBuffer = await (source as Readable).pipe(this.processorInstance).toBuffer();
				} catch (error: unknown) {
					throw new TaskError("Failed to process buffered image", error, 40001);
				}
			} else {
				try {
					outputBuffer = await this.processorInstance.toBuffer();
				} catch (error: unknown) {
					throw new TaskError("Failed to process input stream", error, 40002);
				}
			}

			if (!outputBuffer || !Buffer.isBuffer(outputBuffer) || outputBuffer.byteLength <= 0) {
				throw new TaskError("Unknown error while processing the image", 40003);
			}

			this.bucket.cacheImageBuffer(outputKey, outputBuffer);
		}

		await this.bucket.uploadImageToBucket(outputKey, outputBuffer, this.format);

		return {
			task: this.task, imageUUID: this.imageUUID, outputFile: this.outputFile
		}
	};

	public generateFailureResponse(taskError: TaskError) {
		return {
			failureCode: taskError.code,
			failureCause: taskError.cause,
			failureMessage: taskError.message,
			task: this.task, imageUUID: this.imageUUID, outputFile: this.outputFile
		}
	};
};

export class ImageVerificationTask extends ImageTaskTemplate {
	constructor(message: any, bucket: ImageBucket) {
		super(message, bucket);
	};

	protected override applyImageProcessorParameters() {
		this.processorInstance =
			this.processorInstance!;
	};
};

export class ImageFormattingTask extends ImageVerificationTask {
	constructor(message: any, bucket: ImageBucket) {
		super(message, bucket);
	};

	protected override applyImageProcessorParameters() {
		this.processorInstance =
			this.processorInstance!;
	};
};

export class ImageDownscalingTask extends ImageFormattingTask {
	protected readonly scaledHeight: number;
	protected readonly scaledWidth: number;

	constructor(message: any, bucket: ImageBucket) {
		super(message, bucket);

		this.scaledHeight = message.scaledHeight;
		this.scaledWidth = message.scaledWidth;
	};

	protected override applyImageProcessorParameters() {
		this.processorInstance =
			this.processorInstance!.resize({ width: this.scaledWidth, height: this.scaledHeight, fit: 'inside', withoutEnlargement: true });
	};
};

export class ImageProcessingTask extends ImageDownscalingTask {
	protected readonly angleInRadians: number;
	protected readonly extractHeight: number;
	protected readonly extractWidth: number;
	protected readonly extractLeft: number;
	protected readonly extractTop: number;

	constructor(message: any, bucket: ImageBucket) {
		super(message, bucket);

		this.angleInRadians = message.angleInRadians;
		this.extractHeight = message.extractHeight;
		this.extractWidth = message.extractWidth;
		this.extractLeft = message.extractLeft;
		this.extractTop = message.extractTop;
	};

	protected override applyImageProcessorParameters() {
		const rotationInDegrees = (this.angleInRadians * (180 / Math.PI));
		const angleInRadiansCosine = Math.abs(Math.cos(this.angleInRadians));
		const angleInRadiansSine = Math.abs(Math.sin(this.angleInRadians));

		const expandedCanvasWidth = (this.scaledWidth * angleInRadiansCosine) + (this.scaledHeight * angleInRadiansSine);
		const expandedCanvasHeight = (this.scaledWidth * angleInRadiansSine) + (this.scaledHeight * angleInRadiansCosine);
		const adjustedExtractLeft = Math.round((expandedCanvasWidth / 2) - this.extractLeft - (this.extractWidth / 2));
		const adjustedExtractTop = Math.round((expandedCanvasHeight / 2) - this.extractTop - (this.extractHeight / 2));

		this.processorInstance =
			this.processorInstance!.resize({ width: this.scaledWidth, height: this.scaledHeight, fit: 'inside', withoutEnlargement: true })
				.rotate(rotationInDegrees)
				.extract({ left: adjustedExtractLeft, top: adjustedExtractTop, width: this.extractWidth, height: this.extractHeight });
	};
};
