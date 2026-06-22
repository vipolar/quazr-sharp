import { ProgrammingError, TaskError } from './errors.js';
import { ImageBucket } from './bucket.js';
import type { Sharp } from "sharp";
import sharp from "sharp";

interface FormattingOptions {
	format?: string;
	tune?: string;
	colors?: number;
	quality?: number;
	bitdepth?: number;
	lossless?: boolean;
	progressive?: boolean;
	nearLossless?: boolean;
	compressionLevel?: number;
	chromaSubsampling?: string;
	adaptiveFiltering?: boolean;
	smartSubsample?: boolean;
	smartDeblock?: boolean;
	alphaQuality?: number;
	palette?: boolean;
	dither? : number;
	preset?: string;
	exact?: boolean;
	effort?: number;
	force?: boolean;
};

interface TaskOptions {
	ignoreCached?: boolean;
	doNotCache?: boolean;
};

class ImageTaskTemplate {
	private readonly webpProfileRange = ["default", "photo", "picture", "drawing", "icon", "text"];
	private readonly webpCPUEffort = [0, 1, 2, 3, 4, 5, 6];

	private readonly pngCompressionLevelRange = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
	private readonly pngCPUEffort = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

	private readonly avifQualityTuneRange = ["auto", "iq", "psnr", "ssim"];
	private readonly avifCPUEffort = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
	private readonly avifBitdepthRange = [8, 10, 12];

	private readonly formattingOptions: FormattingOptions;
	protected processorInstance: Sharp | undefined;
	private readonly options: TaskOptions;
	private readonly bucket: ImageBucket;
	private readonly sourceFile: string;
	private readonly outputFile: string;
	private readonly imageUUID: string;
	private readonly task: string;

	constructor(message: any, bucket: ImageBucket) {
		this.formattingOptions = message.format;
		this.outputFile = message.outputFile;
		this.sourceFile = message.sourceFile;
		this.imageUUID = message.imageUUID;
		this.options = message.options;
		this.task = message.task;

		this.bucket = bucket;
	};

	protected applyImageProcessorParameters() {
		this.processorInstance = this.processorInstance!;
	};

	protected applyFormattingToImageProcessor() {
		let {
			format, tune, colors, quality, bitdepth, lossless, progressive, nearLossless,compressionLevel, chromaSubsampling, adaptiveFiltering, smartSubsample, smartDeblock, alphaQuality, palette, dither, preset, exact, effort, force
		} = this.formattingOptions;
		let options: any;

		switch (format) {
			case 'png':
				options = {
					compressionLevel: compressionLevel != null && this.pngCompressionLevelRange.includes(compressionLevel) ? compressionLevel : undefined,
					adaptiveFiltering: adaptiveFiltering === true || adaptiveFiltering === false ? adaptiveFiltering : undefined,
					progressive: progressive === true || progressive === false ? progressive : undefined,
					effort: effort != null && this.pngCPUEffort.includes(effort) ? effort : undefined,
					quality: quality != null && quality > 0 && quality <= 100 ? quality : undefined,
					dither: dither != null && dither >= 0 && dither <= 9.99 ? dither : undefined,
					colors: colors != null && colors > 0 && colors <= 256 ? colors : undefined,
					palette: palette === true || palette === false ? palette : undefined,
					force: force === true || force === false ? force : undefined
				};

				this.processorInstance = this.processorInstance!.toFormat(format, options);
				break;
			case 'webp':
				options = {
					alphaQuality: alphaQuality != null && alphaQuality >= 0 && alphaQuality <= 100 ? alphaQuality : undefined,
					smartSubsample: smartSubsample === true || smartSubsample === false ? smartSubsample : undefined,
					smartDeblock: smartDeblock === true || smartDeblock === false ? smartDeblock : undefined,
					nearLossless: nearLossless === true || nearLossless === false ? nearLossless : undefined,
					preset: preset != null && this.webpProfileRange.includes(preset) ? preset : undefined,
					effort: effort != null && this.webpCPUEffort.includes(effort) ? effort : undefined,
					quality: quality != null && quality > 0 && quality <= 100 ? quality : undefined,
					lossless: lossless === true || lossless === false ? lossless : undefined,
					exact: exact === true || exact === false ? exact : undefined,
					force: force === true || force === false ? force : undefined
				};

				this.processorInstance = this.processorInstance!.toFormat(format, options);
				break;
			case 'avif':
				options = {
					chromaSubsampling: chromaSubsampling != null && (chromaSubsampling === "4:4:4" || chromaSubsampling === "4:2:0") ? chromaSubsampling : undefined,
					bitdepth: bitdepth != null && this.avifBitdepthRange.includes(bitdepth) ? bitdepth : undefined,
					effort: effort != null && this.avifCPUEffort.includes(effort) ? effort : undefined,
					tune: tune != null && this.avifQualityTuneRange.includes(tune) ? tune : undefined,
					quality: quality != null && quality > 0 && quality <= 100 ? quality : undefined,
					lossless: lossless === true || lossless === false ? lossless : undefined,
					force: force === true || force === false ? force : undefined
				};

				this.processorInstance = this.processorInstance!.toFormat(format, options);
				break;
			default:
				throw new ProgrammingError("Unexpected output format");
		}
	};

	public async processTask(): Promise<Object> {
		const outputKey = `${this.imageUUID}/${this.outputFile}`;
		const { ignoreCached, doNotCache }: TaskOptions = this.options ? this.options : {
			ignoreCached: false,
			doNotCache: false
		};

		let outputBuffer: Buffer<ArrayBufferLike> | undefined = !ignoreCached ? this.bucket.getImageBuffer(outputKey) : undefined;
		if (!outputBuffer) {
			const sourceKey = `${this.imageUUID}/${this.sourceFile}`;
			const sourceCachedBuffer = !ignoreCached ? this.bucket.getImageBuffer(sourceKey) : undefined;
			const sourceInputStream = !sourceCachedBuffer ? await this.bucket.downloadImageFromBucket(sourceKey) : undefined;

			if (sourceCachedBuffer) {
				try {
					this.processorInstance = sharp(sourceCachedBuffer);
				} catch (error: unknown) {
					throw new TaskError("Failed to initalize sharp instance for cached buffer", error, 21001);
				}
			} else {
				try {
					this.processorInstance = sharp();
				} catch (error: unknown) {
					throw new TaskError("Failed to initalize sharp instance for input stream", error, 21002);
				}
			}

			try {
				this.applyImageProcessorParameters();
			} catch (error: unknown) {
				const eCode = sourceCachedBuffer ? 22001 : 22002;
				throw new TaskError("Failed to append processing parameters to processor",  error,  eCode);
			}

			try {
				this.applyFormattingToImageProcessor();
			} catch (error: unknown) {
				const eCode = sourceCachedBuffer ? 23001 : 23002;
				throw new TaskError("Failed to append formatting parameters to processor", error, eCode);
			}

			if (sourceCachedBuffer) {
				try {
					outputBuffer = await this.processorInstance.toBuffer();
				} catch (error: unknown) {
					throw new TaskError("Failed to process buffered image and buffer it", error, 24001);
				}
			} else {
				try {
					outputBuffer = await sourceInputStream!.pipe(this.processorInstance).toBuffer();
				} catch (error: unknown) {
					throw new TaskError("Failed to process input stream and buffer it", error, 24002);
				}
			}

			if (!outputBuffer || !Buffer.isBuffer(outputBuffer) || outputBuffer.byteLength <= 0) {
				throw new TaskError("Unknown error while processing the image", 25500);
			}

			if (!doNotCache) {
				this.bucket.cacheImageBuffer(outputKey, outputBuffer);
			}
		}

		await this.bucket.uploadImageToBucket(outputKey, outputBuffer, this.formattingOptions.format!);

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
