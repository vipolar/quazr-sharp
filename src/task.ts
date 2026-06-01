export interface ImageFormattingTask {
	imageKey: string;
	scaledWidth: number;
	scaledHeight: number;
	quality: number;
	format: 'jpeg' | 'png' | 'webp' | 'avif';
}

export interface ImageProcessingTask extends ImageFormattingTask {
	angleInRadians: number;
	extractLeft: number;
	extractTop: number;
	extractWidth: number;
	extractHeight: number;
}