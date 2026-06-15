export class ProgrammingError extends Error {
    cause?: unknown;

    constructor(message: string, cause?: unknown) {
        super(message);
        this.name = "ProgrammingError";
        this.cause = cause;
    };
};

export class TaskError extends Error {
    cause?: unknown;
    code?: number;

    constructor(message: string, cause?: unknown, code?: number) {
        super(message);
        this.name = "TaskError";
        this.cause = cause;
        this.code = code;
    };
};
