import type { DriverStatement, RawResultData } from '../types.ts';

export interface WorkerMessageBase {
	id: string;
}

export interface InitMessage extends WorkerMessageBase {
	type: 'init';
	payload: InitPayload;
}

export interface ExecMessage extends WorkerMessageBase {
	type: 'exec';
	payload: ExecPayload;
}

export interface ExecBatchMessage extends WorkerMessageBase {
	type: 'execBatch';
	payload: ExecBatchPayload;
}

export interface ExportMessage extends WorkerMessageBase {
	type: 'export';
	payload: undefined;
}

export interface ImportMessage extends WorkerMessageBase {
	type: 'import';
	payload: ImportPayload;
}

export interface DestroyMessage extends WorkerMessageBase {
	type: 'destroy';
	payload: undefined;
}

export interface TransactionMessage extends WorkerMessageBase {
	type: 'transaction';
	payload: TransactionPayload;
}

export type WorkerMessage =
	| InitMessage
	| ExecMessage
	| ExecBatchMessage
	| ExportMessage
	| ImportMessage
	| DestroyMessage
	| TransactionMessage;

export interface WorkerResponseBase {
	id: string;
	success: boolean;
	error?: string;
}

export interface WorkerSuccessResponse<T> extends WorkerResponseBase {
	success: true;
	result: T;
}

export interface WorkerErrorResponse extends WorkerResponseBase {
	success: false;
	error: string;
}

export type WorkerResponse<T = unknown> = WorkerSuccessResponse<T> | WorkerErrorResponse;

export type WorkerMessageType =
	| 'init'
	| 'exec'
	| 'execBatch'
	| 'export'
	| 'import'
	| 'destroy'
	| 'transaction';

export interface InitPayload {
	databasePath: string;
}

export interface ExecPayload extends DriverStatement {}

export interface ExecBatchPayload extends Array<DriverStatement> {}

export interface TransactionPayload extends Array<DriverStatement> {}

export interface ImportPayload {
	data: ArrayBuffer;
}

export interface ExportResult {
	name: string;
	data: ArrayBuffer;
}

export type InitResponse = WorkerResponse<void>;
export type ExecResponse = WorkerResponse<RawResultData>;
export type ExecBatchResponse = WorkerResponse<RawResultData[]>;
export type TransactionResponse = WorkerResponse<RawResultData[]>;
export type ExportResponse = WorkerResponse<ExportResult>;
export type ImportResponse = WorkerResponse<void>;
export type DestroyResponse = WorkerResponse<void>;
