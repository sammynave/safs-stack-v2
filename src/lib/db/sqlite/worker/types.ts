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

export interface EmitExecMessage extends WorkerMessageBase {
	type: 'emitExec';
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

export interface GetChangesSinceMessage extends WorkerMessageBase {
	type: 'getChangesSince';
	payload: GetChangesSincePayload;
}

export interface MergeChangesMessage extends WorkerMessageBase {
	type: 'mergeChanges';
	payload: MergeChangesPayload;
}

export type WorkerMessage =
	| InitMessage
	| ExecMessage
	| EmitExecMessage
	| ExecBatchMessage
	| ExportMessage
	| ImportMessage
	| DestroyMessage
	| TransactionMessage
	| GetChangesSinceMessage
	| MergeChangesMessage;

export interface WorkerResponseBase {
	id: string;
	success: boolean;
	error?: string;
	type?: WorkerMessageType;
}

export interface WorkerSuccessResponse<T> extends WorkerResponseBase {
	success: true;
	result: T;
	type?: WorkerMessageType;
}

export interface WorkerErrorResponse extends WorkerResponseBase {
	success: false;
	error: string;
	type?: WorkerMessageType;
}

export type WorkerResponse<T = unknown> = WorkerSuccessResponse<T> | WorkerErrorResponse;

export type WorkerMessageType =
	| 'init'
	| 'exec'
	| 'emit'
	| 'emitExec'
	| 'execBatch'
	| 'export'
	| 'import'
	| 'destroy'
	| 'transaction'
	| 'getChangesSince'
	| 'mergeChanges';

export interface InitPayload {
	databasePath: string;
}

export interface ExecPayload extends DriverStatement {}

export interface ExecBatchPayload extends Array<DriverStatement> {}

export interface TransactionPayload extends Array<DriverStatement> {}

export interface ImportPayload {
	data: ArrayBuffer;
}

export interface GetChangesSincePayload {
	since: number;
}

export interface MergeChangesPayload {
	changes: Array<[string, string, string, unknown, number, number, string, number, number]>;
	affectedTables: string[];
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
