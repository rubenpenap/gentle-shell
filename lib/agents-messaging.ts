export const CHILD_MESSAGE_MAX_BYTES = 8 * 1024;
export const CHILD_ACK_ERROR_MAX_BYTES = 256;
export const CHILD_QUERY_ERROR_MAX_BYTES = 256;
export const CHILD_MESSAGE_MAX_INFLIGHT = 8;
export const CHILD_MESSAGE_TIMEOUT_MS = 2_000;
export const CHILD_QUERY_MAX_INFLIGHT = 4;
export const CHILD_QUERY_TIMEOUT_MS = 30_000;

export interface NotificationFrame {
	id: string;
	kind: "notification";
	message: string;
}

export interface QueryFrame {
	id: string;
	kind: "query";
	message: string;
}

export const CHILD_JOB_MAX_IDS = 64;
export const CHILD_JOB_TAIL_MAX_BYTES = 4096;
export type JobPayload =
	| { phase: "pending"; jobIds: string[] }
	| { phase: "settled"; jobId: string; exitCode: number | null; outputTail: string }
	| { phase: "drained" };
export type JobFrame = { id: string; kind: "job" } & JobPayload;
export type ChildFrame = NotificationFrame | QueryFrame | JobFrame;

interface AckFrame {
	id: string;
	accepted: boolean;
	error?: string;
}

type ReplyFrame = { id: string; message: string } | { id: string; error: string };

export interface IpcEndpoint {
	connected?: boolean;
	send(message: unknown, callback?: (error: Error | null) => void): boolean;
	on(event: "message" | "disconnect", listener: (...args: unknown[]) => void): unknown;
}

type Cancel = () => void;
type Schedule = (fn: () => void, ms: number) => Cancel;
type Pending<T> = { resolve: (value: T) => void; reject: (error: Error) => void; cancel: Cancel };

function record(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function exact(value: Record<string, unknown>, keys: readonly string[]): boolean {
	return Object.keys(value).every((key) => keys.includes(key));
}

function wellFormedUnicode(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xD800 && code <= 0xDBFF) {
			const next = value.charCodeAt(index + 1);
			if (index + 1 >= value.length || next < 0xDC00 || next > 0xDFFF) return false;
			index += 1;
		} else if (code >= 0xDC00 && code <= 0xDFFF) return false;
	}
	return true;
}

function boundedText(value: unknown, maxBytes: number): value is string {
	return typeof value === "string" && wellFormedUnicode(value) && Buffer.byteLength(value, "utf8") <= maxBytes;
}

function validCorrelation(value: unknown, prefix: "n" | "q"): value is string {
	if (typeof value !== "string" || !new RegExp(`^${prefix}[1-9]\\d{0,15}$`).test(value)) return false;
	const counter = Number(value.slice(1));
	return Number.isSafeInteger(counter) && counter > 0;
}

export function validChildId(value: unknown): value is string {
	return validCorrelation(value, "n");
}

export function validChildQueryId(value: unknown): value is string {
	return validCorrelation(value, "q");
}

export function validChildMessage(value: unknown): value is string {
	return boundedText(value, CHILD_MESSAGE_MAX_BYTES);
}

function validJobId(value: unknown): value is string {
	if (typeof value !== "string" || !/^job-[1-9]\d{0,15}$/.test(value)) return false;
	return Number.isSafeInteger(Number(value.slice(4)));
}

function parseJob(value: Record<string, unknown>, id: string): JobFrame | undefined {
	if (!validChildId(id)) return undefined;
	if (value.phase === "drained" && exact(value, ["id", "kind", "phase"])) return { id, kind: "job", phase: "drained" };
	if (value.phase === "pending" && exact(value, ["id", "kind", "phase", "jobIds"]) && Array.isArray(value.jobIds)
		&& value.jobIds.length <= CHILD_JOB_MAX_IDS && value.jobIds.every(validJobId) && new Set(value.jobIds).size === value.jobIds.length) {
		return { id, kind: "job", phase: "pending", jobIds: [...value.jobIds] };
	}
	// Windows native process status is an unsigned DWORD; POSIX statuses fit too.
	if (value.phase === "settled" && exact(value, ["id", "kind", "phase", "jobId", "exitCode", "outputTail"])
		&& validJobId(value.jobId) && (value.exitCode === null || typeof value.exitCode === "number" && Number.isInteger(value.exitCode) && value.exitCode >= 0 && value.exitCode <= 0xFFFFFFFF)
		&& boundedText(value.outputTail, CHILD_JOB_TAIL_MAX_BYTES)) {
		return { id, kind: "job", phase: "settled", jobId: value.jobId, exitCode: typeof value.exitCode === "number" ? value.exitCode : null, outputTail: value.outputTail };
	}
	return undefined;
}

export function parseChildFrame(value: unknown): { frame?: ChildFrame; id?: string; error?: string } {
	if (!record(value)) return { error: "invalid child IPC frame" };
	const id = validChildQueryId(value.id) || validChildId(value.id) ? value.id : undefined;
	if (value.kind === "job") {
		const frame = id === undefined ? undefined : parseJob(value, id);
		return frame ? { frame } : { ...(id === undefined ? {} : { id }), error: "invalid child IPC frame" };
	}
	if (!exact(value, ["id", "kind", "message"])) return { ...(id === undefined ? {} : { id }), error: "invalid child IPC frame" };
	if (id === undefined) return { error: "invalid child IPC correlation" };
	if (value.kind !== "notification" && value.kind !== "query") return { id, error: "unsupported child IPC kind" };
	if (value.kind === "notification" && !validChildId(id) || value.kind === "query" && !validChildQueryId(id)) return { id, error: "invalid child IPC correlation" };
	if (!validChildMessage(value.message)) return { id, error: "invalid child IPC message" };
	return { frame: { id, kind: value.kind, message: value.message } } as { frame: ChildFrame };
}

function parseAck(value: unknown): AckFrame | undefined {
	if (!record(value) || !exact(value, ["id", "kind", "accepted", "error"]) || !validChildId(value.id) || value.kind !== "ack" || typeof value.accepted !== "boolean") return undefined;
	if (value.accepted && value.error !== undefined) return undefined;
	if (value.error !== undefined && !boundedText(value.error, CHILD_ACK_ERROR_MAX_BYTES)) return undefined;
	return { id: value.id, accepted: value.accepted, ...(typeof value.error === "string" ? { error: value.error } : {}) };
}

function parseReply(value: unknown): ReplyFrame | undefined {
	if (!record(value) || !validChildQueryId(value.id) || value.kind !== "reply") return undefined;
	if (exact(value, ["id", "kind", "message"]) && validChildMessage(value.message)) return { id: value.id, message: value.message };
	if (exact(value, ["id", "kind", "error"]) && boundedText(value.error, CHILD_QUERY_ERROR_MAX_BYTES)) return { id: value.id, error: value.error };
	return undefined;
}

// Child-side admissions make acceptance explicit, but do not imply the parent
// model read the message or that any later delivery is guaranteed.
export class ChildMessenger {
	private readonly pending = new Map<string, Pending<void>>();
	private readonly queries = new Map<string, Pending<string>>();
	private readonly endpoint: IpcEndpoint;
	private readonly schedule: Schedule;
	private nextId = 0;
	private nextQueryId = 0;
	private closed = false;

	constructor(endpoint: IpcEndpoint, schedule: Schedule = (fn, ms) => {
		const timer = setTimeout(fn, ms);
		timer.unref?.();
		return () => clearTimeout(timer);
	}) {
		this.endpoint = endpoint;
		this.schedule = schedule;
		endpoint.on("message", (value) => this.receive(value));
		endpoint.on("disconnect", () => this.close("parent notification channel closed"));
	}

	notify(message: string): Promise<void> {
		if (!this.available()) return Promise.reject(new Error("parent notification channel closed"));
		if (!validChildMessage(message)) return Promise.reject(new Error("message is not well-formed UTF-16 or exceeds 8KiB"));
		if (this.pending.size >= CHILD_MESSAGE_MAX_INFLIGHT) return Promise.reject(new Error("too many pending parent-message admissions"));
		if (this.nextId >= Number.MAX_SAFE_INTEGER) return Promise.reject(new Error("parent-message correlation exhausted"));
		return this.send(this.pending, `n${++this.nextId}`, "notification", message, CHILD_MESSAGE_TIMEOUT_MS, "parent-message admission timed out");
	}

	// Reuse this messenger instance: jobs and notifications share ACK correlations.
	job(payload: JobPayload): Promise<void> {
		if (!this.available()) return Promise.reject(new Error("parent notification channel closed"));
		const frame = parseChildFrame({ ...payload, id: "n1", kind: "job" }).frame;
		if (!frame || frame.kind !== "job") return Promise.reject(new Error("invalid child IPC frame"));
		if (this.pending.size >= CHILD_MESSAGE_MAX_INFLIGHT) return Promise.reject(new Error("too many pending parent-message admissions"));
		if (this.nextId >= Number.MAX_SAFE_INTEGER) return Promise.reject(new Error("parent-message correlation exhausted"));
		return this.send(this.pending, `n${++this.nextId}`, "job", "", CHILD_MESSAGE_TIMEOUT_MS, "parent-message admission timed out", frame);
	}

	query(message: string): Promise<string> {
		if (!this.available()) return Promise.reject(new Error("parent notification channel closed"));
		if (!validChildMessage(message)) return Promise.reject(new Error("query is not well-formed UTF-16 or exceeds 8KiB"));
		if (this.queries.size >= CHILD_QUERY_MAX_INFLIGHT) return Promise.reject(new Error("too many pending parent queries"));
		if (this.nextQueryId >= Number.MAX_SAFE_INTEGER) return Promise.reject(new Error("parent query correlation exhausted"));
		return this.send(this.queries, `q${++this.nextQueryId}`, "query", message, CHILD_QUERY_TIMEOUT_MS, "parent query timed out");
	}

	close(reason = "parent notification channel closed"): void {
		if (this.closed) return;
		this.closed = true;
		for (const id of [...this.pending.keys()]) this.settle(this.pending, id, new Error(reason));
		for (const id of [...this.queries.keys()]) this.settle(this.queries, id, new Error(reason));
	}

	private available(): boolean {
		if (!this.closed && this.endpoint.connected !== false) return true;
		this.close("parent notification channel closed");
		return false;
	}

	private send<T>(pending: Map<string, Pending<T>>, id: string, kind: "notification" | "query" | "job", message: string, timeout: number, timeoutMessage: string, job?: JobFrame): Promise<T> {
		return new Promise((resolve, reject) => {
			const settle = (error?: Error, reply?: T) => this.settle(pending, id, error, reply);
			const cancel = this.schedule(() => settle(new Error(timeoutMessage)), timeout);
			pending.set(id, { resolve, reject, cancel });
			try { this.endpoint.send(job ? { ...job, id } : { id, kind, message }, (error) => { if (error) settle(error); }); }
			catch (error) { settle(error instanceof Error ? error : new Error(String(error))); }
		});
	}

	private receive(value: unknown): void {
		const ack = parseAck(value);
		if (ack) {
			this.settle(this.pending, ack.id, ack.accepted ? undefined : new Error(ack.error || "parent rejected notification"));
			return;
		}
		const reply = parseReply(value);
		if (reply) this.settle(this.queries, reply.id, "error" in reply ? new Error(reply.error || "parent rejected query") : undefined, "message" in reply ? reply.message : undefined);
	}

	private settle<T>(pending: Map<string, Pending<T>>, id: string, error?: Error, value?: T): void {
		const entry = pending.get(id);
		if (!entry) return;
		pending.delete(id);
		entry.cancel();
		if (error) entry.reject(error);
		else entry.resolve(value as T);
	}
}
