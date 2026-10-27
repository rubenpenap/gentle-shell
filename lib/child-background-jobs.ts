import { mkdtempSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { createLocalBashOperations, SettingsManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createJobRegistry, type JobExecOperations } from "./background-jobs.ts";
import { registerBackgroundJobTools } from "./background-jobs-tools.ts";
import { CHILD_JOB_TAIL_MAX_BYTES, type ChildMessenger, type IpcEndpoint } from "./agents-messaging.ts";
import { REQUESTED_TOOLS_ENV } from "./agents-runner.ts";

// Bound by UTF-8 bytes without cutting a code point (including lone surrogates).
function outputTail(value: string): string {
	const points = Array.from(value.toWellFormed());
	let size = 0;
	let start = points.length;
	while (start > 0) {
		const bytes = Buffer.byteLength(points[start - 1]);
		if (size + bytes > CHILD_JOB_TAIL_MAX_BYTES) break;
		size += bytes;
		start--;
	}
	return points.slice(start).join("");
}

export function registerChildBackgroundJobs(pi: ExtensionAPI, options: {
	env: NodeJS.ProcessEnv;
	ipc: IpcEndpoint;
	messenger: ChildMessenger;
	/** Resolve only on execution, never on the unchanged capability-off path. */
	agentHome(): string;
	shell?: (cwd: string) => { operations: JobExecOperations; commandPrefix?: string };
}): void {
	const { env, ipc, messenger } = options;
	const requested = (env[REQUESTED_TOOLS_ENV] ?? "").split(",").map(name => name.trim());
	if (env.GENTLE_PI_BACKGROUND_JOBS !== "1" || !requested.includes("bash_background") || ipc.connected === false) return;
	// Never fall back to the parent's shared output directory or system TMPDIR.
	if (!env.TMPDIR || !isAbsolute(env.TMPDIR)) return;
	let outputDir: string;
	try { outputDir = mkdtempSync(join(env.TMPDIR, "jobs-")); }
	catch { return; }
	let closed = false;
	let context: ExtensionContext | undefined;
	const report = (error: unknown) => {
		const message = `Child background completion failed: ${error instanceof Error ? error.message : String(error)}`;
		try {
			if (context) context.ui.notify(message, "warning");
			else console.error(message);
		} catch { console.error(message); }
	};
	const registry = createJobRegistry({
		outputDir: () => outputDir,
		now: Date.now,
		shell: cwd => {
			if (closed || ipc.connected === false) throw new Error("Child background job channel closed");
			const shell = options.shell?.(cwd) ?? (() => {
				const settings = SettingsManager.create(cwd, options.agentHome());
				return {
					operations: createLocalBashOperations({ shellPath: settings.getShellPath() }),
					commandPrefix: settings.getShellCommandPrefix(),
				};
			})();
			// Also contain a shell adapter that throws synchronously during admission.
			return { ...shell, operations: { exec: async (...args) => shell.operations.exec(...args) } };
		},
		onSettled: job => {
			void messenger.job({
				phase: "settled",
				jobId: job.id,
				exitCode: job.exitCode ?? null,
				outputTail: outputTail([...job.tail, ...(job.error ? [`Error: ${job.error}`] : [])].join("\n")),
			}).catch(report);
		},
	});
	const shutdown = () => {
		closed = true;
		return registry.stopAll();
	};
	ipc.on("disconnect", () => { void shutdown().catch(report); });
	pi.on("session_shutdown", shutdown);
	registerBackgroundJobTools(pi, {
		registry,
		now: Date.now,
		childOnly: true,
		sessionId(ctx) {
			context = ctx;
			return ctx.sessionManager.getSessionId();
		},
	});
	pi.on("session_start", (_event, ctx) => {
		context = ctx;
		if (closed || ipc.connected === false) return;
		pi.setActiveTools([...new Set([...pi.getActiveTools(), "bash_background", "job_list", "job_stop"])]);
	});
}
