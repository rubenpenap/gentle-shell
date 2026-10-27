import assert from "node:assert/strict";
import test from "node:test";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import gentleAgents, { type AgentsDeps } from "../extensions/gentle-agents.ts";
import { AgentRunner } from "../lib/agents-runner.ts";
import { TaskStore } from "../lib/agents-protocol.ts";
import { AGENT_MODE } from "../lib/agents-config.ts";
import { fakeChild } from "./agents-fake-child.ts";

function fixture(options: {
	flag?: string;
	requested?: string;
	connected?: boolean;
	owned?: boolean;
	scratch?: string;
	rejectAck?: boolean;
	shell?: AgentsDeps["jobShell"];
} = {}) {
	const scratch = mkdtempSync(join(tmpdir(), "child-jobs-test-"));
	const frames: any[] = [];
	const ipc = Object.assign(new EventEmitter(), {
		connected: options.connected !== false,
		send(frame: any, cb?: (error: Error | null) => void) {
			frames.push(frame);
			cb?.(null);
			queueMicrotask(() => ipc.emit("message", { id: frame.id, kind: "ack", accepted: !options.rejectAck }));
			return true;
		},
	});
	const tools = new Map<string, any>();
	const handlers = new Map<string, Array<(...args: any[]) => any>>();
	let active = ["read", "bash_background", "subagent_parent_message"];
	const warnings: string[] = [];
	const ctx = {
		cwd: scratch,
		sessionManager: { getSessionId: () => "child-session" },
		ui: { notify: (message: string) => warnings.push(message) },
	} as unknown as ExtensionContext;
	const pi = {
		registerTool: (tool: any) => tools.set(tool.name, tool),
		registerMessageRenderer() {},
		on(name: string, fn: any) { handlers.set(name, [...handlers.get(name) ?? [], fn]); },
		getAllTools: () => [...tools.values()],
		getActiveTools: () => active,
		setActiveTools: (names: string[]) => { active = names; },
	} as unknown as ExtensionAPI;
	gentleAgents(pi, {
		GENTLE_PI_AGENTS_CHILD: "1",
		GENTLE_PI_AGENTS_OWNED_IPC: options.owned === false ? undefined : "1",
		GENTLE_PI_BACKGROUND_JOBS: options.flag ?? "1",
		GENTLE_PI_AGENTS_REQUESTED_TOOLS: options.requested ?? "read,bash_background",
		TMPDIR: options.scratch ?? scratch,
	}, { childIpc: ipc, agentHome: scratch, jobShell: options.shell });
	const fire = async (name: string) => {
		for (const fn of handlers.get(name) ?? []) await fn({}, ctx);
	};
	return {
		scratch, ipc, frames, tools, warnings, ctx, fire,
		active: () => active,
		call: (name: string, params = {}, owner = ctx) => tools.get(name).execute("call", params, undefined, undefined, owner),
		async close() {
			await fire("session_shutdown");
			rmSync(scratch, { recursive: true, force: true });
		},
	};
}

test("runner scratch allocation failure removes background capability before spawn", async () => {
	const f = fixture({ flag: "0" });
	const child = fakeChild();
	let launched: NodeJS.ProcessEnv | undefined;
	const runner = new AgentRunner(new TaskStore(), { maxConcurrency: 1, stallTimeoutMs: 10000 }, {
		spawn: (_command, _args, options) => { launched = options.env; return child.child; },
		now: Date.now,
		schedule: () => () => {},
		pi: { command: "pi", args: [] },
	}, { askUser: async () => ({ cancelled: true }) });
	try {
		const task = runner.run({
			agent: {
				name: "check", description: "check", filePath: "check.md", scope: "global",
				instructions: "check", tools: ["bash_background"],
			},
			prompt: "check", mode: AGENT_MODE.TASK, cwd: f.scratch,
			parentSessionId: "parent", sessionDir: f.scratch,
			env: {
				TMPDIR: join(f.scratch, "absent"),
				GENTLE_PI_CONFIG_HOME: f.scratch,
				GENTLE_PI_BACKGROUND_SUBAGENTS: "on",
			},
		} as any);
		await new Promise(resolve => setImmediate(resolve));
		assert.ok(launched);
		assert.equal(launched.GENTLE_PI_BACKGROUND_JOBS, undefined);
		assert.equal(launched.TMPDIR, join(f.scratch, "absent"));
		runner.cancel(task.id);
	} finally { await f.close(); }
});

test("failed runs bound Unicode output and rejected completion admission is surfaced", async () => {
	const f = fixture({
		rejectAck: true,
		shell: () => ({ operations: {
			async exec(_command, _cwd, options) {
				options.onData(Buffer.from("🙂".repeat(2000)));
				throw new Error("shell-failure");
			},
		} }),
	});
	try {
		await f.fire("session_start");
		await f.call("bash_background", { command: "check" });
		for (let n = 0; n < 200 && f.warnings.length === 0; n++) await new Promise(resolve => setTimeout(resolve, 5));
		assert.equal(f.frames.length, 1);
		assert.equal(f.frames[0].exitCode, null);
		assert.ok(Buffer.byteLength(f.frames[0].outputTail) <= 4096);
		assert.equal(f.frames[0].outputTail.isWellFormed(), true);
		assert.match(f.frames[0].outputTail, /Error: shell-failure$/);
		assert.deepEqual(f.warnings, ["Child background completion failed: parent rejected notification"]);
	} finally { await f.close(); }
});

test("child registration requires exact flag, explicit request and live owned IPC", async () => {
	for (const options of [
		{ flag: "0" }, { flag: "true" }, { requested: "read" },
		{ connected: false }, { owned: false }, { scratch: "/nonexistent/child-jobs-scratch" },
	]) {
		const f = fixture(options);
		try {
			await f.fire("session_start");
			assert.equal(f.tools.has("bash_background"), false);
		} finally { await f.close(); }
	}
	const f = fixture();
	try {
		await f.fire("session_start");
		assert.deepEqual([...f.tools.keys()].sort(), ["bash_background", "job_list", "job_stop", "subagent_parent_message"]);
		assert.deepEqual(f.active(), ["read", "bash_background", "subagent_parent_message", "job_list", "job_stop"]);
	} finally { await f.close(); }
});

test("finite configured shell sends one acknowledged exit 7 with private output while child lives", async () => {
	const f = fixture();
	try {
		await f.fire("session_start");
		const result = await f.call("bash_background", {
			command: "printf 'distinct-stdout\\n'; printf 'distinct-stderr\\n' >&2; exit 7",
		});
		const job = result.details.gentleJobs;
		assert.equal(job.jobId, "job-1");
		assert.equal(relative(f.scratch, job.outputPath).startsWith(".."), false);
		for (let n = 0; n < 200 && !f.frames.some(frame => frame.kind === "job"); n++) await new Promise(resolve => setTimeout(resolve, 5));
		assert.deepEqual(f.frames.filter(frame => frame.kind === "job"), [{
			id: "n1", kind: "job", phase: "settled", jobId: "job-1", exitCode: 7,
			outputTail: "distinct-stdout\ndistinct-stderr",
		}]);
		assert.equal(readFileSync(job.outputPath, "utf8"), "distinct-stdout\ndistinct-stderr\n");
		assert.equal(f.ipc.connected, true);
		assert.deepEqual(f.warnings, []);
		await f.call("subagent_parent_message", { message: "still alive" });
		assert.equal(f.frames.at(-1).id, "n2");
	} finally { await f.close(); }
});

test("child ownership, explicit stop and disconnect close admission", async () => {
	const f = fixture();
	try {
		await f.fire("session_start");
		const result = await f.call("bash_background", { command: "sleep 20" });
		await assert.rejects(f.call("job_stop", { job_id: "job-1" }, {
			...f.ctx, sessionManager: { getSessionId: () => "other" },
		} as any), /No background job/);
		await f.call("job_stop", { job_id: result.details.gentleJobs.jobId });
		assert.match((await f.call("job_list")).content[0].text, /stopped/);
		await f.call("bash_background", { command: "sleep 20" });
		f.ipc.connected = false;
		f.ipc.emit("disconnect");
		await assert.rejects(f.call("bash_background", { command: "echo forbidden" }), /closed/);
		await f.fire("session_shutdown");
		assert.match((await f.call("job_list")).content[0].text, /job-2 · stopped/);
		assert.deepEqual(f.frames, []);
	} finally { await f.close(); }
});
