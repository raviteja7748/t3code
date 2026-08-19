// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off
// @effect-diagnostics globalTimers:off
/**
 * PiRpcSessionRuntime — thin RPC transport for `pi --mode rpc`.
 *
 * Spawns a `pi --mode rpc` child process and speaks its JSON-lines protocol:
 *   - commands are JSON objects on stdin with a `type` (and optional `id`)
 *   - responses are `{ id, type: "response", command, success, data|error }`
 *   - agent events are streamed as JSON lines on stdout
 *   - extension UI requests are streamed with `type: "extension_ui_request"`
 *
 * This module owns one child per thread and correlates request/response by
 * `id`. It is deliberately protocol-only: it never imports T3 provider
 * orchestration and carries no cross-provider policy. `PiAdapter` sits on
 * top and maps the event stream to canonical `ProviderRuntimeEvent`s.
 *
 * `discoverModelCatalog` exposes a single-thread variant used by the health
 * probe to list available models without touching a user thread.
 *
 * @module provider/pi/PiRpcSessionRuntime
 */
import * as NodeCrypto from "node:crypto";
const randomUUID = (): string => NodeCrypto.randomUUID();
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";

import {
  type RuntimeMode,
  type ProviderSession,
  type ThreadId,
  TurnId,
  type ProviderTurnStartResult,
} from "@t3tools/contracts";
import type {
  ProviderThreadSnapshot,
  ProviderThreadTurnSnapshot,
} from "../Services/ProviderAdapter.ts";

const RPC_REQUEST_TIMEOUT_MS = 10_000;

export type PiThinkingLevel = "minimal" | "low" | "medium" | "high" | "max" | "xhigh";

export function piModelSlug(provider: string, modelId: string): string {
  return `${provider}/${modelId}`;
}

export function parsePiModelSlug(
  value: string | null | undefined,
): { provider: string; modelId: string } | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const slashIndex = trimmed.indexOf("/");
  if (slashIndex <= 0 || slashIndex === trimmed.length - 1) return null;
  return {
    provider: trimmed.slice(0, slashIndex),
    modelId: trimmed.slice(slashIndex + 1),
  };
}

/** Normalize an arbitrary thinking level value to a known Pi level. */
export function normalizePiThinkingLevel(value: unknown): PiThinkingLevel | undefined {
  if (typeof value !== "string") return undefined;
  const normalized = value.toLowerCase();
  switch (normalized) {
    case "minimal":
    case "low":
    case "medium":
    case "high":
    case "max":
    case "xhigh":
      return normalized;
    default:
      return undefined;
  }
}

export interface PiRpcModel {
  readonly slug: string;
  readonly name: string;
  readonly thinkingLevels: ReadonlyArray<PiThinkingLevel>;
}

type PiRpcCommand =
  | { id?: string; type: "get_state" }
  | { id?: string; type: "get_available_models" }
  | {
      id?: string;
      type: "prompt";
      message: string;
      images?: ReadonlyArray<{ type: "image"; data: string; mimeType: string }>;
    }
  | { id?: string; type: "abort" }
  | { id?: string; type: "switch_session"; sessionPath: string }
  | { id?: string; type: "set_model"; provider: string; modelId: string }
  | { id?: string; type: "set_thinking_level"; level: PiThinkingLevel };

interface PiRpcResponse {
  readonly id?: string;
  readonly type: "response";
  readonly command: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string;
}

interface PiRpcState {
  readonly model?: { id?: string; name?: string; provider?: string } | null;
  readonly thinkingLevel?: string;
  readonly isStreaming?: boolean;
  readonly sessionFile?: string;
  readonly sessionId?: string;
}

export type PiRpcRuntimeEvent =
  | { kind: "rpc-event"; threadId: ThreadId; turnId?: TurnId; payload: Record<string, unknown> }
  | { kind: "stderr"; threadId: ThreadId; turnId?: TurnId; line: string }
  | {
      kind: "exit";
      threadId: ThreadId;
      turnId?: TurnId;
      code: number | null;
      signal: NodeJS.Signals | null;
      expected: boolean;
    };

export interface PiRpcStartSessionInput {
  readonly threadId: ThreadId;
  readonly cwd?: string;
  readonly runtimeMode: RuntimeMode;
  readonly model?: string;
  readonly thinkingLevel?: PiThinkingLevel;
  readonly resumeCursor?: unknown;
  readonly binaryPath?: string;
  readonly agentDir?: string;
  readonly env?: NodeJS.ProcessEnv;
}

export interface PiRpcSendTurnInput {
  readonly threadId: ThreadId;
  readonly input?: string;
  readonly model?: string;
  readonly thinkingLevel?: PiThinkingLevel;
  readonly images?: ReadonlyArray<{ type: "image"; data: string; mimeType: string }>;
}

interface PiRpcSessionState {
  readonly child: NodeChildProcess.ChildProcessWithoutNullStreams;
  readonly stdoutRl: NodeReadline.Interface;
  readonly stderrRl: NodeReadline.Interface;
  readonly pending: Map<
    string,
    { resume: (r: PiRpcResponse) => void; timeout: ReturnType<typeof setTimeout> }
  >;
  readonly turns: ProviderThreadTurnSnapshot[];
  readonly createdAt: string;
  threadId: ThreadId;
  cwd: string | undefined;
  runtimeMode: RuntimeMode;
  model: string | undefined;
  thinkingLevel: PiThinkingLevel | undefined;
  resumeCursor: unknown;
  sessionFile: string | undefined;
  sessionId: string | undefined;
  currentTurnId: TurnId | undefined;
  hasObservedTurnStart: boolean;
  status: ProviderSession["status"];
  updatedAt: string;
  abortRequested: boolean;
  stopping: boolean;
}

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const asString = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

/** Turn start / end / stream / tool markers emitted by Pi agent sessions. */
export const PI_RUNTIME_EVENT_TYPES = new Set([
  "turn_start",
  "turn_end",
  "agent_start",
  "agent_end",
  "agent_settled",
  "message_start",
  "message_update",
  "message_end",
  "tool_call",
  "tool_result",
  "tool_execution_start",
  "tool_execution_update",
  "tool_execution_end",
  "thinking_level_changed",
]);

export const isPiRuntimeEventType = (value: unknown): boolean =>
  typeof value === "string" && PI_RUNTIME_EVENT_TYPES.has(value);

export class PiRpcClient {
  private readonly sessions = new Map<ThreadId, PiRpcSessionState>();
  private readonly listeners = new Set<(event: PiRpcRuntimeEvent) => void>();

  subscribe(listener: (event: PiRpcRuntimeEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private emit(event: PiRpcRuntimeEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private toSession(session: PiRpcSessionState): ProviderSession {
    return {
      provider: "pi" as never,
      status: session.status,
      runtimeMode: session.runtimeMode,
      ...(session.cwd ? { cwd: session.cwd } : {}),
      ...(session.model ? { model: session.model } : {}),
      threadId: session.threadId,
      ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
      ...(session.currentTurnId ? { activeTurnId: session.currentTurnId } : {}),
      createdAt: session.createdAt,
      updatedAt: session.updatedAt,
    };
  }

  private clearPending(session: PiRpcSessionState, error: Error): void {
    for (const pending of session.pending.values()) {
      clearTimeout(pending.timeout);
      pending.resume({
        type: "response",
        command: "",
        success: false,
        error: error.message,
      });
    }
    session.pending.clear();
  }

  private handleResponse(session: PiRpcSessionState, response: PiRpcResponse): void {
    const id = response.id;
    if (!id) return;
    const pending = session.pending.get(id);
    if (!pending) return;
    clearTimeout(pending.timeout);
    session.pending.delete(id);
    pending.resume(response);
  }

  private handleRpcEvent(session: PiRpcSessionState, payload: Record<string, unknown>): void {
    const turnId = session.currentTurnId;
    const type = payload.type;
    if (type === "turn_start") {
      session.hasObservedTurnStart = true;
      session.status = "running";
      session.updatedAt = new Date().toISOString();
    } else if (type === "agent_settled" || type === "agent_end") {
      if (session.currentTurnId) {
        session.turns.push({ id: session.currentTurnId, items: [] });
      }
      session.status = "ready";
      session.abortRequested = false;
      session.hasObservedTurnStart = false;
      session.currentTurnId = undefined;
      session.updatedAt = new Date().toISOString();
    } else if (type === "thinking_level_changed") {
      session.thinkingLevel = normalizePiThinkingLevel(payload.level) ?? session.thinkingLevel;
    }
    this.emit({
      kind: "rpc-event",
      threadId: session.threadId,
      ...(turnId ? { turnId } : {}),
      payload,
    });
  }

  private createSession(input: PiRpcStartSessionInput): PiRpcSessionState {
    const binaryPath = input.binaryPath?.trim() || "pi";
    const args = ["--mode", "rpc"];
    if (input.agentDir?.trim()) {
      args.push("--session-dir", input.agentDir.trim());
    }
    const child = NodeChildProcess.spawn(binaryPath, args, {
      stdio: ["pipe", "pipe", "pipe"],
      ...(input.cwd ? { cwd: input.cwd } : {}),
      env: input.env ?? process.env,
    });
    const stdoutRl = NodeReadline.createInterface({ input: child.stdout });
    const stderrRl = NodeReadline.createInterface({ input: child.stderr });
    const now = new Date().toISOString();
    const session: PiRpcSessionState = {
      child,
      stdoutRl,
      stderrRl,
      pending: new Map(),
      turns: [],
      createdAt: now,
      threadId: input.threadId,
      cwd: input.cwd,
      runtimeMode: input.runtimeMode,
      model: input.model,
      thinkingLevel: input.thinkingLevel,
      resumeCursor: input.resumeCursor,
      sessionFile: undefined,
      sessionId: undefined,
      currentTurnId: undefined,
      hasObservedTurnStart: false,
      status: "connecting",
      updatedAt: now,
      abortRequested: false,
      stopping: false,
    };

    stdoutRl.on("line", (line) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line) as unknown;
      } catch {
        this.emit({ kind: "stderr", threadId: session.threadId, line });
        return;
      }
      const record = asRecord(parsed);
      if (!record) return;
      if (record.type === "response") {
        this.handleResponse(session, record as unknown as PiRpcResponse);
        return;
      }
      if (record.type === "extension_ui_request") {
        return;
      }
      const type = record.type;
      if (typeof type !== "string") return;
      if (isPiRuntimeEventType(record.type)) {
        this.handleRpcEvent(session, record as Record<string, unknown>);
        return;
      }
      this.emit({
        kind: "rpc-event",
        threadId: session.threadId,
        payload: record as Record<string, unknown>,
      });
    });

    stderrRl.on("line", (line) => {
      this.emit({
        kind: "stderr",
        threadId: session.threadId,
        ...(session.currentTurnId ? { turnId: session.currentTurnId } : {}),
        line,
      });
    });

    child.on("error", (error) => {
      session.status = "error";
      session.updatedAt = new Date().toISOString();
      this.clearPending(
        session,
        new Error(
          `Pi RPC process failed to start (${error instanceof Error ? error.message : String(error)}).`,
        ),
      );
      this.emit({
        kind: "exit",
        threadId: session.threadId,
        ...(session.currentTurnId ? { turnId: session.currentTurnId } : {}),
        code: null,
        signal: null,
        expected: false,
      });
    });

    child.on("exit", (code, signal) => {
      session.status = session.stopping ? "closed" : "error";
      session.updatedAt = new Date().toISOString();
      this.clearPending(
        session,
        new Error(
          `Pi RPC process exited (${code ?? "signal"}${signal ? `:${String(signal)}` : ""}).`,
        ),
      );
      this.emit({
        kind: "exit",
        threadId: session.threadId,
        ...(session.currentTurnId ? { turnId: session.currentTurnId } : {}),
        code,
        signal,
        expected: session.stopping,
      });
      this.sessions.delete(session.threadId);
    });

    return session;
  }

  private sendCommand(
    session: PiRpcSessionState,
    command: PiRpcCommand,
    timeoutMs = RPC_REQUEST_TIMEOUT_MS,
  ): Promise<PiRpcResponse> {
    if (session.child.stdin.destroyed) {
      return Promise.reject(new Error(`Pi RPC stdin is closed for thread '${session.threadId}'.`));
    }
    const id = command.id ?? randomUUID();
    const payload = JSON.stringify({ ...command, id });
    return new Promise<PiRpcResponse>((resolve) => {
      const timeout = setTimeout(() => {
        session.pending.delete(id);
        resolve({
          id,
          type: "response",
          command: command.type,
          success: false,
          error: `Pi RPC command '${command.type}' timed out.`,
        });
      }, timeoutMs);
      session.pending.set(id, { resume: resolve, timeout });
      session.child.stdin.write(`${payload}\n`, (error) => {
        if (!error) return;
        clearTimeout(timeout);
        session.pending.delete(id);
        resolve({
          id,
          type: "response",
          command: command.type,
          success: false,
          error: error.message,
        });
      });
    });
  }

  private async getState(session: PiRpcSessionState): Promise<PiRpcState> {
    const response = await this.sendCommand(session, { type: "get_state" });
    if (!response.success) {
      throw new Error(response.error ?? "Pi RPC get_state failed.");
    }
    return (asRecord(response.data) as PiRpcState | undefined) ?? {};
  }

  private async stopSessionInstance(session: PiRpcSessionState): Promise<void> {
    session.stopping = true;
    session.updatedAt = new Date().toISOString();
    session.stdoutRl.close();
    session.stderrRl.close();
    session.child.stdin.end();
    if (!session.child.killed) {
      session.child.kill("SIGTERM");
    }
  }

  private async setModel(session: PiRpcSessionState, model: string): Promise<void> {
    const parsed = parsePiModelSlug(model);
    if (!parsed) {
      throw new Error(`Pi models must use 'provider/modelId' format. Received '${model}'.`);
    }
    const response = await this.sendCommand(session, {
      type: "set_model",
      provider: parsed.provider,
      modelId: parsed.modelId,
    });
    if (!response.success) throw new Error(response.error ?? "Pi RPC set_model failed.");
    session.model = model;
  }

  private async setThinkingLevel(
    session: PiRpcSessionState,
    level: PiThinkingLevel,
  ): Promise<void> {
    const response = await this.sendCommand(session, { type: "set_thinking_level", level });
    if (!response.success) throw new Error(response.error ?? "Pi RPC set_thinking_level failed.");
    session.thinkingLevel = level;
  }

  private buildResumeCursor(session: PiRpcSessionState, state: PiRpcState): unknown {
    const model = asRecord(state.model);
    const provider = asString(model?.provider);
    const modelId = asString(model?.id) ?? asString(model?.name);
    const parsedModel = provider && modelId ? piModelSlug(provider, modelId) : session.model;
    const thinkingLevel = normalizePiThinkingLevel(state.thinkingLevel) ?? session.thinkingLevel;
    return {
      ...(state.sessionFile ? { sessionFile: state.sessionFile } : {}),
      ...(state.sessionId ? { sessionId: state.sessionId } : {}),
      ...(parsedModel
        ? { modelProvider: parsedModel.split("/")[0], modelId: parsedModel.split("/")[1] }
        : {}),
      ...(thinkingLevel ? { thinkingLevel } : {}),
      ...(session.cwd ? { cwd: session.cwd } : {}),
    };
  }

  /**
   * Start a Pi session for a thread. Non-interactive full-access only.
   */
  async startSession(input: PiRpcStartSessionInput): Promise<ProviderSession> {
    if (input.runtimeMode === "approval-required") {
      throw new Error("Pi only supports runtimeMode 'full-access'.");
    }
    await this.stopSession(input.threadId).catch(() => undefined);
    const session = this.createSession(input);
    this.sessions.set(input.threadId, session);
    try {
      const resume = asRecord(input.resumeCursor);
      const resumeSessionFile = asString(resume?.sessionFile);
      if (resumeSessionFile) {
        const switchResponse = await this.sendCommand(session, {
          type: "switch_session",
          sessionPath: resumeSessionFile,
        });
        const switchData = asRecord(switchResponse.data);
        if (switchData?.cancelled === true) {
          throw new Error("Pi session switch was cancelled.");
        }
      }
      if (input.model) {
        await this.setModel(session, input.model);
      }
      if (input.thinkingLevel) {
        await this.setThinkingLevel(session, input.thinkingLevel);
      }
      const state = await this.getState(session);
      const model = asRecord(state.model);
      const modelSlug =
        asString(model?.provider) && (asString(model?.id) ?? asString(model?.name))
          ? piModelSlug(
              asString(model?.provider) as string,
              (asString(model?.id) ?? asString(model?.name)) as string,
            )
          : session.model;
      session.sessionFile = state.sessionFile;
      session.sessionId = state.sessionId;
      session.model = modelSlug ?? session.model;
      session.thinkingLevel =
        normalizePiThinkingLevel(state.thinkingLevel) ?? session.thinkingLevel;
      session.resumeCursor = this.buildResumeCursor(session, state);
      session.status = state.isStreaming ? "running" : "ready";
      session.updatedAt = new Date().toISOString();
      return this.toSession(session);
    } catch (error) {
      await this.discardSession(session);
      throw error;
    }
  }

  private async discardSession(session: PiRpcSessionState): Promise<void> {
    if (this.sessions.get(session.threadId) === session) {
      this.sessions.delete(session.threadId);
    }
    await this.stopSessionInstance(session).catch(() => undefined);
  }

  async sendTurn(input: PiRpcSendTurnInput): Promise<ProviderTurnStartResult> {
    const session = this.sessions.get(input.threadId);
    if (!session) {
      throw new Error(`Unknown Pi RPC thread '${input.threadId}'.`);
    }
    if (input.model && input.model !== session.model) {
      await this.setModel(session, input.model);
    }
    if (input.thinkingLevel && input.thinkingLevel !== session.thinkingLevel) {
      await this.setThinkingLevel(session, input.thinkingLevel);
    }
    const turnId = TurnId.make(randomUUID());
    const previous = {
      currentTurnId: session.currentTurnId,
      hasObservedTurnStart: session.hasObservedTurnStart,
      status: session.status,
      updatedAt: session.updatedAt,
    };
    session.currentTurnId = turnId;
    session.hasObservedTurnStart = false;
    session.abortRequested = false;
    session.status = "running";
    session.updatedAt = new Date().toISOString();
    try {
      const response = await this.sendCommand(session, {
        type: "prompt",
        message: input.input ?? "",
        ...(input.images && input.images.length > 0 ? { images: input.images } : {}),
      });
      if (!response.success) {
        throw new Error(response.error ?? "Pi RPC prompt failed.");
      }
      const state = await this.getState(session);
      const model = asRecord(state.model);
      const modelSlug =
        asString(model?.provider) && (asString(model?.id) ?? asString(model?.name))
          ? piModelSlug(
              asString(model?.provider) as string,
              (asString(model?.id) ?? asString(model?.name)) as string,
            )
          : session.model;
      session.model = modelSlug ?? session.model;
      session.thinkingLevel =
        normalizePiThinkingLevel(state.thinkingLevel) ?? session.thinkingLevel;
      session.resumeCursor = this.buildResumeCursor(session, state);
      session.updatedAt = new Date().toISOString();
      return {
        threadId: input.threadId,
        turnId,
        ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
      };
    } catch (error) {
      session.currentTurnId = previous.currentTurnId;
      session.hasObservedTurnStart = previous.hasObservedTurnStart;
      session.status = previous.status;
      session.updatedAt = new Date().toISOString();
      throw error;
    }
  }

  async interruptTurn(threadId: ThreadId): Promise<void> {
    const session = this.sessions.get(threadId);
    if (!session) throw new Error(`Unknown Pi RPC thread '${threadId}'.`);
    session.abortRequested = true;
    session.updatedAt = new Date().toISOString();
    const response = await this.sendCommand(session, { type: "abort" });
    if (!response.success) throw new Error(response.error ?? "Pi RPC abort failed.");
  }

  async stopSession(threadId: ThreadId): Promise<void> {
    const session = this.sessions.get(threadId);
    if (!session) return;
    await this.stopSessionInstance(session);
    this.sessions.delete(threadId);
  }

  stopAll(): void {
    for (const threadId of Array.from(this.sessions.keys())) {
      void this.stopSession(threadId);
    }
  }

  listSessions(): ReadonlyArray<ProviderSession> {
    return Array.from(this.sessions.values()).map(this.toSession.bind(this));
  }

  hasSession(threadId: ThreadId): boolean {
    return this.sessions.has(threadId);
  }

  async readThread(threadId: ThreadId): Promise<ProviderThreadSnapshot> {
    const session = this.sessions.get(threadId);
    if (!session) throw new Error(`Unknown Pi RPC thread '${threadId}'.`);
    return { threadId, turns: [...session.turns] };
  }

  /**
   * Runs a throwaway Pi RPC session, reads current model state and the
   * available-model catalog, then tears the child down. Used by the provider
   * health probe for dynamic model discovery.
   */
  async discoverModelCatalog(input: {
    readonly cwd?: string;
    readonly binaryPath?: string;
    readonly agentDir?: string;
    readonly env?: NodeJS.ProcessEnv;
  }): Promise<{ state: PiRpcState; models: ReadonlyArray<PiRpcModel> }> {
    const session = this.createSession({
      threadId: `probe:${randomUUID()}` as never,
      runtimeMode: "full-access" as RuntimeMode,
      ...input,
    });
    try {
      const state = await this.getState(session);
      const response = await this.sendCommand(session, { type: "get_available_models" });
      const data = asRecord(response.data);
      const rawModels = Array.isArray(data?.models) ? (data.models as unknown[]) : [];
      const models: PiRpcModel[] = [];
      const seen = new Set<string>();
      for (const raw of rawModels) {
        const record = asRecord(raw);
        const provider = asString(record?.provider);
        const modelId = asString(record?.id) ?? asString(record?.name);
        if (!provider || !modelId) continue;
        const slug = piModelSlug(provider, modelId);
        if (seen.has(slug)) continue;
        seen.add(slug);
        const name = asString(record?.name) ?? modelId;
        const thinkingLevels: PiThinkingLevel[] = [];
        const levelMap = asRecord(record?.thinkingLevelMap);
        if (levelMap) {
          for (const value of Object.values(levelMap)) {
            const level = normalizePiThinkingLevel(value ?? asString(record?.thinkingLevel));
            if (level && !thinkingLevels.includes(level)) thinkingLevels.push(level);
          }
        }
        models.push({
          slug,
          name,
          thinkingLevels:
            thinkingLevels.length > 0 ? thinkingLevels : (["medium"] as PiThinkingLevel[]),
        });
      }
      return { state, models };
    } finally {
      await this.stopSessionInstance(session).catch(() => undefined);
    }
  }
}

export const makePiRpcClient = (): PiRpcClient => new PiRpcClient();
