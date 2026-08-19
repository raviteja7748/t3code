/**
 * PiAdapterLive — surface adapter for the Pi provider.
 *
 * Wraps `PiRpcClient` (the `pi --mode rpc` transport) behind the generic
 * `ProviderAdapterShape` contract and maps Pi RPC events / failures into T3's
 * canonical provider runtime events and adapter error algebra. It owns no
 * persistent policy; all Pi process and RPC lifecycle lives in `PiRpcClient`.
 *
 * Pi is a full-access provider: approval and structured user-input flows are
 * not applicable and surface as unsupported errors.
 *
 * @module provider/Layers/PiAdapter
 */
import * as NodeCrypto from "node:crypto";
const randomUUID = (): string => NodeCrypto.randomUUID();

import {
  EventId,
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  ThreadId,
  TurnId,
  type ProviderTurnStartResult,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { type ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import {
  makePiRpcClient,
  normalizePiThinkingLevel,
  PI_RUNTIME_EVENT_TYPES,
  type PiRpcClient,
  type PiRpcRuntimeEvent,
  type PiRpcStartSessionInput,
  type PiThinkingLevel,
} from "../pi/PiRpcSessionRuntime.ts";

const PROVIDER = ProviderDriverKind.make("pi");

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const asString = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

function toMessage(cause: unknown, fallback: string): string {
  return cause instanceof Error && cause.message.trim().length > 0 ? cause.message : fallback;
}

function detailText(value: unknown, fallback: string): string {
  if (typeof value === "string") return value;
  if (value === undefined || value === null) return fallback;
  return JSON.stringify(value);
}

function toAdapterError(threadId: ThreadId, method: string, cause: unknown): ProviderAdapterError {
  const normalized = toMessage(cause, "").toLowerCase();
  if (normalized.includes("unknown pi rpc thread") || normalized.includes("unknown pi thread")) {
    return new ProviderAdapterSessionNotFoundError({ provider: "pi", threadId, cause });
  }
  return new ProviderAdapterRequestError({
    provider: "pi",
    method,
    detail: toMessage(cause, `${method} failed`),
    cause,
  });
}

function modelFromResumeCursor(resumeCursor: unknown): string | undefined {
  const resume = asRecord(resumeCursor);
  const provider = asString(resume?.modelProvider);
  const modelId = asString(resume?.modelId);
  return provider && modelId ? `${provider}/${modelId}` : undefined;
}

function thinkingLevelFromResumeCursor(resumeCursor: unknown): string | undefined {
  return asString(asRecord(resumeCursor)?.thinkingLevel);
}

function collectPiTextFragments(value: unknown): string[] {
  if (typeof value === "string") return value.length > 0 ? [value] : [];
  if (!value || typeof value !== "object") return [];
  if (Array.isArray(value)) return value.flatMap((entry) => collectPiTextFragments(entry));
  const record = value as Record<string, unknown>;
  if (record.type === "text") {
    const text = asString(record.text);
    return text && text.length > 0 ? [text] : [];
  }
  if (record.type === "thinking" || record.type === "redacted_thinking") return [];
  if ("content" in record) return collectPiTextFragments(record.content);
  return [];
}

function extractPiAssistantText(message: unknown): string | undefined {
  const record = asRecord(message);
  if (!record) return undefined;
  const content = collectPiTextFragments(record.content);
  if (content.length > 0) {
    const text = content.join("");
    return text.trim().length > 0 ? text : undefined;
  }
  const directText = asString(record.text);
  return directText?.trim() ? directText : undefined;
}

function hasVisibleAssistantText(message: unknown): boolean {
  return extractPiAssistantText(message) !== undefined;
}

function assistantItemId(threadId: ThreadId, turnId?: TurnId): string {
  return `pi-assistant:${threadId}:${turnId ?? "session"}`;
}

function assistantTurnKey(threadId: ThreadId, turnId?: TurnId): string {
  return `${threadId}:${turnId ?? "session"}`;
}

export interface MakePiAdapterOptions {
  readonly instanceId?: string;
  readonly binaryPath?: string;
  readonly agentDir?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly client?: PiRpcClient;
}

export const makePiAdapter = (
  _config: unknown,
  options?: MakePiAdapterOptions,
): Effect.Effect<ProviderAdapterShape<ProviderAdapterError>, never, never> =>
  Effect.gen(function* () {
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const client = options?.client ?? makePiRpcClient();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextEvent = () =>
      Effect.all({ eventId: Effect.sync(() => EventId.make(randomUUID())), createdAt: nowIso });
    const offer = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const completedAssistantTurns = new Set<string>();
    const abortingTurnIds = new Map<string, string>();
    const handleRpcEvent = (event: PiRpcRuntimeEvent) =>
      Effect.gen(function* () {
        if (event.kind === "exit") {
          abortingTurnIds.delete(String(event.threadId));
          completedAssistantTurns.delete(assistantTurnKey(event.threadId, event.turnId));
          if (event.expected) {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "session.exited",
              ...stamp,
              provider: PROVIDER,
              threadId: event.threadId,
              ...(event.turnId ? { turnId: event.turnId } : {}),
              payload: { reason: "Pi session stopped", recoverable: true, exitKind: "graceful" },
            });
          } else {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "runtime.error",
              ...stamp,
              provider: PROVIDER,
              threadId: event.threadId,
              ...(event.turnId ? { turnId: event.turnId } : {}),
              payload: {
                message: `Pi RPC process exited unexpectedly (${event.code ?? "signal"}${event.signal ? `:${event.signal}` : ""}).`,
                class: "transport_error",
              },
            });
          }
          return;
        }
        if (event.kind !== "rpc-event") return;
        const { threadId, turnId, payload } = event;
        const type = payload.type;
        if (typeof type !== "string" || !PI_RUNTIME_EVENT_TYPES.has(type)) return;

        if (type === "turn_start") {
          if (!turnId) return;
          abortingTurnIds.delete(String(threadId));
          completedAssistantTurns.delete(assistantTurnKey(threadId, turnId));
          const stamp = yield* nextEvent();
          yield* offer({
            type: "turn.started",
            ...stamp,
            provider: PROVIDER,
            threadId,
            turnId,
            payload: {},
          });
          return;
        }
        if (type === "message_update") {
          const assistantEvent = asRecord(payload.assistantMessageEvent);
          const assistantType = asString(assistantEvent?.type);
          if (assistantType === "text_delta") {
            const delta = asString(assistantEvent?.delta);
            if (!delta) return;
            const stamp = yield* nextEvent();
            yield* offer({
              type: "content.delta",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              itemId: RuntimeItemId.make(assistantItemId(threadId, turnId)),
              payload: { streamKind: "assistant_text", delta },
            });
            return;
          }
          if (assistantType === "thinking_delta") {
            const delta = asString(assistantEvent?.delta);
            if (!delta) return;
            const stamp = yield* nextEvent();
            yield* offer({
              type: "content.delta",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              itemId: RuntimeItemId.make(assistantItemId(threadId, turnId)),
              payload: { streamKind: "reasoning_text", delta },
            });
            return;
          }
          if (assistantType === "error") {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "runtime.error",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              payload: {
                message:
                  asString(assistantEvent?.reason) ??
                  asString(assistantEvent?.error) ??
                  "Pi assistant message failed.",
                class: "provider_error",
                detail: payload,
              },
            });
            return;
          }
          return;
        }
        if (type === "message_end") {
          const message = asRecord(payload.message);
          if (asString(message?.role) !== "assistant") return;
          if (!hasVisibleAssistantText(payload.message)) return;
          completedAssistantTurns.add(assistantTurnKey(threadId, turnId));
          const text = extractPiAssistantText(payload.message);
          const stamp = yield* nextEvent();
          yield* offer({
            type: "item.completed",
            ...stamp,
            provider: PROVIDER,
            threadId,
            ...(turnId ? { turnId } : {}),
            itemId: RuntimeItemId.make(assistantItemId(threadId, turnId)),
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: "Assistant message",
              ...(text ? { detail: text } : {}),
              data: payload.message,
            },
          });
          return;
        }
        if (type === "tool_execution_start") {
          const toolName = asString(payload.toolName) ?? "tool";
          const toolCallId = asString(payload.toolCallId) ?? randomUUID();
          const stamp = yield* nextEvent();
          yield* offer({
            type: "item.started",
            ...stamp,
            provider: PROVIDER,
            threadId,
            ...(turnId ? { turnId } : {}),
            itemId: RuntimeItemId.make(`pi-tool-${toolCallId}`),
            payload: { itemType: "dynamic_tool_call", status: "inProgress", detail: toolName },
          });
          return;
        }
        if (type === "tool_execution_update") {
          const toolCallId = asString(payload.toolCallId);
          if (!toolCallId) return;
          const stamp = yield* nextEvent();
          yield* offer({
            type: "item.updated",
            ...stamp,
            provider: PROVIDER,
            threadId,
            ...(turnId ? { turnId } : {}),
            itemId: RuntimeItemId.make(`pi-tool-${toolCallId}`),
            payload: {
              itemType: "dynamic_tool_call",
              status: "inProgress",
              detail: detailText(payload.partialResult, asString(payload.toolName) ?? "tool"),
            },
          });
          return;
        }
        if (type === "tool_execution_end") {
          const toolName = asString(payload.toolName) ?? "tool";
          const toolCallId = asString(payload.toolCallId) ?? randomUUID();
          const stamp = yield* nextEvent();
          yield* offer({
            type: "item.completed",
            ...stamp,
            provider: PROVIDER,
            threadId,
            ...(turnId ? { turnId } : {}),
            itemId: RuntimeItemId.make(`pi-tool-${toolCallId}`),
            payload: {
              itemType: "dynamic_tool_call",
              status: payload.isError === true ? "failed" : "completed",
              detail: detailText(payload.result, toolName),
            },
          });
          return;
        }
        if (type === "turn_end") {
          const key = assistantTurnKey(threadId, turnId);
          if (!completedAssistantTurns.has(key) && hasVisibleAssistantText(payload.message)) {
            completedAssistantTurns.add(key);
            const text = extractPiAssistantText(payload.message);
            const stamp = yield* nextEvent();
            yield* offer({
              type: "item.completed",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              itemId: RuntimeItemId.make(assistantItemId(threadId, turnId)),
              payload: {
                itemType: "assistant_message",
                status: "completed",
                title: "Assistant message",
                ...(text ? { detail: text } : {}),
                data: payload.message,
              },
            });
          }
          return;
        }
        if (type === "agent_end") {
          if (turnId) completedAssistantTurns.delete(assistantTurnKey(threadId, turnId));
          const aborting = abortingTurnIds.get(String(threadId));
          const interrupted = aborting !== undefined && (aborting === "*" || aborting === String(turnId));
          if (interrupted) abortingTurnIds.delete(String(threadId));
          if (turnId) {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "turn.completed",
              ...stamp,
              provider: PROVIDER,
              threadId,
              turnId,
              payload: { state: interrupted ? "interrupted" : "completed", stopReason: interrupted ? "abort" : null },
            });
          }
          return;
        }
      }).pipe(Effect.ignore);

    client.subscribe((event) => {
      void Effect.runPromise(handleRpcEvent(event));
    });

    const adapter: ProviderAdapterShape<ProviderAdapterError> = {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),

      startSession: (input) =>
        Effect.gen(function* () {
          const model =
            modelFromResumeCursor(input.resumeCursor) ??
            (input.modelSelection?.model ? String(input.modelSelection.model) : undefined);
          const selectedThinking =
            input.modelSelection &&
            (!options?.instanceId || String(input.modelSelection.instanceId) === options.instanceId)
              ? getModelSelectionStringOptionValue(input.modelSelection, "thinking_level")
              : undefined;
          const thought =
            normalizePiThinkingLevel(selectedThinking) ??
            normalizePiThinkingLevel(thinkingLevelFromResumeCursor(input.resumeCursor));
          const startInput: PiRpcStartSessionInput = {
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            ...(input.cwd ? { cwd: input.cwd } : {}),
            ...(input.resumeCursor ? { resumeCursor: input.resumeCursor } : {}),
            ...(model ? { model } : {}),
            ...(thought ? { thinkingLevel: thought as PiThinkingLevel } : {}),
            ...(options?.binaryPath ? { binaryPath: options.binaryPath } : {}),
            ...(options?.agentDir ? { agentDir: options.agentDir } : {}),
            ...(options?.env ? { env: options.env } : {}),
          };
          return yield* Effect.tryPromise({
            try: () => client.startSession(startInput),
            catch: (cause) => toAdapterError(input.threadId, "startSession", cause),
          });
        }),

      sendTurn: (input) =>
        Effect.gen(function* () {
          const model = input.modelSelection?.model
            ? String(input.modelSelection.model)
            : undefined;
          const thinkingLevel = normalizePiThinkingLevel(
            input.modelSelection &&
              (!options?.instanceId ||
                String(input.modelSelection.instanceId) === options.instanceId)
              ? getModelSelectionStringOptionValue(input.modelSelection, "thinking_level")
              : undefined,
          );
          const images = (input.attachments ?? [])
            .map((attachment): { type: "image"; data: string; mimeType: string } | undefined => {
              const record = asRecord(attachment);
              const data = asString(record?.data) ?? asString(attachment as never);
              if (!data) return undefined;
              return { type: "image", data, mimeType: asString(record?.mimeType) ?? "image/png" };
            })
            .filter(
              (image): image is { type: "image"; data: string; mimeType: string } =>
                image !== undefined,
            );
          const turnResult = yield* Effect.tryPromise({
            try: () =>
              client.sendTurn({
                threadId: input.threadId,
                ...(input.input ? { input: input.input } : {}),
                ...(model ? { model } : {}),
                ...(thinkingLevel ? { thinkingLevel } : {}),
                ...(images.length > 0 ? { images } : {}),
              }),
            catch: (cause) => toAdapterError(input.threadId, "sendTurn", cause),
          });
          return turnResult as ProviderTurnStartResult;
        }),

      interruptTurn: (threadId, turnId) =>
        Effect.gen(function* () {
          abortingTurnIds.set(String(threadId), turnId ? String(turnId) : "*");
          yield* Effect.tryPromise({
            try: () => client.interruptTurn(threadId),
            catch: (cause) => {
              abortingTurnIds.delete(String(threadId));
              return toAdapterError(threadId, "interruptTurn", cause);
            },
          });
        }),

      respondToRequest: () =>
        Effect.fail(
          new ProviderAdapterValidationError({
            provider: "pi",
            operation: "respondToRequest",
            issue: "Pi is a full-access provider and does not raise approval requests.",
          }),
        ),

      respondToUserInput: () =>
        Effect.fail(
          new ProviderAdapterValidationError({
            provider: "pi",
            operation: "respondToUserInput",
            issue: "Pi does not raise structured user-input questions through this adapter.",
          }),
        ),

      stopSession: (threadId) =>
        Effect.tryPromise({
          try: () => client.stopSession(threadId),
          catch: (cause) => toAdapterError(threadId, "stopSession", cause),
        }).pipe(Effect.asVoid),

      listSessions: () => Effect.sync(() => client.listSessions()),
      hasSession: (threadId) => Effect.sync(() => client.hasSession(threadId)),

      readThread: (threadId) =>
        Effect.tryPromise({
          try: () => client.readThread(threadId),
          catch: (cause) => toAdapterError(threadId, "readThread", cause),
        }),

      rollbackThread: () =>
        Effect.fail(
          new ProviderAdapterValidationError({
            provider: "pi",
            operation: "rollbackThread",
            issue: "Pi provider does not support thread rollback.",
          }),
        ),

      stopAll: () => Effect.sync(() => client.stopAll()),
    };

    return adapter;
  });
