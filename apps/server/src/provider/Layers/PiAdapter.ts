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

/** Extract assistant text fragments from a Pi message / content block. */
function collectPiTextFragments(value: unknown): string[] {
  if (typeof value === "string") return value.length > 0 ? [value] : [];
  if (!value || typeof value !== "object") return [];
  if (Array.isArray(value)) return value.flatMap((entry) => collectPiTextFragments(entry));
  const record = value as Record<string, unknown>;
  if (record.type === "text") {
    const text = asString(record.text);
    return text && text.length > 0 ? [text] : [];
  }
  if ("content" in record) return collectPiTextFragments(record.content);
  if (typeof record.delta === "string" && record.delta.length > 0) return [record.delta];
  if (typeof record.text === "string" && record.text.length > 0) return [record.text];
  return [];
}

function extractAssistantText(message: unknown): string | undefined {
  const record = asRecord(message);
  if (!record) return undefined;
  const joined = collectPiTextFragments(record.content ?? record.partial).join("");
  return joined.trim().length > 0 ? joined : undefined;
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

    const handleRpcEvent = (event: PiRpcRuntimeEvent) =>
      Effect.gen(function* () {
        if (event.kind !== "rpc-event") return;
        const { threadId, turnId, payload } = event;
        const type = payload.type;
        if (typeof type !== "string" || !PI_RUNTIME_EVENT_TYPES.has(type)) return;

        if (type === "turn_start") {
          if (!turnId) return;
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
          const delta = asString(assistantEvent?.delta);
          const assistantType = asString(assistantEvent?.type);
          if (delta && (assistantType === "text_delta" || assistantType === "thinking_delta")) {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "content.delta",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              payload: {
                streamKind:
                  assistantType === "thinking_delta" ? "reasoning_text" : "assistant_text",
                delta,
                contentIndex: 0,
              },
            });
          }
          return;
        }
        if (type === "message_end") {
          const text = extractAssistantText(payload.message);
          if (text) {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "item.completed",
              ...stamp,
              provider: PROVIDER,
              threadId,
              ...(turnId ? { turnId } : {}),
              itemId: RuntimeItemId.make(`pi-assistant-${threadId}-${turnId ?? "session"}`),
              payload: { itemType: "assistant_message", status: "completed", detail: text },
            });
          }
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
        if (type === "agent_end") {
          if (turnId) {
            const stamp = yield* nextEvent();
            yield* offer({
              type: "turn.completed",
              ...stamp,
              provider: PROVIDER,
              threadId,
              turnId,
              payload: { state: "completed", stopReason: null },
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
          return yield* Effect.tryPromise(() => client.startSession(startInput)).pipe(
            Effect.mapError((cause) => toAdapterError(input.threadId, "startSession", cause)),
          );
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
          return yield* Effect.tryPromise(() =>
            client.sendTurn({
              threadId: input.threadId,
              ...(input.input ? { input: input.input } : {}),
              ...(model ? { model } : {}),
              ...(thinkingLevel ? { thinkingLevel } : {}),
              ...(images.length > 0 ? { images } : {}),
            }),
          ).pipe(
            Effect.mapError((cause) => toAdapterError(input.threadId, "sendTurn", cause)),
            Effect.map((result) => result as ProviderTurnStartResult),
          );
        }),

      interruptTurn: (threadId, _turnId) =>
        Effect.tryPromise(() => client.interruptTurn(threadId)).pipe(
          Effect.mapError((cause) => toAdapterError(threadId, "interruptTurn", cause)),
          Effect.asVoid,
        ),

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
        Effect.tryPromise(() => client.stopSession(threadId)).pipe(
          Effect.mapError((cause) => toAdapterError(threadId, "stopSession", cause)),
          Effect.asVoid,
        ),

      listSessions: () => Effect.sync(() => client.listSessions()),
      hasSession: (threadId) => Effect.sync(() => client.hasSession(threadId)),

      readThread: (threadId) =>
        Effect.tryPromise(() => client.readThread(threadId)).pipe(
          Effect.mapError((cause) => toAdapterError(threadId, "readThread", cause)),
        ),

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
