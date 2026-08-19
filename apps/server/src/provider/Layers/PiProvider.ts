// @effect-diagnostics globalErrorInEffectCatch:off
// @effect-diagnostics globalErrorInEffectFailure:off
/**
 * PiProvider — health probe and model discovery for the Pi driver.
 *
 * Probes `pi --version` for availability/version and, when enabled, runs a
 * throwaway `pi --mode rpc` session to discover the live model catalog and
 * each model's supported thinking levels. Discovery failures degrade to the
 * configured custom models rather than failing the whole snapshot.
 *
 * @module provider/Layers/PiProvider
 */
import {
  type ModelCapabilities,
  type PiSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { makePiRpcClient, type PiRpcModel } from "../pi/PiRpcSessionRuntime.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODEL_DISCOVERY_TIMEOUT_MS = 15_000;

function piModelCapabilities(model: PiRpcModel | undefined): ModelCapabilities {
  if (!model) {
    return createModelCapabilities({ optionDescriptors: [] });
  }
  const choices = model.thinkingLevels.map((level) => ({
    id: level,
    label: level,
    ...(level === "medium" ? { isDefault: true } : {}),
  }));
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking_level",
        label: "Thinking level",
        description: "Pi native reasoning level for this model.",
        type: "select",
        options: choices,
        currentValue: choices.length > 0 ? "medium" : undefined,
      },
    ],
  });
}

function piModelsFromCatalog(
  catalog: ReadonlyArray<PiRpcModel>,
  customModels: ReadonlyArray<string> | undefined,
): ReadonlyArray<ServerProviderModel> {
  const bySlug = new Map<string, ServerProviderModel>();
  for (const model of catalog) {
    const capabilities = piModelCapabilities(model);
    bySlug.set(model.slug, {
      slug: model.slug,
      name: model.name.trim() || model.slug,
      isCustom: false,
      capabilities,
    });
  }
  for (const custom of customModels ?? []) {
    const slug = custom.trim();
    if (!slug) continue;
    if (!bySlug.has(slug)) {
      bySlug.set(slug, {
        slug,
        name: slug,
        isCustom: true,
        capabilities: createModelCapabilities({ optionDescriptors: [] }),
      });
    }
  }
  return Array.from(bySlug.values());
}

const runPiVersionCommand = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = piSettings.binaryPath?.trim() || "pi";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

const discoverPiModels = (piSettings: PiSettings, environment: NodeJS.ProcessEnv = process.env) =>
  Effect.gen(function* () {
    const client = makePiRpcClient();
    return yield* Effect.tryPromise({
      try: () =>
        client.discoverModelCatalog({
          binaryPath: piSettings.binaryPath,
          env: environment,
        }),
      catch: (cause) => new Error(String(cause)),
    });
  }).pipe(
    Effect.timeout(MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.orElseSucceed(() => ({ state: {}, models: [] as ReadonlyArray<PiRpcModel> })),
  );

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: piModelsFromCatalog([], piSettings.customModels),
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runPiVersionCommand(piSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    if (isCommandMissingCause(error)) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: piSettings.enabled,
        checkedAt,
        models: piModelsFromCatalog([], piSettings.customModels),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi CLI is not installed or not found on PATH.",
        },
      });
    }
    yield* Effect.logWarning("Pi CLI health check failed.", { errorTag: error._tag });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: piModelsFromCatalog([], piSettings.customModels),
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Could not read the Pi version.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: piModelsFromCatalog([], piSettings.customModels),
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but timed out while running `pi --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);

  const { models } = yield* discoverPiModels(piSettings, environment);
  yield* Effect.logInfo(`Pi model discovery found ${models.length} models.`);
  const modelList = piModelsFromCatalog(models, piSettings.customModels);

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models: modelList,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "authenticated" },
    },
  });
});

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromCatalog([], piSettings.customModels);
    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Pi CLI availability...",
      },
    });
  });
}
