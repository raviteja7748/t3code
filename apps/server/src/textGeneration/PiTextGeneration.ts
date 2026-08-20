/**
 * PiTextGeneration — Pi does not expose a scoped text-generation surface
 * through the RPC adapter, so the driver supplies a stub that reports the
 * capability as unsupported. Keeps `ProviderInstance.textGeneration` total
 * without wiring commit/PR/branch/title generation through Pi sessions.
 */
import * as Effect from "effect/Effect";
import { TextGenerationError } from "@t3tools/contracts";
import type { TextGeneration } from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "Pi text generation is not available through the T3 Pi provider.",
    }),
  );

export const makePiTextGeneration = (): TextGeneration["Service"] => ({
  generateCommitMessage: () => unsupported("generateCommitMessage"),
  generatePrContent: () => unsupported("generatePrContent"),
  generateBranchName: () => unsupported("generateBranchName"),
  generateThreadTitle: () => unsupported("generateThreadTitle"),
});
