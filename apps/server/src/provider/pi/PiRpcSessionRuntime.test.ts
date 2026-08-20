import { describe, expect, it } from "@effect/vitest";
import { normalizePiThinkingLevel, parsePiModelSlug, piModelSlug } from "./PiRpcSessionRuntime.ts";

describe("PiRpcSessionRuntime model slugs", () => {
  it("builds provider/modelId slugs", () => {
    expect(piModelSlug("openai-codex", "gpt-5.6-sol")).toBe("openai-codex/gpt-5.6-sol");
  });

  it("parses valid provider/modelId slugs", () => {
    expect(parsePiModelSlug("openai-codex/gpt-5.6-sol")).toEqual({
      provider: "openai-codex",
      modelId: "gpt-5.6-sol",
    });
  });

  it("rejects slugs without a provider/modelId split", () => {
    expect(parsePiModelSlug(null)).toBeNull();
    expect(parsePiModelSlug("")).toBeNull();
    expect(parsePiModelSlug("no-slash")).toBeNull();
    expect(parsePiModelSlug("/missing-provider")).toBeNull();
    expect(parsePiModelSlug("missing-model/")).toBeNull();
  });
});

describe("normalizePiThinkingLevel", () => {
  it("normalizes known Pi thinking levels case-insensitively", () => {
    expect(normalizePiThinkingLevel("medium")).toBe("medium");
    expect(normalizePiThinkingLevel("HIGH")).toBe("high");
    expect(normalizePiThinkingLevel("xhigh")).toBe("xhigh");
    expect(normalizePiThinkingLevel("minimal")).toBe("minimal");
  });

  it("returns undefined for unknown levels", () => {
    expect(normalizePiThinkingLevel("turbo")).toBeUndefined();
    expect(normalizePiThinkingLevel(42)).toBeUndefined();
    expect(normalizePiThinkingLevel(undefined)).toBeUndefined();
  });
});
