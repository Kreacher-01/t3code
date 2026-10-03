import { describe, expect, it } from "@effect/vitest";
import { makePiJsonlDecoder, splitPiModelSlug } from "./PiProtocol.ts";

describe("Pi JSONL framing", () => {
  it("reconstructs fragmented records without splitting Unicode separators", () => {
    const decoder = makePiJsonlDecoder();
    const record = JSON.stringify({
      type: "message_update",
      assistantMessageEvent: {
        type: "text_delta",
        delta: "hello\u2028world\u2029!",
        contentIndex: 0,
      },
    });
    expect(decoder.push(record.slice(0, 30))).toEqual([]);
    expect(decoder.push(record.slice(30) + '\r\n{"type":"agent_settled"}\n')).toEqual([
      {
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          delta: "hello\u2028world\u2029!",
          contentIndex: 0,
        },
      },
      { type: "agent_settled" },
    ]);
    expect(() => decoder.finish()).not.toThrow();
  });
  it("rejects malformed and incomplete output", () => {
    expect(() => makePiJsonlDecoder().push("invalid\n")).toThrow();
    const decoder = makePiJsonlDecoder();
    decoder.push('{"type":"agent_settled"}');
    expect(() => decoder.finish()).toThrow("incomplete");
  });
  it("preserves slashes inside provider model IDs", () => {
    expect(splitPiModelSlug("openrouter/anthropic/claude-fable-5-1")).toEqual({
      provider: "openrouter",
      modelId: "anthropic/claude-fable-5-1",
    });
    expect(() => splitPiModelSlug("ambiguous-model")).toThrow();
  });
});
