import * as Schema from "effect/Schema";

const Content = Schema.Struct({
  type: Schema.String,
  text: Schema.optional(Schema.String),
  thinking: Schema.optional(Schema.String),
});
export const PiUsage = Schema.Struct({
  input: Schema.Number,
  output: Schema.Number,
  cacheRead: Schema.Number,
  cacheWrite: Schema.Number,
  totalTokens: Schema.Number,
  cost: Schema.optional(Schema.Struct({ total: Schema.Number })),
});
export const PiMessage = Schema.Struct({
  role: Schema.String,
  content: Schema.optional(Schema.Union([Schema.String, Schema.Array(Content)])),
  stopReason: Schema.optional(Schema.String),
  errorMessage: Schema.optional(Schema.String),
  usage: Schema.optional(PiUsage),
  model: Schema.optional(Schema.String),
});
export const PiModel = Schema.Struct({
  id: Schema.String,
  provider: Schema.String,
  name: Schema.String,
  reasoning: Schema.Boolean,
  contextWindow: Schema.Number,
});
export type PiModel = typeof PiModel.Type;
export const PiModels = Schema.Struct({ models: Schema.Array(PiModel) });
export const PiState = Schema.Struct({
  sessionId: Schema.String,
  sessionFile: Schema.optional(Schema.String),
  model: Schema.optional(PiModel),
  thinkingLevel: Schema.String,
});
export const PiMessages = Schema.Struct({ messages: Schema.Array(PiMessage) });

/** Decode only the native fields used at the adapter boundary. Unknown events remain readable. */
export const PiRecord = Schema.Struct({
  type: Schema.String,
  id: Schema.optional(Schema.String),
  command: Schema.optional(Schema.String),
  success: Schema.optional(Schema.Boolean),
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
  message: Schema.optional(PiMessage),
  assistantMessageEvent: Schema.optional(
    Schema.Struct({
      type: Schema.String,
      delta: Schema.optional(Schema.String),
      contentIndex: Schema.optional(Schema.Number),
    }),
  ),
  toolCallId: Schema.optional(Schema.String),
  toolName: Schema.optional(Schema.String),
  args: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Unknown),
  partialResult: Schema.optional(Schema.Unknown),
  isError: Schema.optional(Schema.Boolean),
  method: Schema.optional(Schema.String),
  title: Schema.optional(Schema.String),
  options: Schema.optional(Schema.Array(Schema.String)),
  timeout: Schema.optional(Schema.Number),
  name: Schema.optional(Schema.String),
  reason: Schema.optional(Schema.String),
  aborted: Schema.optional(Schema.Boolean),
  errorMessage: Schema.optional(Schema.String),
});
export type PiRecord = typeof PiRecord.Type;

const decodeRecord = Schema.decodeSync(Schema.fromJsonString(PiRecord));
const MAX_RECORD_CHARS = 16 * 1024 * 1024;

/** Pi uses LF framing; readline also splits valid U+2028/U+2029 in JSON strings. */
export function makePiJsonlDecoder() {
  let buffer = "";
  return {
    push(chunk: string): PiRecord[] {
      buffer += chunk;
      const records: PiRecord[] = [];
      let boundary: number;
      while ((boundary = buffer.indexOf("\n")) !== -1) {
        if (boundary > MAX_RECORD_CHARS) throw new Error("Pi RPC record exceeds 16 MiB.");
        const line = buffer.slice(0, boundary).replace(/\r$/, "");
        buffer = buffer.slice(boundary + 1);
        if (line.trim()) records.push(decodeRecord(line));
      }
      if (buffer.length > MAX_RECORD_CHARS) throw new Error("Pi RPC record exceeds 16 MiB.");
      return records;
    },
    finish(): void {
      if (buffer.trim()) throw new Error("Pi exited with an incomplete RPC record.");
    },
  };
}

export function piMessageText(message: typeof PiMessage.Type): string {
  return typeof message.content === "string"
    ? message.content
    : (message.content ?? []).map((block) => block.text ?? "").join("");
}

export function piModelSlug(model: Pick<PiModel, "provider" | "id">): string {
  return `${model.provider}/${model.id}`;
}

export function splitPiModelSlug(slug: string): { provider: string; modelId: string } {
  const boundary = slug.indexOf("/");
  if (boundary < 1 || boundary === slug.length - 1) {
    throw new Error("Pi models must use provider/model identifiers.");
  }
  return { provider: slug.slice(0, boundary), modelId: slug.slice(boundary + 1) };
}
