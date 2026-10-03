// @effect-diagnostics nodeBuiltinImport:off
// Opt-in conformance checks against the released CLI, using only a local fake model endpoint.
import * as NodeHttp from "node:http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  EnvironmentId,
  PiSettings,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../../config.ts";
import { makePiTextGeneration } from "../../textGeneration/PiTextGeneration.ts";
import { makePiAdapter } from "./PiAdapter.ts";
import { checkPiProvider } from "./PiProbe.ts";
import { clearMcpProviderSession, setMcpProviderSession } from "../../mcp/McpProviderSession.ts";
import { readTranscriptRecords } from "../../usage/usageTranscriptReader.ts";

const binaryPath = process.env.T3_PI_SMOKE_BINARY;
const layer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-native-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);
const instanceId = ProviderInstanceId.make("pi-native");
const threadId = ThreadId.make("pi-native-thread");
const decodeSettings = Schema.decodeSync(PiSettings);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeCursor = Schema.decodeUnknownSync(Schema.Struct({ sessionFile: Schema.String }));
const decodeMcpRequest = Schema.decodeSync(
  Schema.fromJsonString(
    Schema.Struct({ id: Schema.optional(Schema.Unknown), method: Schema.String }),
  ),
);
class SmokeError extends Schema.TaggedError<SmokeError>()("SmokeError", {
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

function fixtureServer() {
  let tool = false;
  let toolName = "bash";
  let reply = "Native Pi response";
  let requests = 0;
  const mcpMethods: string[] = [];
  const mcpAuthorization: string[] = [];
  const server = NodeHttp.createServer((request, response) => {
    if (request.url === "/mcp") {
      if (request.method !== "POST") {
        response.writeHead(405).end();
        return;
      }
      let body = "";
      request.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      request.on("end", () => {
        const message = decodeMcpRequest(body);
        mcpMethods.push(message.method);
        mcpAuthorization.push(request.headers.authorization ?? "");
        if (message.id === undefined) {
          response.writeHead(202).end();
          return;
        }
        const result =
          message.method === "initialize"
            ? {
                protocolVersion: "2025-03-26",
                capabilities: { tools: {} },
                serverInfo: { name: "t3-smoke", version: "1" },
              }
            : message.method === "tools/call"
              ? { content: [{ type: "text", text: "Native MCP result" }] }
              : {
                  tools: [
                    {
                      name: "echo",
                      description: "Local smoke tool",
                      inputSchema: { type: "object", properties: {}, additionalProperties: false },
                    },
                  ],
                };
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(encodeJson({ jsonrpc: "2.0", id: message.id, result }));
      });
      return;
    }
    request.resume();
    request.on("end", () => {
      requests++;
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finishReason: string | null) =>
        response.write(
          `data: ${encodeJson({
            id: "t3-pi-smoke",
            object: "chat.completion.chunk",
            created: 1,
            model: "model",
            choices: [{ index: 0, delta, finish_reason: finishReason }],
          })}\n\n`,
        );
      if (tool && requests === 1) {
        chunk(
          {
            role: "assistant",
            tool_calls: [
              {
                index: 0,
                id: "smoke-tool",
                type: "function",
                function: {
                  name: toolName,
                  arguments: encodeJson(
                    toolName === "bash" ? { command: "printf pi-smoke > native-tool.txt" } : {},
                  ),
                },
              },
            ],
          },
          null,
        );
        chunk({}, "tool_calls");
      } else {
        chunk({ role: "assistant", content: reply }, null);
        chunk({}, "stop");
      }
      response.write(
        `data: ${encodeJson({ id: "t3-pi-smoke", object: "chat.completion.chunk", created: 1, model: "model", choices: [], usage: { prompt_tokens: 20, completion_tokens: 5, total_tokens: 25 } })}\n\n`,
      );
      response.end("data: [DONE]\n\n");
    });
  });
  return {
    server,
    mcpMethods,
    mcpAuthorization,
    setTool: (name = "bash") => {
      tool = true;
      toolName = name;
    },
    setReply: (value: string) => {
      reply = value;
    },
  };
}
const harness = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-native-profile-" });
  const fixture = fixtureServer();
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve, reject) => {
          fixture.server.once("error", reject);
          fixture.server.listen(0, "127.0.0.1", resolve);
        }),
      catch: (cause) =>
        new SmokeError({ detail: "Could not start the local smoke endpoint", cause }),
    }),
    () =>
      Effect.promise(() => new Promise<void>((resolve) => fixture.server.close(() => resolve()))),
  );
  const address = fixture.server.address();
  if (!address || typeof address === "string")
    return yield* new SmokeError({ detail: "Missing local endpoint address" });
  const agentDir = `${directory}/agent`;
  yield* fs.makeDirectory(agentDir);
  yield* fs.writeFileString(
    `${agentDir}/models.json`,
    encodeJson({
      providers: {
        "t3-smoke": {
          baseUrl: `http://127.0.0.1:${address.port}/v1`,
          api: "openai-completions",
          apiKey: "local-smoke-key",
          models: [
            {
              id: "model",
              name: "Local smoke model",
              reasoning: false,
              input: ["text", "image"],
              contextWindow: 32000,
              maxTokens: 1024,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            },
          ],
        },
      },
    }),
  );
  // An allowlist prevents any workstation credentials or real Pi profile from reaching this process.
  const environment: NodeJS.ProcessEnv = {
    PATH: process.env.PATH,
    HOME: directory,
    USERPROFILE: directory,
    TMPDIR: directory,
    PI_CODING_AGENT_DIR: agentDir,
    PI_SKIP_VERSION_CHECK: "1",
  };
  const settings = decodeSettings({ binaryPath, agentDir, enabled: true });
  const adapter = yield* makePiAdapter(settings, { instanceId, environment });
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const collected: ProviderRuntimeEvent[] = [];
  yield* adapter.streamEvents.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        collected.push(event);
        yield* Queue.offer(queue, event);
      }),
    ),
    Effect.forkScoped,
  );
  yield* Effect.yieldNow;
  const next = (type: ProviderRuntimeEvent["type"]) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(queue);
        if (event.type === type) return event;
      }
    });
  return {
    ...fixture,
    mcpEndpoint: `http://127.0.0.1:${address.port}/mcp`,
    adapter,
    next,
    collected,
    settings,
    environment,
    directory,
    fs,
  };
});

describe.skipIf(!binaryPath)("Released Pi runtime", () => {
  it.effect(
    "connects T3's process-local MCP server using the native Pi extension",
    () =>
      Effect.gen(function* () {
        const h = yield* harness;
        h.setTool("mcp__t3_code__echo");
        setMcpProviderSession({
          environmentId: EnvironmentId.make("smoke-environment"),
          threadId,
          providerInstanceId: instanceId,
          providerSessionId: "smoke-session",
          endpoint: h.mcpEndpoint,
          authorizationHeader: "Bearer local-smoke",
          capabilities: new Set(["preview"]),
        });
        yield* Effect.addFinalizer(() => Effect.sync(() => clearMcpProviderSession(threadId)));
        yield* h.adapter.startSession({
          threadId,
          cwd: h.directory,
          runtimeMode: "full-access",
          modelSelection: { instanceId, model: "t3-smoke/model" },
        });
        yield* h.adapter.sendTurn({ threadId, input: "Say hello" });
        expect((yield* h.next("turn.completed")).payload).toMatchObject({ state: "completed" });
        expect(h.mcpMethods).toContain("initialize");
        expect(h.mcpMethods).toContain("tools/list");
        expect(h.mcpMethods).toContain("tools/call");
        expect(h.mcpAuthorization.every((header) => header === "Bearer local-smoke")).toBe(true);
        expect(
          h.collected.some(
            (event) =>
              event.type === "item.completed" && event.payload.itemType === "mcp_tool_call",
          ),
        ).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(layer)),
    { timeout: 60000 },
  );
  it.effect(
    "discovers models, streams a real turn, and resumes its durable session",
    () =>
      Effect.gen(function* () {
        const h = yield* harness;
        const snapshot = yield* checkPiProvider(h.settings, h.environment, h.directory);
        expect(snapshot.status).toBe("ready");
        expect(snapshot.models.some((model) => model.slug === "t3-smoke/model")).toBe(true);
        const session = yield* h.adapter.startSession({
          threadId,
          cwd: h.directory,
          runtimeMode: "full-access",
          modelSelection: { instanceId, model: "t3-smoke/model" },
        });
        yield* h.adapter.sendTurn({ threadId, input: "Say hello" });
        expect((yield* h.next("turn.completed")).payload).toMatchObject({ state: "completed" });
        expect(
          h.collected
            .filter((event) => event.type === "content.delta")
            .map((event) => event.payload.delta)
            .join(""),
        ).toBe("Native Pi response");
        const history = yield* Effect.promise(() =>
          readTranscriptRecords(decodeCursor(session.resumeCursor).sessionFile, "pi"),
        );
        expect(history?.records[0]?.totals).toMatchObject({
          uncachedInputTokens: 20,
          outputTokens: 5,
        });
        yield* h.adapter.stopSession(threadId);
        expect(
          (yield* h.adapter.startSession({
            threadId,
            cwd: h.directory,
            runtimeMode: "full-access",
            resumeCursor: session.resumeCursor,
          })).resumeCursor,
        ).toEqual(session.resumeCursor);
      }).pipe(Effect.scoped, Effect.provide(layer)),
    { timeout: 60000 },
  );

  it.effect(
    "gates a real bash tool call on T3 approval",
    () =>
      Effect.gen(function* () {
        const h = yield* harness;
        h.setTool();
        yield* h.adapter.startSession({
          threadId,
          cwd: h.directory,
          runtimeMode: "approval-required",
          modelSelection: { instanceId, model: "t3-smoke/model" },
        });
        yield* h.adapter.sendTurn({ threadId, input: "Run the test command" });
        const request = yield* h.next("request.opened");
        expect(yield* h.fs.exists(`${h.directory}/native-tool.txt`)).toBe(false);
        yield* h.adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(request.requestId!),
          "accept",
        );
        expect((yield* h.next("turn.completed")).payload).toMatchObject({ state: "completed" });
        expect(yield* h.fs.readFileString(`${h.directory}/native-tool.txt`)).toBe("pi-smoke");
      }).pipe(Effect.scoped, Effect.provide(layer)),
    { timeout: 60000 },
  );

  it.effect(
    "blocks writes in plan mode and generates structured text without tools",
    () =>
      Effect.gen(function* () {
        const h = yield* harness;
        h.setTool();
        yield* h.adapter.startSession({
          threadId,
          cwd: h.directory,
          runtimeMode: "full-access",
          modelSelection: { instanceId, model: "t3-smoke/model" },
        });
        yield* h.adapter.sendTurn({ threadId, input: "Plan the test", interactionMode: "plan" });
        expect((yield* h.next("turn.completed")).payload).toMatchObject({ state: "completed" });
        expect(yield* h.fs.exists(`${h.directory}/native-tool.txt`)).toBe(false);
        h.setReply('{"title":"Native Pi smoke"}');
        expect(
          h.collected.find((event) => event.type === "turn.proposed.completed")?.payload,
        ).toMatchObject({ planMarkdown: "Native Pi response" });
        const generation = yield* makePiTextGeneration(h.settings, h.environment);
        expect(
          yield* generation.generateThreadTitle({
            cwd: h.directory,
            message: "Hello",
            modelSelection: { instanceId, model: "t3-smoke/model" },
          }),
        ).toMatchObject({ title: "Native Pi smoke" });
      }).pipe(Effect.scoped, Effect.provide(layer)),
    { timeout: 60000 },
  );
});
