import {
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  RuntimeItemId,
  RuntimeRequestId,
  TurnId,
  type CanonicalItemType,
  type ModelSelection,
  type PiSettings,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  readMcpProviderSession,
  withAgentDeviceEnvironment,
} from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { EventNdjsonLogger } from "../Layers/EventNdjsonLogger.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { buildPiExtension } from "./PiExtension.ts";
import { applyPiModelSelection, piEnvironment } from "./PiModelSelection.ts";
import {
  PiCompactionResult,
  PiPromptResult,
  PiState,
  piModelSlug,
  piMessageText,
  type PiRecord,
} from "./PiProtocol.ts";
import { makePiRpcClient, type PiRpcClient } from "./PiRpcClient.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const ResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  sessionFile: Schema.NonEmptyString,
});
const decodeResume = Schema.decodeUnknownOption(ResumeCursor);
const decodeState = Schema.decodeUnknownEffect(PiState);
const isValidationError = Schema.is(ProviderAdapterValidationError);
const isRequestError = Schema.is(ProviderAdapterRequestError);
const decodePromptResult = Schema.decodeUnknownEffect(PiPromptResult);
const decodeCompaction = Schema.decodeUnknownOption(PiCompactionResult);
const decodeAnswer = Schema.decodeUnknownOption(
  Schema.Union([Schema.String, Schema.Array(Schema.String)]),
);
const decodeToolContent = Schema.decodeUnknownOption(
  Schema.Struct({
    content: Schema.Array(
      Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) }),
    ),
  }),
);
const decodeToolArgs = Schema.decodeUnknownOption(
  Schema.Struct({ command: Schema.optional(Schema.String), path: Schema.optional(Schema.String) }),
);
type EventBody<E = ProviderRuntimeEvent> = E extends ProviderRuntimeEvent
  ? Omit<E, "eventId" | "provider" | "providerInstanceId" | "threadId" | "createdAt">
  : never;
interface MessageItem {
  readonly id: RuntimeItemId;
  readonly type: "assistant_message" | "reasoning" | "plan";
  text: string;
}
interface PendingDialog {
  readonly native: PiRecord;
  readonly approval: boolean;
  readonly turnId: TurnId | undefined;
}
interface SessionContext {
  readonly scope: Scope.Closeable;
  readonly rpc: PiRpcClient;
  readonly lock: Semaphore.Semaphore;
  readonly ready: Deferred.Deferred<void, ProviderAdapterRequestError>;
  drain: Deferred.Deferred<void, ProviderAdapterRequestError>;
  draining: boolean;
  readonly requests: Map<ApprovalRequestId, PendingDialog>;
  readonly items: Map<number, MessageItem>;
  readonly tools: Map<string, { readonly name: string; readonly args: unknown }>;
  readonly turns: Array<{ id: TurnId; items: unknown[] }>;
  session: ProviderSession;
  turnId: TurnId | undefined;
  messageIndex: number;
  plan: boolean;
  stopped: boolean;
  interrupted: boolean;
  error: string | undefined;
  usage: {
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
    cost: number;
    available: boolean;
  };
  maxTokens: number | undefined;
}

function toolType(name: string): CanonicalItemType {
  return name === "bash"
    ? "command_execution"
    : ["write", "edit"].includes(name)
      ? "file_change"
      : name.startsWith("mcp_")
        ? "mcp_tool_call"
        : "dynamic_tool_call";
}
function toolText(value: unknown): string {
  const content = decodeToolContent(value);
  return Option.isSome(content)
    ? content.value.content.map((part) => part.text ?? "").join("\n")
    : "";
}

/** One scoped native RPC process per T3 thread; orchestration remains provider independent. */
export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  settings: PiSettings,
  options: {
    readonly instanceId: ProviderInstanceId;
    readonly environment: NodeJS.ProcessEnv;
    readonly nativeEventLogger?: EventNdjsonLogger;
  },
) {
  const crypto = yield* Crypto.Crypto;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const config = yield* ServerConfig;
  const sessions = new Map<ThreadId, SessionContext>();
  const startLock = yield* Semaphore.make(1);
  const events = yield* Effect.acquireRelease(
    PubSub.unbounded<ProviderRuntimeEvent>(),
    PubSub.shutdown,
  );
  const now = Effect.map(DateTime.now, DateTime.formatIso);
  const error = (method: string, detail: string, cause?: unknown) =>
    new ProviderAdapterRequestError({
      provider: PROVIDER,
      method,
      detail,
      ...(cause ? { cause } : {}),
    });
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) => error("randomUUID", "Could not create a Pi identifier.", cause)),
  );
  const sessionDir = path.join(
    config.stateDir,
    "pi",
    encodeURIComponent(options.instanceId),
    "sessions",
  );
  const emit = (context: SessionContext, body: EventBody) =>
    Effect.gen(function* () {
      yield* PubSub.publish(events, {
        ...body,
        eventId: EventId.make(yield* randomId),
        createdAt: yield* now,
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        threadId: context.session.threadId,
        turnId: body.turnId ?? context.turnId,
      });
    });
  const requireSession = (threadId: ThreadId) =>
    Effect.suspend(() => {
      const context = sessions.get(threadId);
      return context && !context.stopped
        ? Effect.succeed(context)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    });
  const validateSelection = (selection: ModelSelection | undefined) =>
    selection && selection.instanceId !== options.instanceId
      ? Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "setModel",
            issue: "The selected model belongs to another Pi instance.",
          }),
        )
      : Effect.void;
  const setModel = (context: SessionContext, selection: ModelSelection | undefined) =>
    Effect.gen(function* () {
      yield* validateSelection(selection);
      yield* applyPiModelSelection(context.rpc, selection);
      if (selection)
        context.session = { ...context.session, model: selection.model, updatedAt: yield* now };
      const state = yield* context.rpc.request("get_state").pipe(
        Effect.flatMap(decodeState),
        Effect.mapError((cause) => error("get_state", "Could not read Pi session state.", cause)),
      );
      context.maxTokens = state.model?.contextWindow;
      context.session = {
        ...context.session,
        ...(state.model ? { model: piModelSlug(state.model) } : {}),
      };
    });
  const resolveDialog = (
    context: SessionContext,
    requestId: ApprovalRequestId,
    response: Record<string, unknown>,
    answer?: Record<string, unknown>,
  ) =>
    Effect.gen(function* () {
      const dialog = context.requests.get(requestId);
      if (!dialog) return;
      context.requests.delete(requestId);
      yield* context.rpc.write({
        type: "extension_ui_response",
        id: dialog.native.id,
        ...response,
      });
      if (dialog.approval)
        yield* emit(context, {
          type: "request.resolved",
          turnId: dialog.turnId,
          requestId: RuntimeRequestId.make(requestId),
          payload: {
            requestType: "permission_approval",
            decision: response.confirmed ? "accept" : "decline",
          },
        });
      else
        yield* emit(context, {
          type: "user-input.resolved",
          turnId: dialog.turnId,
          requestId: RuntimeRequestId.make(requestId),
          payload: { answers: answer ?? {} },
        });
    });
  const cancelDialogs = (context: SessionContext) =>
    Effect.forEach(
      [...context.requests.keys()],
      (id) => resolveDialog(context, id, { cancelled: true }).pipe(Effect.ignore),
      { discard: true },
    );
  const finishTurn = (context: SessionContext, failure?: string) =>
    Effect.gen(function* () {
      const turnId = context.turnId;
      if (!turnId) return;
      // Claim settlement before yielding: a rejected prompt and a process exit can race.
      context.turnId = undefined;
      yield* cancelDialogs(context);
      const usage = context.usage;
      const errorMessage = failure ?? context.error;
      for (const item of context.items.values())
        yield* emit(context, {
          type: "item.completed",
          turnId,
          itemId: item.id,
          payload: {
            itemType: item.type,
            status: context.interrupted || failure ? "failed" : "completed",
          },
        });
      for (const [id, tool] of context.tools)
        yield* emit(context, {
          type: "item.completed",
          turnId,
          itemId: RuntimeItemId.make(id),
          payload: {
            itemType: toolType(tool.name),
            title: tool.name,
            status: context.interrupted ? "declined" : "failed",
          },
        });
      yield* emit(context, {
        type: "turn.completed",
        turnId,
        payload: {
          state: context.interrupted ? "interrupted" : errorMessage ? "failed" : "completed",
          ...(errorMessage ? { errorMessage } : {}),
          ...(usage.available ? { totalCostUsd: usage.cost } : {}),
          tokenUsage: usage.available
            ? {
                usageScope: "main_agent",
                usageStatus: "complete",
                hasSubagents: false,
                inputTokens: usage.input + usage.cacheRead + usage.cacheWrite,
                outputTokens: usage.output,
                cachedInputTokens: usage.cacheRead,
                cacheCreationTokens: usage.cacheWrite,
              }
            : { usageScope: "main_agent", usageStatus: "unavailable", hasSubagents: false },
        },
      });
      context.items.clear();
      context.tools.clear();
      context.session = {
        ...context.session,
        activeTurnId: undefined,
        status: failure ? "error" : "ready",
        updatedAt: yield* now,
        ...(errorMessage ? { lastError: errorMessage } : { lastError: undefined }),
      };
      yield* emit(context, {
        type: "session.state.changed",
        payload: { state: failure ? "error" : "ready" },
      });
      yield* Deferred.succeed(context.drain, undefined);
    });
  const stopContext = (context: SessionContext) =>
    Effect.gen(function* () {
      if (context.stopped) return;
      context.stopped = true;
      context.interrupted = true;
      yield* cancelDialogs(context);
      yield* finishTurn(context);
      yield* Scope.close(context.scope, Exit.void);
      if (sessions.get(context.session.threadId) === context)
        sessions.delete(context.session.threadId);
      yield* emit(context, { type: "session.exited", payload: { exitKind: "graceful" } });
    }).pipe(Effect.uninterruptible);
  const stopAll = () =>
    startLock
      .withPermit(Effect.forEach([...sessions.values()], stopContext, { discard: true }))
      .pipe(Effect.ignore);
  yield* Effect.addFinalizer(stopAll);

  const textDelta = (
    context: SessionContext,
    index: number,
    type: MessageItem["type"],
    delta: string,
  ) =>
    Effect.gen(function* () {
      let item = context.items.get(index);
      if (!item) {
        item = {
          id: RuntimeItemId.make(`${context.turnId}:${context.messageIndex}:${index}`),
          type,
          text: "",
        };
        context.items.set(index, item);
        yield* emit(context, {
          type: "item.started",
          itemId: item.id,
          payload: { itemType: type, status: "inProgress" },
        });
      }
      item.text += delta;
      if (type === "plan") {
        yield* emit(context, { type: "turn.proposed.delta", payload: { delta } });
        return;
      }
      yield* emit(context, {
        type: "content.delta",
        itemId: item.id,
        payload: {
          streamKind: type === "reasoning" ? "reasoning_text" : "assistant_text",
          delta,
          contentIndex: index,
        },
      });
    });
  const handleRecord = (context: SessionContext, record: PiRecord) =>
    Effect.gen(function* () {
      if (context.stopped) return;
      if (options.nativeEventLogger)
        yield* options.nativeEventLogger.write(
          { source: "pi.rpc", payload: record },
          context.session.threadId,
        );
      if (record.type === "t3_process_exit") {
        const detail = record.error ?? "Pi process stopped.";
        yield* Deferred.fail(context.ready, error("startSession", detail));
        yield* Deferred.fail(context.drain, error("drain", detail));
        yield* finishTurn(context, detail);
        context.session = {
          ...context.session,
          status: "error",
          lastError: detail,
          updatedAt: yield* now,
        };
        yield* emit(context, {
          type: "session.exited",
          payload: { exitKind: "error", reason: detail, recoverable: true },
        });
        return;
      }
      if (record.type === "extension_ui_request") {
        if (record.method === "notify" && record.message === "t3-pi-ready") {
          yield* Deferred.succeed(context.ready, undefined);
          return;
        }
        if (!record.id || !["confirm", "select", "input", "editor"].includes(record.method ?? ""))
          return;
        // Scope to this process: native request IDs alone may collide across instances.
        const requestId = yield* importApprovalId(record.id);
        const approval = record.method === "confirm";
        context.requests.set(requestId, { native: record, approval, turnId: context.turnId });
        if (approval)
          yield* emit(context, {
            type: "request.opened",
            requestId: RuntimeRequestId.make(requestId),
            payload: {
              requestType: "permission_approval",
              detail:
                `${record.title ?? "Pi tool approval"}\n${typeof record.message === "string" ? record.message : ""}`.trim(),
              options: [
                { decision: "accept", label: "Allow once" },
                { decision: "decline", label: "Decline" },
              ],
            },
          });
        else
          yield* emit(context, {
            type: "user-input.requested",
            requestId: RuntimeRequestId.make(requestId),
            payload: {
              questions: [
                {
                  id: requestId,
                  header: "Pi",
                  question: record.title || "Pi requests input",
                  multiSelect: false,
                  allowCustomAnswer: record.method !== "select",
                  options: (record.options ?? [])
                    .filter(Boolean)
                    .map((value) => ({ label: value, value, description: "" })),
                },
              ],
            },
          });
        if (record.timeout && record.timeout > 0)
          yield* Effect.sleep(record.timeout).pipe(
            Effect.andThen(resolveDialog(context, requestId, { cancelled: true })),
            Effect.ignore,
            Effect.forkScoped,
          );
        return;
      }
      if (record.type === "session_info_changed" && record.name) {
        yield* emit(context, { type: "thread.metadata.updated", payload: { name: record.name } });
        return;
      }
      if (record.type === "compaction_end" && !record.aborted && !record.errorMessage) {
        const result = Option.getOrUndefined(decodeCompaction(record.result));
        yield* emit(context, {
          type: "thread.state.changed",
          payload: {
            state: "compacted",
            ...(result?.tokensBefore !== undefined ? { beforeTokens: result.tokensBefore } : {}),
            ...(result?.estimatedTokensAfter !== undefined
              ? { afterTokens: result.estimatedTokensAfter }
              : {}),
          },
        });
        return;
      }
      if (record.type === "auto_retry_start") context.error = undefined;
      if (!context.turnId) return;
      if (record.type === "agent_settled") {
        if (context.draining) yield* Deferred.succeed(context.drain, undefined);
        else yield* finishTurn(context);
        return;
      }
      const message = typeof record.message === "string" ? undefined : record.message;
      if (record.type === "message_start" && message?.role === "assistant") {
        context.messageIndex++;
        context.items.clear();
      }
      if (record.type === "message_update" && record.assistantMessageEvent?.delta) {
        const update = record.assistantMessageEvent;
        if (update.type === "text_delta" || update.type === "thinking_delta")
          yield* textDelta(
            context,
            update.contentIndex ?? 0,
            update.type === "thinking_delta"
              ? "reasoning"
              : context.plan
                ? "plan"
                : "assistant_message",
            update.delta ?? "",
          );
      }
      if (record.type === "message_end" && message?.role === "assistant") {
        const blocks =
          typeof message.content === "string"
            ? [{ type: "text", text: message.content }]
            : (message.content ?? []);
        for (const [index, block] of blocks.entries()) {
          const text = block.type === "thinking" ? block.thinking : block.text;
          if (text) {
            const streamed = context.items.get(index)?.text ?? "";
            if (text.startsWith(streamed) && text.length > streamed.length)
              yield* textDelta(
                context,
                index,
                block.type === "thinking"
                  ? "reasoning"
                  : context.plan
                    ? "plan"
                    : "assistant_message",
                text.slice(streamed.length),
              );
          }
        }
        for (const item of context.items.values())
          yield* emit(context, {
            type: "item.completed",
            itemId: item.id,
            payload: {
              itemType: item.type,
              status: message.stopReason === "error" ? "failed" : "completed",
            },
          });
        context.turns.at(-1)?.items.push(message);
        const planMarkdown =
          context.plan && message.stopReason === "stop" ? piMessageText(message).trim() : "";
        if (planMarkdown)
          yield* emit(context, { type: "turn.proposed.completed", payload: { planMarkdown } });
        context.items.clear();
        if (message.stopReason === "error")
          context.error = message.errorMessage || "Pi model request failed.";
        else if (message.stopReason === "aborted") context.interrupted = true;
        else context.error = undefined;
        if (message.usage) {
          const usage = message.usage;
          context.usage.input += usage.input;
          context.usage.output += usage.output;
          context.usage.cacheRead += usage.cacheRead;
          context.usage.cacheWrite += usage.cacheWrite;
          context.usage.cost += usage.cost?.total ?? 0;
          context.usage.available = true;
          yield* emit(context, {
            type: "thread.token-usage.updated",
            payload: {
              usage: {
                usedTokens: usage.totalTokens,
                ...(context.maxTokens ? { maxTokens: context.maxTokens } : {}),
                compactsAutomatically: true,
              },
            },
          });
        }
      }
      if (record.type.startsWith("tool_execution_") && record.toolCallId) {
        const id = record.toolCallId;
        if (record.type === "tool_execution_start")
          context.tools.set(id, { name: record.toolName ?? "tool", args: record.args });
        const tool = context.tools.get(id);
        const args = Option.getOrUndefined(decodeToolArgs(tool?.args ?? record.args));
        const name = record.toolName ?? tool?.name ?? "tool";
        const result =
          record.type === "tool_execution_update" ? record.partialResult : record.result;
        const detail = toolText(result);
        yield* emit(context, {
          type:
            record.type === "tool_execution_end"
              ? "item.completed"
              : record.type === "tool_execution_start"
                ? "item.started"
                : "item.updated",
          itemId: RuntimeItemId.make(id),
          payload: {
            itemType: toolType(name),
            title: name,
            status:
              record.type === "tool_execution_end"
                ? record.isError
                  ? "failed"
                  : "completed"
                : "inProgress",
            ...(detail ? { detail: detail.slice(0, 32000) } : {}),
            data: {
              toolCallId: id,
              toolName: name,
              command: args?.command,
              path: args?.path,
              rawInput: tool?.args ?? record.args,
              rawOutput: result,
            },
          },
        });
        if (record.type === "tool_execution_end") context.tools.delete(id);
      }
    });
  // Branding is a boundary operation, while native IDs stay opaque.
  const importApprovalId = (id: string) =>
    Effect.map(randomId, (prefix) => {
      return ApprovalRequestId.make(`${prefix}:${id}`);
    });

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    startLock.withPermit(
      Effect.gen(function* () {
        yield* validateSelection(input.modelSelection);
        if (input.sandboxMode && input.sandboxMode !== "danger-full-access")
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue:
              "Pi does not provide an OS sandbox. Use T3 tool approvals or configure an external sandbox.",
          });
        if (input.providerInstanceId && input.providerInstanceId !== options.instanceId)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The requested Pi instance does not match this adapter.",
          });
        const resume = decodeResume(input.resumeCursor);
        if (input.resumeCursor !== undefined && Option.isNone(resume))
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Invalid saved Pi session. Start a new thread.",
          });
        const resumeFile = Option.getOrUndefined(resume)?.sessionFile;
        if (
          resumeFile &&
          (path.dirname(path.resolve(resumeFile)) !== sessionDir || !resumeFile.endsWith(".jsonl"))
        )
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "The saved session does not belong to this Pi instance.",
          });
        const previous = sessions.get(input.threadId);
        if (previous) yield* stopContext(previous);
        const scope = yield* Scope.make("sequential");
        return yield* Effect.gen(function* () {
          yield* fs.makeDirectory(sessionDir, { recursive: true });
          if (resumeFile && !(yield* fs.exists(resumeFile)))
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "The saved Pi session file is missing. Start a new thread.",
            });
          const sessionFile = resumeFile ?? path.join(sessionDir, `${yield* randomId}.jsonl`);
          const extensionDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-extension-" });
          const extensionFile = path.join(extensionDir, "t3.mjs");
          const mcp = readMcpProviderSession(input.threadId);
          yield* fs.writeFileString(
            extensionFile,
            buildPiExtension({ runtimeMode: input.runtimeMode, ...(mcp ? { mcp } : {}) }),
          );
          yield* fs.chmod(extensionFile, 0o600);
          const cwd = path.resolve(input.cwd ?? config.cwd);
          const rpc = yield* makePiRpcClient({
            command: settings.binaryPath,
            args: [
              "--mode",
              "rpc",
              "--session-dir",
              sessionDir,
              "--session",
              sessionFile,
              "--extension",
              extensionFile,
            ],
            cwd,
            env: withAgentDeviceEnvironment(piEnvironment(settings, options.environment), mcp),
          });
          const createdAt = yield* now;
          const context: SessionContext = {
            scope,
            rpc,
            lock: yield* Semaphore.make(1),
            ready: yield* Deferred.make<void, ProviderAdapterRequestError>(),
            drain: yield* Deferred.make<void, ProviderAdapterRequestError>(),
            draining: false,
            requests: new Map(),
            items: new Map(),
            tools: new Map(),
            turns: [],
            messageIndex: 0,
            turnId: undefined,
            stopped: false,
            plan: false,
            interrupted: false,
            error: undefined,
            maxTokens: undefined,
            usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, available: false },
            session: {
              threadId: input.threadId,
              provider: PROVIDER,
              providerInstanceId: options.instanceId,
              status: "ready",
              cwd,
              runtimeMode: input.runtimeMode,
              createdAt,
              updatedAt: createdAt,
              resumeCursor: { schemaVersion: 1, sessionFile },
            },
          };
          yield* rpc.events.pipe(
            Stream.runForEach((record) => handleRecord(context, record)),
            Effect.catch((cause) =>
              Effect.gen(function* () {
                yield* Deferred.fail(
                  context.ready,
                  error("events", "Pi event processing failed.", cause),
                );
                yield* finishTurn(context, "Pi event processing failed.");
              }),
            ),
            Effect.forkScoped,
          );
          yield* Deferred.await(context.ready).pipe(
            Effect.timeoutOrElse({
              duration: "30 seconds",
              orElse: () =>
                Effect.fail(
                  error(
                    "startSession",
                    "Pi did not load the T3 integration extension. Pi 1.0.0 or later is required.",
                  ),
                ),
            }),
          );
          yield* setModel(context, input.modelSelection);
          sessions.set(input.threadId, context);
          yield* emit(context, {
            type: "session.started",
            payload: { resume: context.session.resumeCursor },
          });
          yield* emit(context, { type: "thread.started", payload: {} });
          yield* emit(context, { type: "session.state.changed", payload: { state: "ready" } });
          return context.session;
        }).pipe(
          Effect.provideService(Scope.Scope, scope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
          Effect.mapError((cause) =>
            isValidationError(cause) || isRequestError(cause)
              ? cause
              : error("startSession", "Could not initialize Pi session.", cause),
          ),
        );
      }),
    );
  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      return yield* context.lock.withPermit(
        Effect.gen(function* () {
          yield* validateSelection(input.modelSelection);
          const planning = input.interactionMode === "plan";
          const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
          const files: string[] = [];
          for (const attachment of input.attachments ?? []) {
            const attachmentPath = resolveAttachmentPath({
              attachmentsDir: config.attachmentsDir,
              attachment,
            });
            if (!attachmentPath)
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "sendTurn",
                issue: "Unsupported attachment type.",
              });
            if (attachment.type === "image") {
              const data = yield* fs
                .readFile(attachmentPath)
                .pipe(
                  Effect.mapError((cause) =>
                    error("attachments", "Could not read uploaded image.", cause),
                  ),
                );
              images.push({
                type: "image",
                data: Buffer.from(data).toString("base64"),
                mimeType: attachment.mimeType,
              });
            } else files.push(attachmentPath);
          }
          const message = [
            input.input ?? "",
            ...(files.length ? ["Attached files:", ...files] : []),
          ]
            .filter(Boolean)
            .join("\n");
          if (!message && !images.length)
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Pi requires a message or an attachment.",
            });
          if (!context.turnId && context.session.status === "running")
            yield* Deferred.await(context.drain);
          if (context.stopped)
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          if (context.turnId) {
            // Abort and drain before reprompting, preserving the T3 turn identity.
            // Native prompt(..., steer) can start a new run as the previous one settles.
            context.draining = true;
            yield* Effect.gen(function* () {
              yield* cancelDialogs(context);
              yield* context.rpc.request("clear_queue");
              yield* context.rpc.request("abort");
              yield* Deferred.await(context.drain).pipe(
                Effect.timeoutOrElse({
                  duration: "30 seconds",
                  orElse: () =>
                    Effect.fail(error("drain", "Pi did not settle after interruption.")),
                }),
              );
              for (const item of context.items.values())
                yield* emit(context, {
                  type: "item.completed",
                  itemId: item.id,
                  payload: { itemType: item.type, status: "failed" },
                });
              context.items.clear();
              for (const [id, tool] of context.tools)
                yield* emit(context, {
                  type: "item.completed",
                  itemId: RuntimeItemId.make(id),
                  payload: { itemType: toolType(tool.name), status: "declined", title: tool.name },
                });
              context.tools.clear();
              context.interrupted = false;
              context.error = undefined;
            }).pipe(
              Effect.tapError((cause) => finishTurn(context, cause.message)),
              Effect.ensuring(
                Effect.sync(() => {
                  context.draining = false;
                }),
              ),
            );
          }
          yield* setModel(context, input.modelSelection).pipe(
            Effect.tapError((cause) => finishTurn(context, cause.message)),
          );
          if (planning !== context.plan) {
            yield* context.rpc
              .request("prompt", { message: `/t3-mode ${planning ? "plan" : "default"}` })
              .pipe(Effect.tapError((cause) => finishTurn(context, cause.message)));
            context.plan = planning;
          }
          if (context.stopped)
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          const alreadyRunning = context.turnId !== undefined;
          const turnId = context.turnId ?? TurnId.make(yield* randomId);
          if (!alreadyRunning) {
            context.turnId = turnId;
            context.error = undefined;
            context.interrupted = false;
            context.usage = {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              cost: 0,
              available: false,
            };
            context.turns.push({ id: turnId, items: [] });
            context.session = {
              ...context.session,
              activeTurnId: turnId,
              status: "running",
              updatedAt: yield* now,
            };
            yield* emit(context, {
              type: "turn.started",
              payload: { model: context.session.model },
            });
            yield* emit(context, { type: "session.state.changed", payload: { state: "running" } });
          }
          context.drain = yield* Deferred.make<void, ProviderAdapterRequestError>();
          const result = yield* context.rpc
            .request("prompt", {
              message,
              ...(images.length ? { images } : {}),
            })
            .pipe(
              Effect.flatMap(decodePromptResult),
              Effect.mapError((cause) =>
                isRequestError(cause) ? cause : error("prompt", "Pi rejected the prompt.", cause),
              ),
              Effect.tapError((failure) => finishTurn(context, failure.detail)),
            );
          if (result.disposition === "handled") yield* finishTurn(context);
          return { threadId: input.threadId, turnId, resumeCursor: context.session.resumeCursor };
        }),
      );
    });
  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
    startSession,
    sendTurn,
    compaction: {
      type: "native",
      start: (threadId: ThreadId, selection?: ModelSelection) =>
        Effect.gen(function* () {
          const context = yield* requireSession(threadId);
          yield* context.lock.withPermit(
            Effect.gen(function* () {
              if (context.turnId)
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "compact",
                  issue: "Wait for the Pi turn to finish before compacting.",
                });
              yield* setModel(context, selection);
              yield* context.rpc.request("compact");
            }),
          );
        }),
    },
    interruptTurn: (threadId: ThreadId, turnId?: TurnId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        if (turnId && context.turnId !== turnId) return;
        context.interrupted = true;
        yield* cancelDialogs(context);
        yield* context.rpc.request("clear_queue");
        yield* context.rpc.request("abort");
      }),
    respondToRequest: (threadId, requestId, decision) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        if (!context.requests.get(requestId)?.approval)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "respondToRequest",
            issue: "Pi approval is no longer pending.",
          });
        if (!["accept", "decline", "cancel"].includes(decision))
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "respondToRequest",
            issue: "Pi supports allowing this call once or declining it.",
          });
        yield* resolveDialog(context, requestId, { confirmed: decision === "accept" });
      }),
    respondToUserInput: (threadId, requestId, answers) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const pending = context.requests.get(requestId);
        const answer = Option.getOrUndefined(decodeAnswer(answers[requestId]));
        const value = typeof answer === "string" ? answer : answer?.[0];
        if (
          !pending ||
          pending.approval ||
          value === undefined ||
          (pending.native.method === "select" && !pending.native.options?.includes(value))
        )
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "respondToUserInput",
            issue: "Choose a valid answer for the pending Pi question.",
          });
        yield* resolveDialog(context, requestId, { value }, answers);
      }),
    stopSession: (threadId) =>
      startLock.withPermit(requireSession(threadId).pipe(Effect.flatMap(stopContext))),
    stopAll,
    listSessions: () => Effect.sync(() => [...sessions.values()].map((context) => context.session)),
    hasSession: (threadId) =>
      Effect.sync(() => {
        const context = sessions.get(threadId);
        return context !== undefined && !context.stopped && context.session.status !== "error";
      }),
    readThread: (threadId) =>
      requireSession(threadId).pipe(Effect.map((context) => ({ threadId, turns: context.turns }))),
    rollbackThread: () =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Pi conversation rewind is not supported. Start a new thread instead.",
        }),
      ),
    streamEvents: Stream.fromPubSub(events),
  } satisfies ProviderAdapterShape<ProviderAdapterError>;
});
