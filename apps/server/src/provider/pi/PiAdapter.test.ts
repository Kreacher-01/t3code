import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiSettings,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

const instanceId = ProviderInstanceId.make("pi-test");
const threadId = ThreadId.make("pi-thread");
const decodeSettings = Schema.decodeSync(PiSettings);
const isRuntimeEvent = Schema.is(ProviderRuntimeEvent);
const layer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-test-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);
const fixture = `
import fs from 'node:fs';
const args = process.argv.slice(2);
const file = args[args.indexOf('--session') + 1];
const send = (event) => process.stdout.write(JSON.stringify(event) + '\\n');
const state = { sessionId: 'native-session', sessionFile: file, thinkingLevel: 'off', model: { id: 'test/model', provider: 'test', name: 'Test model', reasoning: true, contextWindow: 10000 } };
send({type:'extension_ui_request',method:'notify',message:'t3-pi-ready'});
let buffer = '';
let pending;
const end = () => {
  send({type:'message_end',message:{role:'assistant',content:[{type:'text',text:'Hello world'}], stopReason:'stop',usage:{input:10,output:2,cacheRead:3,cacheWrite:4,totalTokens:19,cost:{total:0.01}}}});
  send({type:'agent_end'});
  send({type:'agent_settled'});
};
process.stdin.on('data', (chunk) => {
  buffer += chunk;
  let boundary;
  while ((boundary = buffer.indexOf('\\n')) !== -1) {
    const command = JSON.parse(buffer.slice(0, boundary));
    buffer = buffer.slice(boundary + 1);
    const reply = (data) => send({type:'response',id:command.id,command:command.type,success:true,data});
    if (command.type === 'get_state') reply(state);
    else if (command.type === 'set_model') { state.model.id = command.modelId; state.model.provider = command.provider; reply(state.model); }
    else if (command.type === 'set_thinking_level') { state.thinkingLevel = command.level; reply(); }
    else if (command.type === 'prompt') {
      if (command.message.startsWith('/t3-mode')) { reply({disposition:'handled'}); continue; }
      if (command.message === 'crash') process.exit(4);
      fs.writeFileSync(file, 'saved');
      reply({disposition:'started'});
      send({type:'message_start',message:{role:'assistant',content:[]}});
      send({type:'message_update',assistantMessageEvent:{type:'text_delta',delta:'Hello ',contentIndex:0}});
      if (command.message === 'approve' || command.message === 'input') {
        // A low-level end during retries must not settle T3's turn.
        send({type:'agent_end'});
        pending = true;
        send({type:'extension_ui_request',id:'dialog',method:command.message === 'approve' ? 'confirm' : 'select', title:'Run command?',message:'echo hello',options:['One','Two']});
      } else if (command.message !== 'wait') end();
    } else if (command.type === 'extension_ui_response' && pending) { pending = false; end(); }
    else if (command.type === 'clear_queue') reply({});
    else if (command.type === 'abort') { reply({}); send({type:'agent_settled'}); }
    else if (command.type === 'compact') { send({type:'compaction_end',reason:'manual',result:{tokensBefore:19,estimatedTokensAfter:4}}); reply({}); }
  }
});`;
const harness = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-fixture-" });
  const binaryPath = `${directory}/pi`;
  yield* fs.writeFileString(binaryPath, `#!${process.execPath}\n${fixture}`);
  yield* fs.chmod(binaryPath, 0o755);
  const adapter = yield* makePiAdapter(decodeSettings({ binaryPath }), {
    instanceId,
    environment: process.env,
  });
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const collected: ProviderRuntimeEvent[] = [];
  yield* adapter.streamEvents.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        expect(isRuntimeEvent(event)).toBe(true);
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
  return { adapter, collected, next, fs };
});

describe.skipIf(HostProcessPlatform.defaultValue() === "win32")("Pi adapter", () => {
  it.effect("streams complete messages and resumes the same native session file", () =>
    Effect.gen(function* () {
      const { adapter, next, collected } = yield* harness;
      const session = yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "test/test/model" },
      });
      expect(session.model).toBe("test/test/model");
      const turn = yield* adapter.sendTurn({ threadId, input: "hello" });
      const complete = yield* next("turn.completed");
      expect(complete.turnId).toBe(turn.turnId);
      expect(complete.payload).toMatchObject({
        state: "completed",
        totalCostUsd: 0.01,
        tokenUsage: { inputTokens: 17, outputTokens: 2 },
      });
      expect(
        collected
          .filter((event) => event.type === "content.delta")
          .map((event) => event.payload.delta)
          .join(""),
      ).toBe("Hello world");
      yield* adapter.stopSession(threadId);
      const resumed = yield* adapter.startSession({
        threadId,
        runtimeMode: "approval-required",
        resumeCursor: session.resumeCursor,
      });
      expect(resumed.resumeCursor).toEqual(session.resumeCursor);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("keeps approvals pending through agent_end and settles after the response", () =>
    Effect.gen(function* () {
      const { adapter, next, collected } = yield* harness;
      yield* adapter.startSession({ threadId, runtimeMode: "approval-required" });
      yield* adapter.sendTurn({ threadId, input: "approve" });
      const request = yield* next("request.opened");
      expect(collected.some((event) => event.type === "turn.completed")).toBe(false);
      expect((yield* adapter.listSessions())[0]?.status).toBe("running");
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(request.requestId!),
        "accept",
      );
      expect((yield* next("request.resolved")).payload).toMatchObject({ decision: "accept" });
      expect((yield* next("turn.completed")).payload).toMatchObject({ state: "completed" });
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("validates native choices and resolves extension questions", () =>
    Effect.gen(function* () {
      const { adapter, next } = yield* harness;
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "input" });
      const question = yield* next("user-input.requested");
      const requestId = ApprovalRequestId.make(question.requestId!);
      const invalid = yield* adapter
        .respondToUserInput(threadId, requestId, { [requestId]: "Wrong" })
        .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      expect(invalid?._tag).toBe("ProviderAdapterValidationError");
      yield* adapter.respondToUserInput(threadId, requestId, { [requestId]: ["Two"] });
      yield* next("user-input.resolved");
      expect((yield* next("turn.completed")).payload).toMatchObject({ state: "completed" });
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("interrupts active turns and reports native compaction", () =>
    Effect.gen(function* () {
      const { adapter, next } = yield* harness;
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const turn = yield* adapter.sendTurn({ threadId, input: "wait" });
      yield* adapter.interruptTurn(threadId, turn.turnId);
      expect((yield* next("turn.completed")).payload).toMatchObject({ state: "interrupted" });
      yield* adapter.compaction.start(threadId);
      expect((yield* next("thread.state.changed")).payload).toMatchObject({
        state: "compacted",
        beforeTokens: 19,
        afterTokens: 4,
      });
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("rejects foreign model selections and cursors before starting Pi", () =>
    Effect.gen(function* () {
      const { adapter } = yield* harness;
      const result = yield* adapter
        .startSession({
          threadId,
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionFile: "/tmp/another-instance.jsonl" },
        })
        .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      expect(result?._tag).toBe("ProviderAdapterValidationError");
      const selection = yield* adapter
        .startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: { instanceId: ProviderInstanceId.make("other"), model: "test/model" },
        })
        .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));
      expect(selection?._tag).toBe("ProviderAdapterValidationError");
      expect(yield* adapter.hasSession(threadId)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
