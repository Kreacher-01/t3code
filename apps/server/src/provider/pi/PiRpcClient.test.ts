import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { makePiRpcClient } from "./PiRpcClient.ts";

const script = `
let buffer = '';
const requests = [];
process.stdin.on('data', (chunk) => {
  buffer += chunk;
  let boundary;
  while ((boundary = buffer.indexOf('\\n')) !== -1) {
    const command = JSON.parse(buffer.slice(0, boundary));
    buffer = buffer.slice(boundary + 1);
    if (command.type === 'exit') process.exit(7);
    if (command.type === 'reject') {
      process.stdout.write(JSON.stringify({type:'response', id:command.id, command:command.type,
        success:false, error:'Unsupported model'}) + '\\n');
      continue;
    }
    requests.push(command);
    if (requests.length === 2) {
      process.stdout.write(JSON.stringify({type:'agent_settled'}) + '\\n');
      for (const request of requests.reverse()) process.stdout.write(JSON.stringify({
        type:'response', id:request.id, command:request.type, success:true, data:request.value,
      }) + '\\n');
    }
  }
});`;
const client = () =>
  makePiRpcClient({
    command: process.execPath,
    args: ["-e", script],
    cwd: process.cwd(),
    env: process.env,
  });
describe("Pi RPC subprocess", () => {
  it.effect("correlates concurrent replies and preserves interleaved events", () =>
    Effect.gen(function* () {
      const rpc = yield* client();
      const [first, second, event] = yield* Effect.all(
        [
          rpc.request("first", { value: "one" }),
          rpc.request("second", { value: "two" }),
          Stream.runHead(rpc.events),
        ],
        { concurrency: "unbounded" },
      );
      expect(first).toBe("one");
      expect(second).toBe("two");
      expect(event).toMatchObject({ value: { type: "agent_settled" } });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("returns native request errors", () =>
    Effect.gen(function* () {
      const rpc = yield* client();
      const error = yield* Effect.flip(rpc.request("reject"));
      expect(error.detail).toBe("Unsupported model");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("fails pending requests when the child exits", () =>
    Effect.gen(function* () {
      const rpc = yield* client();
      const error = yield* Effect.flip(rpc.request("exit"));
      expect(error.detail).toContain("Pi exited (7)");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
