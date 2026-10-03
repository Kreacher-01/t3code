import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { ProviderAdapterRequestError } from "../Errors.ts";
import { makePiJsonlDecoder, type PiRecord } from "./PiProtocol.ts";

const encodeRecord = Schema.encodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const isRequestError = Schema.is(ProviderAdapterRequestError);

export const makePiRpcClient = Effect.fn("makePiRpcClient")(function* (input: {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const error = (method: string, detail: string, cause?: unknown) =>
    new ProviderAdapterRequestError({
      provider: "pi",
      method,
      detail,
      ...(cause ? { cause } : {}),
    });
  const command = yield* resolveSpawnCommand(input.command, input.args, { env: input.env }).pipe(
    Effect.mapError((cause) => error("spawn", "Could not resolve the Pi executable.", cause)),
  );
  const child = yield* spawner
    .spawn(
      ChildProcess.make(command.command, command.args, {
        cwd: input.cwd,
        env: input.env,
        shell: command.shell,
      }),
    )
    .pipe(
      Effect.mapError((cause) =>
        error("spawn", "Could not start Pi. Check its binary path.", cause),
      ),
    );
  const outgoing = yield* Effect.acquireRelease(Queue.unbounded<Uint8Array>(), Queue.shutdown);
  const events = yield* Effect.acquireRelease(Queue.unbounded<PiRecord>(), Queue.shutdown);
  const pending = new Map<string, Deferred.Deferred<unknown, ProviderAdapterRequestError>>();
  let nextId = 0;
  let closed: ProviderAdapterRequestError | undefined;
  let stderr = "";
  const fail = (failure: ProviderAdapterRequestError) =>
    Effect.gen(function* () {
      if (closed) return;
      closed = failure;
      for (const reply of pending.values()) yield* Deferred.fail(reply, failure);
      pending.clear();
      yield* Queue.offer(events, { type: "t3_process_exit", error: failure.detail });
    });
  yield* Effect.addFinalizer(() => fail(error("close", "Pi session was closed.")));
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        stderr = (stderr + chunk).slice(-2048);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catch((cause) => fail(error("write", "Could not write to Pi.", cause))),
    Effect.forkScoped,
  );
  const decoder = makePiJsonlDecoder();
  yield* child.stdout.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Effect.gen(function* () {
        const records = yield* Effect.try({
          try: () => decoder.push(chunk),
          catch: (cause) => error("read", "Pi emitted an invalid RPC record.", cause),
        });
        for (const record of records) {
          if (record.type !== "response") {
            yield* Queue.offer(events, record);
            continue;
          }
          const reply = record.id ? pending.get(record.id) : undefined;
          if (!reply) continue;
          if (record.success === true) yield* Deferred.succeed(reply, record.data);
          else
            yield* Deferred.fail(
              reply,
              error(record.command ?? "request", record.error ?? "Pi rejected the request."),
            );
        }
      }),
    ),
    Effect.andThen(
      Effect.try({
        try: () => decoder.finish(),
        catch: (cause) => error("read", "Pi ended its RPC stream unexpectedly.", cause),
      }),
    ),
    Effect.mapError((cause) =>
      isRequestError(cause) ? cause : error("read", "Could not read Pi's RPC stream.", cause),
    ),
    Effect.catch(fail),
    Effect.forkScoped,
  );
  yield* child.exitCode.pipe(
    Effect.flatMap((code) =>
      fail(
        error(
          "exit",
          `Pi exited (${Number(code)}).${stderr ? " Check Pi's configuration and authentication." : ""}`,
        ),
      ),
    ),
    Effect.catch((cause) => fail(error("exit", "Pi process terminated unexpectedly.", cause))),
    Effect.forkScoped,
  );

  const write = (record: Readonly<Record<string, unknown>>) =>
    Effect.gen(function* () {
      if (closed) return yield* closed;
      const encoded = yield* Effect.try({
        try: () => encodeRecord(record),
        catch: (cause) => error("write", "Could not encode a Pi RPC record.", cause),
      });
      yield* Queue.offer(outgoing, new TextEncoder().encode(`${encoded}\n`));
    });
  const request = Effect.fn("PiRpcClient.request")(function* (
    type: string,
    fields: Readonly<Record<string, unknown>> = {},
  ) {
    if (closed) return yield* closed;
    const id = `t3-${++nextId}`;
    const reply = yield* Deferred.make<unknown, ProviderAdapterRequestError>();
    pending.set(id, reply);
    return yield* write({ ...fields, type, id }).pipe(
      Effect.andThen(Deferred.await(reply)),
      Effect.timeoutOrElse({
        duration: type === "compact" ? "5 minutes" : "30 seconds",
        orElse: () => Effect.fail(error(type, "Pi RPC request timed out.")),
      }),
      Effect.ensuring(
        Effect.sync(() => {
          pending.delete(id);
        }),
      ),
    );
  });
  return { request, write, events: Stream.fromQueue(events) };
});
export type PiRpcClient = Effect.Success<ReturnType<typeof makePiRpcClient>>;
