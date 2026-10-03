import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { PiDriver } from "./PiDriver.ts";

const layer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-driver-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);
const noSpawner = ChildProcessSpawner.make(() =>
  Effect.die("Disabled Pi must not start a process"),
);

it.effect("keeps disabled Pi instances isolated and reports manual CLI maintenance", () =>
  Effect.gen(function* () {
    const make = (name: string) =>
      PiDriver.create({
        instanceId: ProviderInstanceId.make(name),
        displayName: name,
        enabled: false,
        environment: [],
        config: { ...PiDriver.defaultConfig(), agentDir: `/profiles/${name}` },
      });
    const first = yield* make("personal");
    const second = yield* make("work");
    expect((yield* first.snapshot.refresh).status).toBe("disabled");
    expect((yield* first.snapshot.getSnapshot).instanceId).toBe("personal");
    expect(first.continuationIdentity).not.toEqual(second.continuationIdentity);
    const maintenance = yield* first.snapshot.resolveMaintenance();
    expect(maintenance.packageName).toBe("@earendil-works/pi-coding-agent");
    expect(maintenance.update).toBeNull();
    yield* first.adapter.stopAll();
    expect(yield* second.adapter.listSessions()).toEqual([]);
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawner),
    Effect.scoped,
    Effect.provide(layer),
  ),
);
