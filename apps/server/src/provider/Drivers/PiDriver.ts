import { PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makePiTextGeneration } from "../../textGeneration/PiTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { makePiAdapter } from "../pi/PiAdapter.ts";
import { checkPiProvider, makePiSnapshot } from "../pi/PiProbe.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
} from "../providerUpdateSettings.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const DRIVER = ProviderDriverKind.make("pi");
const decodeSettings = Schema.decodeSync(PiSettings);
export type PiDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER,
  metadata: { displayName: "Pi", supportsMultipleInstances: true },
  configSchema: PiSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverSettings = yield* ServerSettingsService;
      const { cwd } = yield* ServerConfig;
      const loggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const settings = { ...config, enabled };
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER,
        instanceId,
      });
      const identity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const adapter = yield* makePiAdapter(settings, {
        instanceId,
        environment: processEnv,
        ...(loggers.native ? { nativeEventLogger: loggers.native } : {}),
      });
      const textGeneration = yield* makePiTextGeneration(settings, processEnv);
      const snapshotSettings = makeProviderSnapshotSettingsSource(settings, serverSettings);
      const snapshot = yield* makeManagedServerProvider({
        ...snapshotSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({
              provider: DRIVER,
              packageName: "@earendil-works/pi-coding-agent",
            }),
          ),
        initialSnapshot: () =>
          makePiSnapshot(settings, {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
          }).pipe(Effect.map(identity)),
        checkProvider: checkPiProvider(settings, processEnv, cwd).pipe(
          Effect.map(identity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER,
              instanceId,
              detail: "Could not build the Pi provider snapshot.",
              cause,
            }),
        ),
      );
      return {
        instanceId,
        driverKind: DRIVER,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
        refreshModels: () =>
          snapshot.refresh.pipe(
            Effect.asVoid,
            Effect.mapError(
              (cause) =>
                new ProviderDriverError({
                  driver: DRIVER,
                  instanceId,
                  detail: "Could not refresh Pi models.",
                  cause,
                }),
            ),
          ),
      } satisfies ProviderInstance;
    }),
};
