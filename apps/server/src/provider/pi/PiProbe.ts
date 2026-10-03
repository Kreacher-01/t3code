import { ProviderDriverKind, type PiSettings, type ServerProviderModel } from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ChildProcess } from "effect/unstable/process";
import { ProviderAdapterRequestError } from "../Errors.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
} from "../providerSnapshot.ts";
import { piEnvironment } from "./PiModelSelection.ts";
import { PiModels, PiState, piModelSlug } from "./PiProtocol.ts";
import { makePiRpcClient } from "./PiRpcClient.ts";

/** Health and text helpers must not load user hooks, execute tools, or save conversations. */
export const PI_ISOLATED_RPC_ARGS = [
  "--mode",
  "rpc",
  "--no-session",
  "--no-tools",
  "--no-extensions",
  "--no-skills",
  "--no-prompt-templates",
  "--no-context-files",
  "--no-themes",
  "--no-approve",
] as const;
const decodeModels = Schema.decodeUnknownEffect(PiModels);
const decodeState = Schema.decodeUnknownEffect(PiState);
const PROVIDER = ProviderDriverKind.make("pi");
const reasoning = {
  optionDescriptors: [
    buildSelectOptionDescriptor({
      id: "reasoningEffort",
      label: "Thinking",
      options: ["off", "minimal", "low", "medium", "high"].map((value) => ({
        value,
        label: value,
      })),
    }),
  ],
};

export function piModels(
  models: (typeof PiModels.Type)["models"],
  state: typeof PiState.Type,
  settings: PiSettings,
): ReadonlyArray<ServerProviderModel> {
  const current = state.model ? piModelSlug(state.model) : undefined;
  return providerModelsFromSettings(
    models.map((model) => ({
      slug: piModelSlug(model),
      name: model.name,
      subProvider: model.provider,
      isCustom: false,
      isDefault: piModelSlug(model) === current,
      capabilities: model.reasoning ? reasoning : {},
    })),
    settings.customModels,
    {},
  );
}

export const makePiSnapshot = Effect.fn("makePiSnapshot")(function* (
  settings: PiSettings,
  probe: ProviderProbeResult,
  models: ReadonlyArray<ServerProviderModel> = [],
) {
  return {
    ...buildServerProvider({
      driver: PROVIDER,
      presentation: {
        displayName: "Pi",
        reportsContextWindow: true,
        showInteractionModeToggle: true,
        requiresNewThreadForModelChange: false,
        supportsConversationRollback: false,
      },
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models,
      probe,
    }),
    supportsTextGeneration: true,
    setup: { canAuthenticate: false, canInstall: false },
  };
});

export const checkPiProvider = Effect.fn("checkPiProvider")(function* (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
) {
  if (!settings.enabled)
    return yield* makePiSnapshot(settings, {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
    });
  const env = piEnvironment(settings, environment);
  const version = yield* resolveSpawnCommand(settings.binaryPath, ["--version"], { env }).pipe(
    Effect.flatMap((command) =>
      spawnAndCollect(
        settings.binaryPath,
        ChildProcess.make(command.command, command.args, { env, cwd, shell: command.shell }),
      ),
    ),
    Effect.map((result) => (result.code === 0 ? parseGenericCliVersion(result.stdout) : null)),
    Effect.orElseSucceed(() => null),
  );
  if (!version)
    return yield* makePiSnapshot(settings, {
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message:
        "Pi was not found. Install @earendil-works/pi-coding-agent and check the binary path.",
    });
  if (Number(version.split(".")[0]) < 1)
    return yield* makePiSnapshot(settings, {
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message: "Pi 1.0.0 or later is required for the T3 integration.",
    });
  return yield* Effect.gen(function* () {
    const rpc = yield* makePiRpcClient({
      command: settings.binaryPath,
      args: PI_ISOLATED_RPC_ARGS,
      cwd,
      env,
    });
    const catalog = yield* rpc.request("get_available_models").pipe(Effect.flatMap(decodeModels));
    const state = yield* rpc.request("get_state").pipe(Effect.flatMap(decodeState));
    const models = piModels(catalog.models, state, settings);
    return yield* makePiSnapshot(
      settings,
      {
        installed: true,
        version,
        status: models.length ? "ready" : "warning",
        auth: {
          status: catalog.models.length
            ? "authenticated"
            : models.length
              ? "unknown"
              : "unauthenticated",
          label: "Pi model credentials",
        },
        ...(!catalog.models.length
          ? {
              message: models.length
                ? "Custom model credentials will be checked when Pi starts a session."
                : "No authenticated Pi models were found. Run pi and configure /login, API keys, or models.json, then refresh.",
            }
          : {}),
      },
      models,
    );
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: "20 seconds",
      orElse: () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "probe",
            detail: "Pi model discovery timed out.",
          }),
        ),
    }),
    Effect.catch(() =>
      makePiSnapshot(settings, {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi model discovery failed. Check your Pi configuration and refresh.",
      }),
    ),
  );
});
