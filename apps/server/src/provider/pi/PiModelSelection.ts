import type { ModelSelection, PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { ProviderAdapterValidationError } from "../Errors.ts";
import { splitPiModelSlug } from "./PiProtocol.ts";
import type { PiRpcClient } from "./PiRpcClient.ts";
const decodeThinkingLevel = Schema.decodeUnknownEffect(
  Schema.Literals(["off", "minimal", "low", "medium", "high", "xhigh"]),
);

export function piEnvironment(settings: PiSettings, env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const home = env.HOME ?? env.USERPROFILE;
  const agentDir =
    home && settings.agentDir === "~"
      ? home
      : home && settings.agentDir.startsWith("~/")
        ? `${home}/${settings.agentDir.slice(2)}`
        : settings.agentDir;
  return { ...env, ...(agentDir ? { PI_CODING_AGENT_DIR: agentDir } : {}) };
}

export const applyPiModelSelection = Effect.fn("applyPiModelSelection")(function* (
  rpc: PiRpcClient,
  selection: ModelSelection | undefined,
) {
  if (!selection) return;
  if (selection.model !== "pi-default") {
    const model = yield* Effect.try({
      try: () => splitPiModelSlug(selection.model),
      catch: () =>
        new ProviderAdapterValidationError({
          provider: "pi",
          operation: "setModel",
          issue: "Select a Pi model using its provider/model identifier.",
        }),
    });
    yield* rpc.request("set_model", model);
  }
  const level = getModelSelectionStringOptionValue(selection, "reasoningEffort");
  if (level) {
    const selected = yield* decodeThinkingLevel(level).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterValidationError({
            provider: "pi",
            operation: "setThinkingLevel",
            issue: "Unknown Pi thinking level.",
            cause,
          }),
      ),
    );
    yield* rpc.request("set_thinking_level", { level: selected });
  }
});
