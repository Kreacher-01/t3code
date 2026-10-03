import type { ModelSelection, PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { ProviderAdapterValidationError } from "../Errors.ts";
import { splitPiModelSlug } from "./PiProtocol.ts";
import type { PiRpcClient } from "./PiRpcClient.ts";

export function piEnvironment(settings: PiSettings, env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const agentDir = settings.agentDir.startsWith("~/")
    ? `${env.HOME ?? env.USERPROFILE}/${settings.agentDir.slice(2)}`
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
  if (level) yield* rpc.request("set_thinking_level", { level });
});
