import type { RuntimeMode } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { McpProviderSessionConfig } from "../../mcp/McpProviderSession.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** A process-local extension: no user or project Pi configuration is overwritten. */
export function buildPiExtension(input: {
  readonly runtimeMode: RuntimeMode;
  readonly mcp?: McpProviderSessionConfig;
}) {
  const config = encodeJson({
    runtimeMode: input.runtimeMode,
    instructions: buildRuntimeInstructions({ harness: "Pi" }),
    mcp: input.mcp
      ? { url: input.mcp.endpoint, headers: { Authorization: input.mcp.authorizationHeader } }
      : null,
  });
  return `export default function(pi) {
    const config = ${config};
    let planning = false;
    const reads = new Set(["read", "grep", "find", "ls"]);
    if (config.mcp) pi.registerMcpServer("t3_code", { ...config.mcp, exposure: "direct" });
    pi.on("session_start", (_event, ctx) => ctx.ui.notify("t3-pi-ready", "info"));
    pi.registerCommand("t3-mode", { description: "Set T3 interaction mode", handler: async (args) => {
      planning = args.trim() === "plan";
    }});
    pi.on("before_agent_start", (event) => ({ systemPrompt: event.systemPrompt + "\\n\\n" + config.instructions
      + (planning ? "\\nPlan the requested work. Do not modify files or run commands." : "") }));
    pi.on("tool_call", async (event, ctx) => {
      if (planning && !reads.has(event.toolName)) return { block: true, reason: "Tool is unavailable in plan mode." };
      if (reads.has(event.toolName) || config.runtimeMode === "full-access") return;
      if (config.runtimeMode === "auto-accept-edits" && ["edit", "write"].includes(event.toolName)) return;
      const allowed = await ctx.ui.confirm("T3 approval: " + event.toolName, JSON.stringify(event.input));
      if (!allowed) return { block: true, reason: "Tool call declined in T3 Code." };
    });
  }`;
}
