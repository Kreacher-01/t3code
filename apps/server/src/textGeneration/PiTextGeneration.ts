import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { type PiSettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { applyPiModelSelection, piEnvironment } from "../provider/pi/PiModelSelection.ts";
import { PI_ISOLATED_RPC_ARGS } from "../provider/pi/PiProbe.ts";
import { piMessageText } from "../provider/pi/PiProtocol.ts";
import { makePiRpcClient } from "../provider/pi/PiRpcClient.ts";

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runPiJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const rpc = yield* makePiRpcClient({
        command: settings.binaryPath,
        args: PI_ISOLATED_RPC_ARGS,
        cwd,
        env: piEnvironment(settings, environment),
      });
      const settled = yield* Deferred.make<void, TextGenerationError>();
      let output = "";
      yield* rpc.events.pipe(
        Stream.runForEach((record) =>
          Effect.gen(function* () {
            if (record.type === "t3_process_exit")
              yield* Deferred.fail(
                settled,
                new TextGenerationError({
                  operation,
                  detail: record.error ?? "Pi process stopped.",
                }),
              );
            const message = typeof record.message === "string" ? undefined : record.message;
            if (record.type === "message_end" && message?.role === "assistant") {
              if (message.stopReason === "error" || message.stopReason === "aborted")
                yield* Deferred.fail(
                  settled,
                  new TextGenerationError({
                    operation,
                    detail: message.errorMessage || "Pi text generation failed.",
                  }),
                );
              output = piMessageText(message);
            }
            if (record.type === "agent_settled") yield* Deferred.succeed(settled, undefined);
          }),
        ),
        Effect.forkScoped,
      );
      yield* applyPiModelSelection(rpc, modelSelection);
      yield* rpc.request("prompt", { message: prompt });
      yield* Deferred.await(settled);
      if (!output.trim())
        return yield* new TextGenerationError({ operation, detail: "Pi returned empty output." });
      return yield* Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson))(
        extractJsonObject(output),
      );
    }).pipe(
      Effect.timeoutOrElse({
        duration: "3 minutes",
        orElse: () =>
          Effect.fail(
            new TextGenerationError({ operation, detail: "Pi text generation timed out." }),
          ),
      }),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Pi structured text generation failed.",
            cause,
          }),
      ),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.scoped,
    );
  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        linkedContext: input.linkedContext,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
        ...(generated.needsRefinement ? { needsRefinement: true } : {}),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
