import * as ByteSize from "effect/ByteSize";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpIncomingMessage from "effect/unstable/http/HttpIncomingMessage";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";

import { ScheduledTaskService, WEBHOOK_ROUTE_PREFIX } from "./ScheduledTaskService.ts";

/** Largest request body a webhook accepts. The relay enforces the same cap. */
export const WEBHOOK_MAX_BODY_BYTES = 1024 * 1024;

const json = (status: number, body: Record<string, string>) =>
  HttpServerResponse.jsonUnsafe(body, { status });

/**
 * Public entry point for webhook tasks: `/api/hooks/:hookId/:token`. It is
 * unauthenticated by design; the token in the URL (and an optional signature)
 * is the credential, and the service checks both. It is reachable directly,
 * over the managed tunnel, or through the relay's stable `/v1/hooks/...` URL.
 */
const handleWebhook = (scheduledTasks: ScheduledTaskService["Service"]) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);
    if (Option.isNone(url)) return json(400, { error: "bad_request" });

    const [hookId, token, ...rest] = url.value.pathname
      .slice(`${WEBHOOK_ROUTE_PREFIX}/`.length)
      .split("/");
    if (!hookId || !token || rest.length > 0) return json(404, { error: "hook_not_found" });
    let taskId: string;
    try {
      taskId = decodeURIComponent(hookId);
    } catch {
      return json(404, { error: "hook_not_found" });
    }

    const contentLength = Number(request.headers["content-length"] ?? "0");
    if (!Number.isFinite(contentLength) || contentLength > WEBHOOK_MAX_BODY_BYTES) {
      return json(413, { error: "body_too_large" });
    }
    // Chunked requests carry no content-length, so the reader itself is capped.
    const body = yield* request.arrayBuffer.pipe(
      Effect.map((buffer) => new Uint8Array(buffer)),
      Effect.provideService(
        HttpIncomingMessage.MaxBodySize,
        ByteSize.bytes(WEBHOOK_MAX_BODY_BYTES),
      ),
      Effect.option,
    );
    if (Option.isNone(body)) return json(413, { error: "body_too_large_or_unreadable" });
    if (body.value.byteLength > WEBHOOK_MAX_BODY_BYTES)
      return json(413, { error: "body_too_large" });

    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries(request.headers)) {
      if (typeof value === "string") headers[name.toLowerCase()] = value;
    }

    const result = yield* scheduledTasks
      .triggerWebhook({
        hookId: taskId,
        token,
        method: request.method,
        path: `${WEBHOOK_ROUTE_PREFIX}/${hookId}`,
        query: url.value.search.replace(/^\?/, ""),
        headers,
        body: body.value,
        bodyText: new TextDecoder().decode(body.value),
      })
      .pipe(
        Effect.catch((error) =>
          Effect.logWarning("Webhook delivery failed", { error: error.message }).pipe(
            Effect.as({ _tag: "error" as const }),
          ),
        ),
      );

    switch (result._tag) {
      case "accepted":
        return json(202, { deliveryId: result.deliveryId });
      case "not_found":
        return json(404, { error: "hook_not_found" });
      case "rejected_signature":
        return json(401, { error: "invalid_signature" });
      case "disabled":
        return json(409, { error: "hook_disabled" });
      case "rate_limited":
        return json(429, { error: "rate_limited" });
      case "error":
        return json(500, { error: "internal_error" });
    }
  });

export const webhookRouteLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const router = yield* HttpRouter.HttpRouter;
    const handler = handleWebhook(yield* ScheduledTaskService);
    for (const method of ["POST", "PUT", "PATCH", "GET"] as const) {
      yield* router.add(method, `${WEBHOOK_ROUTE_PREFIX}/*`, handler);
    }
  }),
);
