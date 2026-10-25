# Moderation API Rate-Limit Errors: Five Tenant-Aware Handling Strategies for Healthtech

The operational constraint changes the design: an OpenAI moderation API rate limit cannot be allowed to turn one busy healthtech tenant's sales-call summary into a queue-wide outage. The practical choice is to put admission control, bounded retries, and per-tenant accounting around the moderation API boundary, then make the degraded result explicit in the CRM workflow.

Short answer: treat a rate-limit response as a scheduling signal, not as a request to retry harder. Charge every attempt to the tenant that caused it, honor server timing when it is available, cap exponential backoff with jitter, and stop after a small retry budget. If moderation still cannot run, keep the transcript out of automated CRM actions and route the item for review. That policy protects the SLO and preserves cost visibility at the same time.

Measure it twice.

## How should a moderation API handle rate-limit errors?

The useful unit is not the HTTP request. It is one call-summary job moving through transcription, summarization, moderation, and CRM action generation. A single job can produce several provider calls, and a retry can multiply that count. If the queue only records total requests, the platform team will know that moderation is busy but not which tenant is consuming the budget or which workflow is creating the pressure.

For this scenario, I would define two measurements before changing retry code: moderation attempts per tenant, and completed CRM actions per tenant. The first exposes pressure; the second tells us whether pressure is preventing useful work. Keep the dimensions bounded. A tenant identifier, response class, and workflow name are useful. Raw transcript text, customer names, and unbounded error strings do not belong in metrics.

The invariant is simple: no retry may bypass tenant admission control. A job that is allowed into the queue must still acquire capacity immediately before the moderation call, because queue age and provider capacity change while the job waits. That small placement decision prevents a healthy-looking worker pool from issuing a burst after a downstream limit clears.

That is the boundary.

## Why does a retry policy need a tenant budget?

A global retry loop is attractive because it is short. It is also blind. Suppose Tenant A submits a 20-minute campaign review while Tenant B submits a handful of ordinary calls. If every worker retries on the same schedule, the larger workload gets more opportunities to consume shared capacity, even when Tenant A is already responsible for the majority of failed attempts. The incident then appears to be a provider problem, although the fairness problem is local.

Use three budgets instead of one: a per-request attempt limit, a per-tenant in-flight limit, and a global concurrency limit. The request budget bounds damage from one job. The tenant limit gives cost and fairness a visible control point. The global limit protects the rest of the platform and leaves room for other healthtech workflows. These limits should be configuration, not constants hidden in a worker.

The buy-versus-build decision is narrower than “managed service or self-hosted model.” A managed moderation boundary may reduce maintenance work, while an in-house queue and limiter preserve accounting and failure policy. A self-hosted moderation component may improve control over scheduling, but it transfers capacity planning, model updates, and on-call responsibility to the platform team. For a team that owns the roadmap, the deciding question is which boundary makes per-tenant cost auditable without adding an unmeasured operational obligation.

| Control surface | Buy-oriented default | Build-oriented default | Failure to watch |
| --- | --- | --- | --- |
| Admission | Provider quota plus local cap | Local token bucket with measured capacity | Bursts pass before accounting |
| Recovery | Provider timing hint plus bounded backoff | Queue rescheduling with durable attempt state | Workers sleep while holding slots |
| Accounting | Provider usage export where available | Per-tenant attempt ledger | Retries disappear into aggregate cost |
| Review path | Explicit moderation-pending state | Durable manual-review queue | CRM action runs on an unmoderated result |

This is the part I initially underestimated in systems like this: “retryable” is a property of the failure, not permission to repeat indefinitely. A rate limit can clear while a retry budget remains, but a budget can also expire while the business value of the job is still real. Those are separate decisions.

No exceptions.

Five minutes is a useful example of the trade-off: a retry schedule can preserve a batch job's value, but the same schedule can violate an interactive CRM SLO. The deadline must therefore be part of the job policy, not an afterthought in the HTTP client.

## How should the failure path behave?

Classify the response before deciding what to do. A rate-limit response is usually retryable; malformed input, an authentication failure, and a policy rejection need different handling. The classifier should return a small internal enum, not expose provider-specific text to every caller. Preserve the original status and request identifier in structured logs, with transcript content excluded.

The retry delay should use a server-provided delay when it is valid and within the local maximum. Otherwise, use exponential backoff with full jitter. Do not let a worker hold a tenant or global concurrency slot while waiting. Release the slot, persist the next eligible time, and re-enqueue the job. Otherwise, a small number of sleeping workers can make the system look saturated even though no useful request is running.

Here is the shape of that policy in Go. It is deliberately an interface around the moderation call: the scheduling rule should be testable without a live provider, and the caller should not need to know which service implements the boundary.

```go
package moderation

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

var ErrRateLimited = errors.New("moderation rate limited")

type Result struct {
	Allowed    bool
	RetryAfter time.Duration
	Retryable  bool
}

type Client interface {
	Moderate(context.Context, string) (Result, error)
}

type Policy struct {
	MaxAttempts int
	MaxBackoff  time.Duration
}

func (p Policy) Run(ctx context.Context, c Client, text string) (Result, error) {
	if p.MaxAttempts < 1 {
		return Result{}, errors.New("max attempts must be positive")
	}

	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		result, err := c.Moderate(ctx, text)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, ErrRateLimited) || attempt == p.MaxAttempts {
			return Result{}, err
		}

		delay := time.Duration(1<<uint(attempt-1)) * 250 * time.Millisecond
		if delay > p.MaxBackoff {
			delay = p.MaxBackoff
		}
		jitter := time.Duration(rand.Int63n(int64(delay) + 1))
		timer := time.NewTimer(jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Result{}, ctx.Err()
		case <-timer.C:
		}
	}

	return Result{}, errors.New("retry policy exhausted")
}
```

Production code should replace the example delay with a parsed server hint when the client exposes one, use an injected random source for deterministic tests, and validate overflow before shifting. The important shape is the boundary: retry only the classified condition, stop at a budget, and let cancellation win. A successful moderation response still requires the workflow to record the attempt and the resulting action state.

The queue should remember why it waited.

This policy does not apply unchanged to a streaming workflow that must deliver partial output within a hard interactive deadline. There, the right response may be an immediate moderation-pending state or a shorter deadline with manual review, because waiting for the retry budget could violate the product SLO.

## What should the SLO and cost ledger measure?

A useful SLO is not “the moderation API returns quickly.” For the CRM job, measure the percentage of eligible calls that reach a reviewed moderation decision and a safe CRM state within the agreed window. Track the window by tenant class if service obligations differ, but avoid a dashboard with dozens of one-off objectives that nobody can operate.

The cost ledger should record at least tenant ID, job ID, moderation attempt count, outcome class, input-size bucket, queue delay, and whether the attempt was an original call or a retry. Keep the ledger append-only or otherwise auditable. A mutable “latest usage” counter is easy to display and hard to reconcile after a worker restart.

Capacity planning follows from those records. Estimate peak eligible jobs per minute, the maximum attempts per job, and the concurrency that the downstream limit permits. The worst-case retry load is not the normal request rate multiplied by a vague safety factor; it is the admitted workload multiplied by the retry policy’s actual bound. That number belongs in load tests and in the per-tenant budget review.

Watch four signals together: rate-limit responses, retry attempts, queue age, and moderation-pending jobs. A rise in one signal is ambiguous. A rise in all four means the user-facing SLO is at risk. A rise in retries without queue age may indicate transient recovery; a rise in queue age without rate limits may indicate local worker or database capacity instead.

## When should a job stop retrying?

Stop when the attempt budget is exhausted, the job deadline has passed, the request is not retryable, or the tenant has exceeded its admitted work. The terminal state should retain enough reason to support an operator decision without storing sensitive transcript content. “Moderation pending” is a valid business state; silently creating CRM actions from a fallback path is not.

The manual-review queue needs its own capacity limit and SLO. Otherwise, the system merely moves the outage into a human inbox. Give reviewers a safe summary of the failure class, timestamp, tenant, and job identifier, then let them retrieve the minimum required source data through the authorized application path.

This design is a poor fit for a workflow that cannot tolerate a moderation-pending state or a human-review delay. In that case, choose a synchronous path with a deliberately smaller input and deadline, or do not automate the CRM action; the retry policy cannot manufacture availability after the downstream limit is reached.

Vendor-neutrality matters here because the failure policy is part of the application, not a feature to outsource. A provider can change its quota behavior or return a different timing hint; the application should still preserve tenant attribution, bounded recovery, and a clear terminal state. Those are durable controls.

The decision rule is therefore modest: keep the moderation boundary replaceable, make every attempt visible to the tenant ledger, and spend retries only while they can still satisfy the job’s SLO. That gives the platform team a defensible answer to both questions that arrive after an incident: why did this tenant consume capacity, and why did this CRM action happen?

## Sources

- https://python.langchain.com/docs/integrations/chat/openai/
- https://www.rfc-editor.org/rfc/rfc6585
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429
- https://developer.mozilla.org/en-US/docs/Web/API/AbortController
