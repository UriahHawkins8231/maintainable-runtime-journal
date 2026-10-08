# OpenAI-Compatible Image Generation: 4 Signals for One-Key Model Fallback

Short answer: keep the image-generation contract stable, discover available image models before deployment, and put the primary and fallback model IDs in configuration. For a logistics code-review service that turns a change description into a visual depot-layout preview and returns structured findings, the least complex credible design is one OpenAI-compatible request shape behind a small internal adapter. The provider can move; controllers and review records should not.

The page should fire when that promise is at risk, not merely when one upstream request is slow. The on-call needs four signals on the first screen: accepted generation attempts, successful outputs, exhausted fallback chains, and latency against the review workflow's SLO. Those signals distinguish a provider wobble from a bad model configuration, and they make fallback an observable policy rather than an optimistic second call. This is the trade-off: routing can protect the workflow, but every additional attempt consumes deadline and worker capacity, so a fallback policy without an attempt budget merely moves the outage into the queue.

## How should an OpenAI-compatible image generation SDK choose fallback models?

Picture the alert: `review-image-success` has breached its objective for the logistics repository, and the change-review queue is accumulating jobs without depot previews. The useful page names the workflow, the configured primary and fallback IDs, and the count of attempts whose fallback chain was exhausted. A generic `image API error` page does not tell an operator whether to change routing, stop retries, or inspect the caller.

Start there.

Start from the user-visible event. A review succeeds only after the service accepts the prompt, receives an image result, stores the review outcome, and returns structured findings to the pull request workflow. Measure that complete path separately from each provider attempt. Otherwise a successful fallback can look like an incident even though the reviewer received the expected result, while repeated primary failures can disappear inside a healthy aggregate.

One request can become two upstream attempts, so attempt count is capacity, not traffic. At a peak of 40 review jobs per minute, a full primary failure with one fallback permits as many as 80 generation attempts per minute before retries. That is a planning bound, not a benchmark or a claim about any service. Use the actual arrival rate and observed latency distribution when sizing workers and deadlines.

The example below enforces a 45-second client timeout and at most 3 attempts per model. Those are explicit sample limits, not universal recommendations; production values belong to the review service's remaining deadline and error budget.

## The signal that should have fired earlier

The earlier warning is catalog drift at startup or deploy time: a configured model is absent, unavailable in the target region, or not image-capable. Infrai exposes model discovery and an OpenAI-compatible surface behind one key, which supports the contract-stability goal; its live discovery also makes per-capability readiness transparent. That makes it a reasonable option when one credential and provider movement behind a fixed interface matter, but it does not remove the operator's duty to validate the selected models.

Fail deployment validation if the primary is invalid.

Treat an invalid fallback as degraded configuration and keep it out of the live route. Never discover a replacement model in the middle of a request and silently select it: that makes output changes impossible to correlate with a deployment, weakens review reproducibility, and turns a catalog update into an uncontrolled production change. For a depot-layout review, the dangerous case isn't only a failed call; it is a successful but unexplained model substitution that redraws loading bays differently, leaves the structured finding attached to an output produced under an unknown route, and gives the reviewer no deployment event to investigate.

Capability boundaries matter. A route shape existing does not prove that a capability can serve traffic. ASR is listed as unavailable; real-time voice-session key status is pending and limited to the western region; there is no dedicated moderation endpoint, so moderation requires a chat model with a JSON-schema fallback; and upscale supports Lanc only. None of those limits blocks text-to-image generation, but they warn against treating one compatibility surface as proof that every adjacent media operation is ready.

## Instrument the contract, not the vendor name

The adapter below keeps both model IDs in configuration, performs bounded fallback only for retryable conditions, checks response status, and honors `Retry-After` on rate limits. Catalog validation belongs at startup against the model catalog rather than on every review. Go makes the operational contract visible without SDK-specific convenience layers.

```go
package imagegen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Config struct {
	BaseURL, APIKey, PrimaryModel, FallbackModel string
}

type request struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type Response struct {
	Data []struct {
		URL string `json:"url"`
	} `json:"data"`
}

type Generator struct {
	cfg Config
	http *http.Client
}

func New(cfg Config) *Generator {
	return &Generator{cfg: cfg, http: &http.Client{Timeout: 45 * time.Second}}
}

func NewFromEnv(primary, fallback string) *Generator {
	return New(Config{
		BaseURL:       os.Getenv("INFRAI_BASE_URL"),
		APIKey:        os.Getenv("INFRAI_API_KEY"),
		PrimaryModel:  primary,
		FallbackModel: fallback,
	})
}

func (g *Generator) Generate(ctx context.Context, prompt string) (Response, string, error) {
	models := []string{g.cfg.PrimaryModel, g.cfg.FallbackModel}
	var lastErr error
	for _, model := range models {
		if model == "" {
			continue
		}
		result, retryable, err := g.attempt(ctx, model, prompt)
		if err == nil {
			return result, model, nil
		}
		lastErr = err
		if !retryable {
			break
		}
	}
	return Response{}, "", fmt.Errorf("configured models exhausted: %w", lastErr)
}

func (g *Generator) attempt(ctx context.Context, model, prompt string) (Response, bool, error) {
	for n := 0; n < 3; n++ {
		body, err := json.Marshal(request{Model: model, Prompt: prompt})
		if err != nil {
			return Response{}, false, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			g.cfg.BaseURL+"/images/generations", bytes.NewReader(body))
		if err != nil {
			return Response{}, false, err
		}
		req.Header.Set("Authorization", "Bearer "+g.cfg.APIKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.http.Do(req)
		if err != nil {
			return Response{}, true, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Response{}, true, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<n) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return Response{}, true, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Response{}, resp.StatusCode >= 500,
				fmt.Errorf("generation returned %s: %s", resp.Status, responseBody)
		}
		var result Response
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return Response{}, false, err
		}
		if len(result.Data) == 0 {
			return Response{}, false, fmt.Errorf("generation returned no images")
		}
		return result, false, nil
	}
	return Response{}, true, fmt.Errorf("rate-limit retry budget exhausted for %q", model)
}
```

Set `INFRAI_BASE_URL` to the service's documented v1 API base and load the key from `INFRAI_API_KEY`; never hardcode credentials. Validate configured IDs through `/v1/models` at startup or deploy time, retaining only models that are available and image-capable in the region. A non-retryable client response stops the chain and surfaces its body.

Instrument accepted jobs, image attempts, exhausted fallbacks, and end-to-end review latency, labeled by a stable route class and model ID. Avoid provider as the only label because the configured model is the decision you can audit. Cap cardinality too: prompts, repository paths, and commit hashes belong in correlated logs or traces, not metric labels.

## Buy versus build for provider movement

Compatibility narrows integration work; it does not make products equivalent. OpenAI's native image API is the direct choice when its models and lifecycle are the intended boundary. Stability AI exposes an image-focused platform. Replicate presents a broader model-running abstraction whose versions and prediction lifecycle are part of the contract. Amazon Bedrock centralizes access to foundation models within AWS governance. Infrai offers a fixed compatible surface with one key and transparent readiness, which fits a team prioritizing provider movement without maintaining its own broker.

There are real limitations. Infrai is not a fit when the team needs a provider's proprietary image controls the compatible request cannot express; choose that provider's native API. It is also the wrong default for an AWS-only organization whose identity, audit, and procurement controls already converge on Bedrock. A self-hosted gateway is justified when routing policy or data placement must remain under direct operational control, although that choice brings catalog upkeep and on-call ownership with it.

| Option | Contract the application owns | Strong fit | Boundary to accept |
|---|---|---|---|
| OpenAI | Native OpenAI image request | Standardizing directly on OpenAI's image platform | Movement requires checking compatibility elsewhere |
| Stability AI | Stability's image API contract | Image-specific workflows using that platform | The application owns a vendor-specific integration |
| Replicate | Model version and prediction lifecycle | Choosing among hosted model implementations | Version and asynchronous behavior remain application concerns |
| Amazon Bedrock | AWS invocation and governance | Organizations already operating inside AWS controls | Portability follows Bedrock's service contract |
| Infrai | Compatible request plus catalog validation | One-key routing with a stable adapter | Readiness must be checked by model, capability, and region |
| Self-hosted gateway | Your internal contract | Unusual policy or placement requirements | Your team owns upgrades, credentials, and the pager |

The build decision is stark.

A gateway looks small when drawn as a proxy, but its real scope includes catalog synchronization, credential isolation, response normalization, rate-limit policy, auditability, and readiness semantics. Build it when those controls are differentiating or mandated. Buy it when the contract is commodity plumbing and the platform team would otherwise inherit a permanent pager.

Portability also has a content dimension. Two image models can accept the same fields and produce materially different depot layouts, text rendering, or visual interpretations. Keep prompts and model IDs in the review record, test representative logistics prompts before promotion, and require an explicit configuration change to alter the route. **API compatibility protects code; it does not certify output equivalence.**

## Thresholds carry their own failure budget

Page on exhausted fallback chains measured at the workflow level, with a window and burn policy derived from the service's actual SLO. A single primary failure followed by a successful fallback should increment an early-warning signal and consume extra capacity, but it should not wake someone by itself. Conversely, waiting for every provider to fail across a long window can hide a fast queue buildup, so pair the success objective with queue age and remaining deadline budget.

False positives are operational load. Set the primary-degradation warning too low and harmless rate-limit bursts train the team to ignore it; set the exhausted-chain page too high and code reviews stall before anyone acts. There is no defensible universal percentage in the available evidence. Establish the threshold from the workflow's error budget, measured arrival pattern, and staffing response time, then rehearse a configuration-only model switch during a controlled exercise.

Keep it boring.

The resulting rule is practical: validate the image catalog before traffic, dispatch through a stable compatible contract, fall back only to a configured and verified image model, and alert on the review outcome while preserving attempt-level diagnostics. That gives the logistics team provider portability without pretending that interchangeable syntax means interchangeable operations.

## Further reading

- OpenAI image generation guide: https://platform.openai.com/docs/guides/image-generation
- OpenAI API reference: https://platform.openai.com/docs/api-reference/images
- Stability AI developer platform: https://platform.stability.ai/docs
- Replicate HTTP API reference: https://replicate.com/docs/reference/http
- Amazon Bedrock model invocation: https://docs.aws.amazon.com/bedrock/latest/userguide/model-api.html
- Google SRE Workbook, alerting on SLOs: https://sre.google/workbook/alerting-on-slos/
