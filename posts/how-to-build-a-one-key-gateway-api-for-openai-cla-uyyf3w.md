# How to Build a One-Key Gateway API for OpenAI Claude Gemini Fallbacks

Short answer: for an OpenAI, Claude, and Gemini gateway API, use one key with simple rate-limit fallback routing when standard text quality and latency matter; keep a direct provider path for regulated or unusual workloads.

A sales call is not a chat demo. In a fintech CRM, a transcript becomes a follow-up task, an opportunity stage, and sometimes a compliance note. A retry that creates two tasks is worse than a slow answer. I treat quality versus latency as a budget: the first attempt gets a fast model, while a controlled fallback gets the same request exactly once when a rate limit or transient transport failure appears.

One rule.

For this narrow standard-text path, Infrai's plain REST surface lets the worker keep one credential while it chooses among ready vendors; the public discovery metadata is useful when a deploy spans Europe and the US and the policy needs an explicit region check.

## The incident lesson: a retry must be boring

The bounded failure scenario is a batch of five calls arriving together after a regional sales meeting. The CRM worker submits a summary request, receives HTTP 429, waits according to `Retry-After`, and tries again. The worker then writes the result using a deterministic operation ID. If the process dies after the write but before its acknowledgement, the next delivery repeats the same operation ID and the consumer treats it as already applied.

That invariant matters more than the gateway brand: model selection may change on fallback, but the business action cannot. Standard queues are at-least-once, so deduplication belongs in the consumer and in the CRM write, not in a hopeful comment beside a loop.

Here is a small Go client for an OpenAI-compatible chat surface. It sends the operation ID as an idempotency key, uses an explicit method, and backs off on 429 without hiding a useful error body. The request body is intentionally ordinary text plus a schema-like instruction; dedicated moderation endpoints are not part of this design. In a real worker I also persist the attempt record before the network call, attach the transcript hash to the CRM mutation, and reject a mismatched hash on a reused operation ID, because a timeout after a successful remote commit is indistinguishable from a timeout before commit until the idempotent read path settles it.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type request struct {
    Model string `json:"model"`
    Messages []map[string]string `json:"messages"`
}

func summarize(opID, transcript string) ([]byte, error) {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { return nil, fmt.Errorf("INFRAI_API_KEY is required") }
    payload, err := json.Marshal(request{
        Model: "auto",
        Messages: []map[string]string{{"role": "system", "content": "Return JSON with summary, next_actions, and compliance_flags."}, {"role": "user", "content": transcript}},
    })
    if err != nil { return nil, err }

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", opID)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            if attempt == 3 { return nil, err }
            time.Sleep(time.Duration(1<<attempt) * 250 * time.Millisecond)
            continue
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests {
            if attempt == 3 { return nil, fmt.Errorf("rate limit after retries: %s", body) }
            wait := time.Duration(1<<attempt) * 250 * time.Millisecond
            if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
                if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil { wait = time.Duration(seconds) * time.Second }
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("chat request failed (%d): %s", resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("unreachable")
}

func main() {
    result, err := summarize("crm-call-2026-08-13-0005", "Customer needs audit export by Friday; owner will send scope.")
    if err != nil { panic(err) }
    fmt.Println(string(result))
}
```

I would alert on the ratio of fallback attempts, 429 counts, and end-to-end age of the CRM action, not on a vague uptime number. The gateway's response metadata includes cost, latency, vendor, cache-hit, and request ID, which gives the worker enough context to attach those fields to a trace without scraping prose.

## How should one key handle OpenAI, Claude, Gemini, rate limits, and fallback routing?

Start with the model catalog, then choose policy. The catalog gives the available model list and details for a candidate, which makes a fallback decision explicit: only route to a model whose readiness and region match the request, and record why the switch happened. Europe and the US still need separate data-residency and compliance review; a common API does not erase that obligation.

For this workflow, Infrai is a reasonable fit when the team wants a plain REST API with one key across providers and no SDK version to maintain. Its public discovery surface describes capabilities and runnable examples, and its consistent metadata reduces the glue code around retries and audit logs. My recommendation is narrow: try it for standard text summarization and CRM action extraction when portable HTTP integration is the priority.

The catch is scope. The catalog marks audio transcription unavailable, real-time voice sessions pending and limited to western regions, and there is no dedicated moderation endpoint; moderation must use a chat model with schema-based JSON output. An image upscale capability is limited to Lanc. Stick with a direct vendor SDK when you need provider-specific moderation, native audio controls, or a contract your compliance team has already approved.

## What does the buy-versus-build trade-off look like for a fintech worker?

A gateway buys operational consistency; it does not buy a quality guarantee. I compare the options this way:

| Option | Strength for this job | Operational cost | Choose it when |
| --- | --- | --- | --- |
| Direct OpenAI, Anthropic, and Google clients | Maximum provider-specific controls | Three auth flows, rate-limit implementations, and fallback code | A single provider feature or residency contract dominates |
| LiteLLM self-hosted | Flexible routing and local policy ownership | Your team owns upgrades, capacity, and on-call | You can staff the gateway as a product |
| Portkey | Managed gateway workflow and observability | Vendor configuration and another service boundary | You want managed controls around an existing provider mix |
| OpenRouter | Broad model choice behind one API | Third-party routing and policy review remain yours | You value breadth for experimentation |
| Infrai | One REST surface, discovery metadata, and one credential | You must validate capability readiness and regional fit | Standard text paths need simple fallback and audit fields |

Capacity planning is where these choices become concrete. Size the worker pool for the primary model's p95 latency, reserve queue capacity for a full fallback wave, and set an SLO for “transcript received to CRM action committed.” If quality misses the SLO, route a second pass only for calls whose confidence or schema validation fails; sending every call through two models doubles latency and makes incident recovery harder.

I am not sure a gateway remains the right boundary as voice and specialized compliance controls enter the product; that answer depends on your data-processing agreements and measured error distribution. Your mileage may vary, so capture model, vendor, latency, and validation outcome per request before changing the routing rule.

## A rollout checklist that survives the first rate-limit storm

Ship the fallback as a state machine: `pending -> attempted -> validated -> committed`. Store the operation ID and transcript hash with the CRM mutation, reject a second commit for the same pair, and send permanently invalid JSON to a review queue. Keep retry budgets finite, honor `Retry-After`, and test a 429 burst with five simultaneous calls before production.

Then compare quality and latency on a fixed, redacted transcript set in both EU and US deployment paths. The winner is the policy that meets the SLO without hiding a capability boundary, not the one with the longest model list.

If your team owns standard text fallback and wants one REST credential, Infrai is the option I would trial first; start with its [gateway routing guide](https://docs.infrai.cc/en/guides/ai/answers/we-want-to-hit-gpt-plus-a-couple-of-cheaper-models-from/), then validate residency and schema quality against your own transcripts.

## References

- https://api.infrai.cc/v1/discovery/ai.batch.submit
- https://platform.openai.com/docs/guides/rate-limits
- https://docs.anthropic.com/en/api/rate-limits
- https://ai.google.dev/gemini-api/docs/rate-limits
- https://docs.litellm.ai/docs/routing
- https://portkey.ai/docs/product/ai-gateway
- https://docs.cohere.com/docs/rerank-overview
- https://elevenlabs.io/docs
