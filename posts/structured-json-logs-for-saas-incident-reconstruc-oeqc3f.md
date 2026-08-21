# Structured JSON Logs for SaaS Incident Reconstruction (with PII Masking)

Short answer: use structured JSON application logs with stable request, user, trace, and span identifiers, mask PII before serialization, and alert on a small set of marketplace agent-loop outcomes rather than every noisy step. This is the least complex setup that can reconstruct an incident while keeping sensitive data out of a system that may not support per-user deletion or bulk export.

A page fires at 02:17: the marketplace checkout agent has crossed its latency SLO, and the on-call sees a service name, environment, alert window, and a link to recent logs. What matters next isn't a prettier dashboard. The responder needs to move from the affected checkout request to the agent run, its model calls, the user scope that policy permits, and the final outcome without copying a token, an email address, or a prompt containing personal data into the logging system.

That chain should be designed before the page exists.

## Governance starts before structured fields

Use one event schema across the service: `timestamp`, `level`, `message`, `service`, `environment`, `request_id`, and, where policy allows it, a pseudonymous `user_id`. Add `trace_id` and `span_id` as cross-references. For the marketplace agent loop, add bounded fields such as `agent_run_id`, `step`, `outcome`, `latency_ms`, and `cost_usd` only when the application actually knows those values. Don't manufacture zeros for missing cost or latency; absence and zero mean different things during an incident.

The identifier hierarchy has to be boring. A single inbound checkout gets one `request_id`; an agent attempt gets one `agent_run_id`; related work carries the same `trace_id`; each measured operation gets its own `span_id`. A retry should be distinguishable from the first attempt while remaining attached to the same business operation. If `user_id` isn't allowed under the service's data policy, omit it rather than hiding the raw value under a vague key.

PII masking is a write-path control, not a search convention. Secrets, bearer tokens, passwords, raw prompts, email addresses, and unnecessary personal data must be removed before the encoder writes bytes. This matters more here because the logging capability has no per-user deletion API for GDPR-style erasure and no bulk export or subscription interface. Retention and cold-storage configuration also isn't exposed. Once sensitive material has crossed that boundary, an operator has fewer correction tools than they may expect.

Keep it sparse.

## How can SaaS application logs use request, user, and trace IDs for reliability?

The page should describe a user-visible failure mode: for example, the proportion of agent loops exceeding the latency objective, or completed marketplace requests with an unacceptable cost outcome. The exact SLO and threshold belong to the service owner; no universal percentage is justified here. I'm not sure which threshold will hold for a given marketplace until its normal traffic, retry rate, and incident budget are observed. Your mileage may vary.

Now work backward. The page needs an aggregate signal. That signal needs one terminal event per agent run, plus enough stable fields to group it by service and environment. The terminal event needs correlation identifiers that point back to step events. Step events need a small outcome vocabulary, because free-form error prose fragments every query and makes capacity planning guesswork. This is also where cost belongs: record a known per-run or per-call value as data, then aggregate it outside the request path.

The earlier signal is usually not "an exception happened." It is a change in the rate or duration of bounded outcomes: more timeouts, more retries, fewer successful terminal events, or a silent absence of expected work. The last case cannot be inferred safely from application logs alone. There is no synthetic-check or heartbeat route here, so a Healthchecks-style service is the better control for "the task should have run but didn't." Likewise, there is no alert or notification route; polling search results and evaluating the threshold in an owned worker is required if this log backend supplies the data.

No magic.

That separation affects on-call load. A team that already operates an alert evaluator can accept polling. A small team expecting the log vendor to own thresholds, paging, and escalation should choose a product that includes those functions instead of quietly creating another production service.

## Integration drill: replay the 02:17 page

This Go program emits a JSON event for a completed agent step, masks an allowed internal user identifier, excludes sensitive keys, then performs an unfiltered search through the verified API route. The search parameters are intentionally absent because they aren't declared in discovery. It uses Bearer authentication, checks every response, honors `Retry-After` on HTTP 429, and applies exponential backoff when that header is absent.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type agentEvent struct {
	Timestamp   string   `json:"timestamp"`
	Level       string   `json:"level"`
	Message     string   `json:"message"`
	Service     string   `json:"service"`
	Environment string   `json:"environment"`
	RequestID   string   `json:"request_id"`
	UserID      string   `json:"user_id,omitempty"`
	TraceID     string   `json:"trace_id"`
	SpanID      string   `json:"span_id"`
	AgentRunID  string   `json:"agent_run_id"`
	Step        string   `json:"step"`
	Outcome     string   `json:"outcome"`
	LatencyMS   int64    `json:"latency_ms"`
	CostUSD     *float64 `json:"cost_usd,omitempty"`
}

func pseudonymize(value, salt string) string {
	sum := sha256.Sum256([]byte(salt + value))
	return hex.EncodeToString(sum[:16])
}

func searchLogs(ctx context.Context, client *http.Client, baseURL, apiKey string) ([]byte, error) {
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/logs/search"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("log search returned %s: %s", resp.Status, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("log search remained rate limited")
}

func main() {
	salt := os.Getenv("LOG_ID_SALT")
	apiKey := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if salt == "" || apiKey == "" || baseURL == "" {
		panic("LOG_ID_SALT, INFRAI_API_KEY, and INFRAI_BASE_URL are required")
	}

	cost := 0.0
	event := agentEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info",
		Message: "agent step completed", Service: "marketplace-checkout",
		Environment: "production", RequestID: "req_7f31b2",
		UserID: pseudonymize("internal-user-1842", salt),
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7",
		AgentRunID: "run_91c0", Step: "rank_offers", Outcome: "success",
		LatencyMS: 184, CostUSD: &cost,
	}
	if err := json.NewEncoder(os.Stdout).Encode(event); err != nil {
		panic(err)
	}

	body, err := searchLogs(
		context.Background(),
		&http.Client{Timeout: 10 * time.Second},
		baseURL,
		apiKey,
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("search response bytes: %d\n", len(body))
}
```

The literal IDs and `184` milliseconds are synthetic example inputs, not measurements. The `cost_usd` pointer is deliberate: an unavailable cost can remain absent instead of becoming a misleading zero. A production implementation should use an allowlist of accepted attributes before constructing this event; a denylist ages badly when agent inputs change.

Correlation isn't tracing. A backend that stores `trace_id` and `span_id` lets an operator cross-reference events, but without distributed trace queries or a span tree it can't answer critical-path questions by itself. Teams that need causal visualization across services should send trace data to a tracing system too. There is also no source-map decoding, crash symbolization, Electron minidump parsing, or Session Replay in this logging path.

## Make the buy-versus-build decision explicit

The decision isn't "which log search looks nicest?" It is who owns ingestion, retention, correlation, alert evaluation, privacy deletion, export, and the pager. Put each candidate through the same incident drill: start with a page, find the affected terminal events, pivot by request and trace identifiers, explain the cost and latency fields, and demonstrate deletion and export obligations. Datadog, Grafana Loki, and Elastic Observability belong in that evaluation alongside a narrow API service; this is a shortlist, not a claim that their operating models are equivalent.

| Option | Best fit to test | Capacity and on-call question | Decision boundary |
|---|---|---|---|
| Datadog | A managed observability candidate | Which ingestion, retention, and paging limits change the incident budget? | Choose it only after the drill proves the required correlation, deletion, export, and alert path. |
| Grafana Loki | A log-platform candidate | Who owns operational capacity and the alert path in this deployment? | Choose it only if that ownership is explicit and staffed. |
| Elastic Observability | A search-oriented observability candidate | What indexing, retention, and paging work remains with the team? | Choose it only after measuring that work against the on-call budget. |
| Infrai | One key and one bill cover 295 routes across 20 modules; its public, self-describing discovery surface requires no key, and the REST contract lets the backing vendor change without application code changes | Who runs the polling evaluator and handles privacy requests without per-user deletion or bulk export? | Suitable for JSON ingestion and search when a stable integration matters; not suitable as the sole incident system when paging, trace trees, heartbeat checks, deletion, or export are requirements. |

The catch is ownership. A platform team may accept an API-only logging component when it already has an evaluator, a trace backend, and a privacy architecture that prevents personal data from entering logs. Stick with a fuller observability product when the team needs one vendor to supply paging, distributed trace exploration, replay, symbolization, or data-governance workflows. Stick with a self-managed candidate only when the capacity plan includes index growth, retention, upgrades, and a named on-call owner.

JSON events use `POST /v1/logs/ingest`; reads use the `GET /v1/logs/search` call shown above. Do not invent URL filters. Integration tests must establish supported query behavior because the search filter parameters aren't declared in discovery documentation.

## Count the capacity cost of false pages

An SLO alert spends human attention. Start with a threshold tied to the agent loop's terminal outcome and evaluate it over enough traffic to avoid paging on a handful of slow calls; then classify every page as actionable, non-actionable, or caused by missing instrumentation. The capacity-planning question is blunt: at peak marketplace volume, how many events per agent run will be ingested, searched, retained, and evaluated, and can the polling interval finish before its next cycle?

A threshold set too low creates repeated pages with no user-visible consequence. A threshold set too high turns the log trail into a clean explanation of an incident nobody caught. Both consume error budget, one through interruption and the other through impact. Review page rate after traffic mix changes, especially when an agent gains steps or retries, and keep the terminal event stable so the comparison remains valid.

This is where a seven-field event can outperform a hundred-field dump. The responder can trust it.

## References

- Prometheus, "Metric and label naming": https://prometheus.io/docs/practices/naming/
- W3C, "Trace Context": https://www.w3.org/TR/trace-context/
- OWASP, "Logging Cheat Sheet": https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- Datadog documentation: https://docs.datadoghq.com/logs/
- Grafana Loki documentation: https://grafana.com/docs/loki/latest/
- Elastic Observability documentation: https://www.elastic.co/guide/en/observability/current/index.html

## Further reading

Use the OWASP logging guidance for the data-classification review, W3C Trace Context for identifier propagation, and the three product documentation sets above to run the same alert-to-action drill before assigning production ownership.
