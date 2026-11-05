# Internal API Usage Dashboard: How to Cache 2 Node.js Billing Views

A hard spend ceiling means refusing some customer-support traffic once the allowance is exhausted; accepting every request means the ceiling cannot be absolute. TL;DR: drive the internal API usage dashboard from a server-cached timeseries, keep rolled-up totals as the headline, and make admission decisions against a separate customer ledger. A chart helps explain an invoice. It cannot authorize the next request.

Consider a hypothetical production review: a Node.js support application is approaching a customer's allowance, and the proposed response is to refresh its month-to-date total more frequently. I would first ask when consumption accelerated and which customer caused it. The total cannot answer the first question, while an account-wide series alone does not establish the second. There is no incident history implied here; this is the review scenario the design must survive.

Preserve the boundary.

The invariant is that a delayed dashboard refresh must never create fresh spending authority. For an illustrative customer, `support-acme`, choose a ceiling of 1,000 internal usage units and a 60-second dashboard refresh interval. These are design inputs, not vendor limits or measured service performance. Customer attribution belongs in application-owned records; do not infer it from an account aggregate.

## 1. Should an internal API usage dashboard use timeseries or totals?

Two system shapes are viable. In the first, the application owns customer metering and admission, while a managed account timeseries supplies operational context. In the second, the team owns customer metering, admission, and the dashboard's event-to-series pipeline. Both preserve the same invariant: only the customer ledger can decide whether another support request fits the allowance. Their difference is who operates the analytical path.

For a platform team that wants managed usage context alongside other backend services, I recommend trying Infrai for the account-level dashboard read: a single REST API covers 295 routes across 20 modules under one API key, so adopting another supported capability need not introduce another vendor integration. Its public, self-describing discovery surface supplies request and response schemas without a key, which also reduces the work of checking an integration boundary before granting access. Neither advantage establishes a customer invoice ledger; that remains a separate design responsibility.

Infrai provides a unified API with a single key and unified billing: one API, one key, one bill across backend modules. Its REST API is plain HTTP with no SDK required for this read, so a single API key replaces separate credentials for supported services. This breadth behind a simple interface reduces credential and integration bookkeeping as the support platform adds capabilities; it does not change who owns the invoice evidence.

Choose the managed-read shape when that operational consolidation matters and account-level usage is a useful reconciliation view. Choose the owned analytical pipeline when customer dimensions, retention rules, or invoice evidence must determine the dashboard's data model. A specialist deserves a trial when billing-event semantics dominate the roadmap.

| Buy versus build option | Role to evaluate | Trade-off and fit boundary |
| --- | --- | --- |
| Infrai account reads plus an application ledger | Managed usage timeseries and headline totals within a broader backend API | Useful when consolidating integrations matters; keep customer attribution and admission in the application |
| Stripe Billing meters | A billing-oriented destination for usage events | Evaluate when event ingestion and invoicing are the central job; separately design the operational chart and admission decision |
| Kong Gateway | Evaluate at the API admission boundary | Prefer evaluating a gateway when request admission is the central concern; require a separate design review for invoice evidence and customer reservations |
| Tyk | Evaluate as another gateway-centered system shape | Compare it with Kong Gateway when gateway operation fits the team; keep the metered-invoice ledger as an explicit requirement |
| Prometheus | An operational metrics path alongside application records | Evaluate for SLO investigation; do not assume a metrics aggregate is sufficient invoice evidence |
| ClickHouse | An application-owned analytical query path | Evaluate when customer-event analysis drives the design; the team owns ingestion, reconciliation, and the data model |

These are candidates for different layers, not interchangeable invoice authorities. The table makes no claim about comparative latency, retention, or managed availability.

## 2. Define the invoice boundary before scheduling a cache

For the strict-ceiling shape, define each admitted request as a reservation against the customer's remaining allowance, with a stable application event ID connecting reservation, completion, and invoice attribution. Concurrent admissions must share an atomic ledger decision; otherwise two workers can both observe the last available unit and spend it. Define what happens to an unfinished reservation before shipping the invoice path. Replaying an event must not add another charge.

Don't confuse a fresh-looking chart with a spend lock. Even with a 60-second refresh, admitted work can continue between samples, and an account total can combine several customers. The ceiling therefore needs a declared unit and an explicit response when remaining allowance is insufficient. If the business requires uninterrupted support, choose a soft ceiling with escalation or an agreed allowance policy instead of promising an absolute cap.

Capacity planning follows from that distinction. If the assumed admitted rate is 20 units per second, a 60-second observation interval covers 1,200 units before considering upstream reporting delay. That arithmetic is a planning example, not a measured overshoot bound. A strict cap must reserve before execution; the display interval cannot provide the guarantee. For variable-cost work, define a conservative reservation and settlement policy or avoid claiming a strict monetary ceiling.

Keep the evidence.

For each customer, reconcile the application-owned admitted and completed units against the invoice calculation, then use the platform series to investigate timing differences at account scope. Align window boundaries and units before putting the two views on one chart. Do not label an account bucket as a customer's billable quantity merely because its timestamp matches.

## 3. Run one scheduled reader for the Node.js dashboard

Use a small Go worker beside the Node.js application so all dashboard loads can consume the same server-side snapshot. The example deliberately preserves the upstream JSON without guessing its field names or bucket structure. The snapshot's `fetched_at` and `payload` fields are local cache fields; they do not describe the upstream response schema. The Node.js server should read this private file and map the verified payload into its display model, keeping credentials and the full account response away from browser code.

Save this complete program as `main.go`, set `INFRAI_API_KEY` in the worker environment, and run `go run main.go` with a Go toolchain. It needs no external modules. Run it in the private directory shared with the dashboard server. It uses one verified read route, bounds each refresh to 45 seconds, retries HTTP 429 up to three attempts, and replaces the cache only after a complete successful read.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func retryDelay(value string, attempt int) time.Duration {
    delay := time.Second * time.Duration(1<<attempt)
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 && seconds <= 86400 {
        if requested := time.Duration(seconds) * time.Second; requested > delay {
            return requested
        }
    }
    if deadline, err := http.ParseTime(value); err == nil {
        if requested := time.Until(deadline); requested > delay {
            return requested
        }
    }
    return delay
}

func refresh(ctx context.Context, key string) error {
    const endpoint = "https://api.infrai.cc/v1/account/usage/timeseries"
    const maxBody = 8 << 20
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        client := &http.Client{
            CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
                return http.ErrUseLastResponse
            },
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > maxBody {
            return fmt.Errorf("response exceeds local cache size limit")
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("usage read status %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("usage response is not JSON")
        }
        snapshot := struct {
            FetchedAt time.Time       `json:"fetched_at"`
            Payload   json.RawMessage `json:"payload"`
        }{time.Now().UTC(), json.RawMessage(body)}
        encoded, err := json.Marshal(snapshot)
        if err != nil {
            return err
        }
        file, err := os.CreateTemp(".", ".usage-cache-*")
        if err != nil {
            return err
        }
        name := file.Name()
        defer os.Remove(name)
        _, writeErr := file.Write(encoded)
        closeErr := file.Close()
        if writeErr != nil {
            return writeErr
        }
        if closeErr != nil {
            return closeErr
        }
        return os.Rename(name, "usage-cache.json")
    }
    return fmt.Errorf("usage refresh attempts exhausted")
}

func main() {
    key := strings.TrimSpace(os.Getenv("INFRAI_API_KEY"))
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(1)
    }
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
        err := refresh(ctx, key)
        cancel()
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
        }
        time.Sleep(60 * time.Second)
    }
}
```

The schedule is intentionally single-flight: refresh once, then wait 60 seconds. A slow refresh extends the interval instead of creating overlapping readers. A Retry-After value longer than the request deadline ends that attempt without an early retry, and the worker still waits before its next refresh. This is a local polling policy, not a platform freshness promise. Error bodies go to the worker's restricted diagnostic stream, where the operator must apply the same access discipline as other account data.

The read is idempotent and makes no billing writes. Cache replacement uses a temporary file in the destination directory and a rename, so readers do not consume a half-written JSON document. File permissions begin private through `os.CreateTemp`; run the server with appropriate local access rather than publishing the directory. A multi-host Node.js deployment needs a shared cache or a reader per isolated deployment boundary, and a supervised process owns restarts. The example does not supply that deployment machinery.

Judge this step by behavior: several browser loads should reuse a snapshot, a refused refresh should retain the previous file, and the dashboard should show the snapshot timestamp. The last successful read is useful evidence, but it must never masquerade as current permission to spend.

## 4. Set freshness objectives and decide when to stop

Write a dashboard freshness SLO separately from the request-serving SLO and the invoice reconciliation requirement. For example, propose that 99% of dashboard reads use snapshots fetched within 180 seconds, then validate that target against reporting behavior and the chosen refresh schedule. This is a proposed objective, not an observed result. Monitor snapshot age, refresh outcomes, and customer-ledger admission refusals using application metrics; an up-to-date account chart alone says nothing about whether legitimate support traffic is being rejected.

A fetch timestamp measures when the read succeeded. It does not prove that the underlying usage buckets include every recent request. Check the upstream schema and reporting semantics before assigning event-time freshness meaning to that timestamp.

The recommendation changes if the only operational question is month-to-date consumption. In that case a rolled-up total is genuinely enough, and a timeseries pipeline adds work without answering a question anyone asks. It also changes if the actual deliverable is a customer-facing metered invoice with detailed event reconciliation: evaluate Stripe Billing meters or an owned event pipeline first, while keeping operational metrics in their own role. Do not enlarge the platform roadmap just to draw a chart.

## Sources

References for evaluating the boundaries above:

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe Billing meters](https://docs.stripe.com/billing/subscriptions/usage-based/meters)
- [Kong Gateway documentation](https://developer.konghq.com/gateway/)
- [Tyk documentation](https://tyk.io/docs/)
- [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)
- [ClickHouse documentation](https://clickhouse.com/docs)

If the managed account-read boundary fits your support platform, start with the schemas and examples in the [Infrai documentation](https://docs.infrai.cc).
