# Missed-job detection for Node.js cron: self-hosted heartbeat monitoring with timeouts

A scheduled job that never fires produces no logs, no stack trace, and no error rate to alert on, so the cheapest correct fix is a dead man's switch: every run pings a receiver you control, and the receiver pages you when the ping fails to arrive on time. Use a self-hosted ping endpoint plus a periodic overdue sweep and you have missed-job detection for Node.js cron without installing an agent on every host, without a vendor contract, and without touching the job's business logic beyond two curl calls.

That is the entire mechanism. Everything after this is failure modes, retention, and who gets paged at 3am.

Take a fintech reconciliation loop: a Node.js cron entry fires every five minutes, hands a batch of unmatched transactions to an LLM agent, and writes the agent's proposed matches to a review queue. Two questions decide the design — what did the loop cost, and what was its latency — and both of them are worthless unless you can also answer the third one, which is whether the loop ran at all during the window someone is now asking you about.

## Why a health check endpoint never sees the run that didn't happen

An HTTP health check answers "is this process accepting connections". A cron entry that didn't fire is not a process that stopped accepting connections; it is an absence, and absences don't emit. You can have a green dashboard, a green health check endpoint, a 100% success rate on the runs that did happen, and a six-hour hole in your reconciliation history, all at the same time.

The absences come from a short and boring list. A scheduler container gets evicted and the replacement starts after the window closed — Kubernetes CronJob will simply skip a schedule it missed past `startingDeadlineSeconds` and record nothing louder than an event. An in-process timer library dies with the process that owned it, which is the usual outcome when the cron entry lives inside the same Node.js service as the API and someone restarts the API. The event loop gets blocked by a synchronous JSON parse over a large batch and the timer fires eleven minutes late, which is not a miss but reconstructs identically in the logs. The previous run hasn't finished, so the next one either overlaps and double-writes or gets suppressed and silently skipped, depending on which guard someone added last. Daylight saving arrives and the local-time schedule fires twice or not at all. And the run that everybody forgets: the job ran, exited non-zero, and cron mailed the output to a mailbox nobody has read since the machine was provisioned.

None of these fire an alert. That's the whole problem.

## How do I detect a missed cron job in Node.js when I have no heartbeat monitoring?

Invert the check. Instead of asking the job whether it is healthy, make the job prove it existed, and alert on the proof not showing up. Three pieces, none of them large.

First, an expectation registry: a slug per schedule, the period it should fire at, a grace window because late is not the same as dead, and a maximum run duration. Second, a ping contract with three events — start, ok, fail — carrying a run id, the attempt number, and whatever cost and latency numbers the run wants to report. Third, a sweeper that wakes on its own interval and compares wall-clock now against the last successful completion per slug.


## Buy, self-host, or reuse the metrics stack: how the three approaches compare

Three shapes solve this, and they fail differently.

| Approach | What you operate | Detection path | Main limitation |
| --- | --- | --- | --- |
| Self-hosted heartbeat receiver | One small service, one datastore, one alert route | Overdue sweep on stored last-success | It shares your infrastructure's failure domain |
| Managed ping service | Nothing; free tiers exist for a handful of checks | Vendor's sweep, vendor's alert routing | Egress from your job to a third party, per-check limits |
| Metric absence rules | Whatever you already run | Alert on missing or stale time series | Absent-metric rules are subtle to write and easy to get wrong |

The self-hosted option is not free — it costs a service, a page, and a place to store run records — but it buys you the run payload, which the managed ping services generally do not model in the shape a cost-per-run question needs. Healthchecks, the open-source project behind the pattern most people mean when they say heartbeat monitoring, is BSD-licensed, self-hostable, and deliberately narrow: it tells you a check is late or failing, and it is a fine answer if late-or-failing is genuinely all you need. The catch is that "was this loop's p95 latency and its per-run agent spend inside budget" is a different question, and answering it from a system that models checks rather than runs means bolting a second store onto the side anyway.

Prometheus-style absence rules deserve one honest note: `absent()` and `time() - max(last_success_timestamp)` alerts work, and if a metrics pipeline with alert routing is already in production, the marginal cost of another rule is close to zero. Stick with that path when the alerting and on-call rotation are already wired. The subtler trap is structural — the absence rule evaluates against a series that also disappears when the scrape target disappears, so the rule that was supposed to catch the missing job goes silent at exactly the moment the job goes missing.


## Retries, timeouts, and the incident you actually want to page on

Timeouts and retries are where the naive version falls apart, so be explicit about both. A run that pinged start and never pinged anything else is a different incident from a run that never started: the first one is hung and probably holding a database transaction, the second one means the scheduler is gone, and paging them with the same message guarantees the wrong first move at 3am. Retries need a run id, because three ping-fail events from three attempts of the same scheduled fire are one incident and not three; alert on the final attempt, count the earlier ones, and keep the attempt number in the record so incident reconstruction can tell "flaky upstream, recovered on attempt two" apart from "broken since Tuesday". The ping call itself gets a hard timeout of a few seconds and a swallowed error — a monitoring endpoint that can fail the job it monitors is a worse availability risk than the thing you set out to catch.

Page on the second miss, not the first, unless the schedule is daily.

## A minimal receiver: one ping handler, one sweep, and the code path in between

Two moving parts: an HTTP handler that records pings, and a sweep that turns silence into a page.

```go
package deadman

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Expectation is the contract a schedule signed with us.
type Expectation struct {
	Slug   string        // "recon-agent-loop"
	Period time.Duration // how often the cron entry should fire
	Grace  time.Duration // late is not dead
	MaxRun time.Duration // a start with no finish is its own incident
}

// Run is what a single scheduled fire reports about itself.
type Run struct {
	ID        string  `json:"id"`
	Attempt   int     `json:"attempt"`
	LatencyMS int64   `json:"latency_ms"`
	CostUSD   float64 `json:"cost_usd"`
	Started   time.Time
}

type Monitor struct {
	mu    sync.Mutex
	exp   map[string]Expectation
	last  map[string]time.Time // last ok per slug
	open  map[string]Run       // started, not yet finished
	page  func(slug, reason string)
	sink  func(slug string, r Run, ok bool) // durable run record
}

// Ping handles POST /ping/{slug}/{start|ok|fail}. An empty body is still valid.
func (m *Monitor) Ping(w http.ResponseWriter, req *http.Request) {
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "ping" {
		http.Error(w, "expected /ping/{slug}/{event}", http.StatusNotFound)
		return
	}
	slug, event := parts[1], parts[2]

	var r Run
	_ = json.NewDecoder(http.MaxBytesReader(w, req.Body, 4<<10)).Decode(&r)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, known := m.exp[slug]; !known {
		http.Error(w, "unknown slug", http.StatusNotFound)
		return
	}
	switch event {
	case "start":
		r.Started = time.Now()
		m.open[slug] = r
	case "ok":
		delete(m.open, slug)
		m.last[slug] = time.Now()
		m.sink(slug, r, true)
	case "fail":
		delete(m.open, slug)
		m.sink(slug, r, false) // retries land here too; page on the last attempt
	default:
		http.Error(w, "unknown event", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Sweep is the part that actually detects absence. Call it on a ticker.
func (m *Monitor) Sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for slug, e := range m.exp {
		window := e.Period + e.Grace
		if last, seen := m.last[slug]; !seen || now.Sub(last) > window {
			m.page(slug, "no successful run within "+window.String())
		}
		if run, running := m.open[slug]; running && now.Sub(run.Started) > e.MaxRun {
			m.page(slug, "run "+run.ID+" started but never reported an outcome")
		}
	}
}
```

The Node.js side needs no library at all, because the wrapper around the cron entry is the integration point and it is six lines of shell:

```bash
set -u
slug=recon-agent-loop
run_id=$(uuidgen)
started=$(date +%s)
curl -fsS -m 5 -X POST "$MONITOR/ping/$slug/start" -d "{\"id\":\"$run_id\",\"attempt\":${ATTEMPT:-1}}" || true
node ./jobs/recon-agent-loop.js
rc=$?
elapsed=$(( ($(date +%s) - started) * 1000 ))
event=ok; [ $rc -ne 0 ] && event=fail
curl -fsS -m 5 -X POST "$MONITOR/ping/$slug/$event" -d "{\"id\":\"$run_id\",\"latency_ms\":$elapsed}" || true
exit $rc
```

Note the two `|| true` guards and the `-m 5` timeout. The monitor is allowed to be down; the job is not allowed to care.

## What this doesn't give you, and where the capacity math bites

A run record per fire is a row, and rows are a capacity question before they are an observability question. Forty schedules at a five-minute period is 11,520 runs a day, roughly four million a year, and if each row carries the agent loop's token counts and per-model spend it is still small enough that a columnar store handles it without thinking — which is the argument for keeping run records in an analytical store rather than in the alerting system, since incident reconstruction is a scan over a time range and not a point lookup. Percentiles matter more than averages here, and the web performance world settled this argument years ago with a p75 threshold on user-facing metrics rather than a mean; an agent loop whose median finishes in nine seconds and whose p95 finishes in four minutes is a loop that will blow its window under load, and the average will never tell you.

Now the boundaries, since this pattern is narrower than it looks.

It doesn't support partial-failure detection: a run that processed three of forty batches still pings ok, so the receiver reports a healthy schedule while the backlog grows. Detecting that needs a work-completed counter and a separate rule, and it is a genuinely different alert with a different owner. It doesn't tell you why a run was missed — you get the absence, the scheduler logs give you the cause, and pretending otherwise leads to a very unproductive first ten minutes of an incident. It has no answer for a monitor that stops sweeping, so the receiver needs its own external heartbeat, which is the one place where a free managed ping check earns its keep: it is the watcher of the watcher, and it should sit outside your failure domain on purpose. And if the schedule is sub-minute, the grace window swallows the signal; at that frequency the right instrument is a queue depth or lag metric, not a heartbeat.

I am not sure there is a version of this that stays simple once you add per-tenant schedules — the expectation registry turns into a dynamic set, the sweep turns into a join, and at that point the honest comparison is against a real workflow engine with durable timers rather than against another ping endpoint.

## References

- Kubernetes CronJob concepts, including missed schedules and `startingDeadlineSeconds`: https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
- Node.js event loop and timer scheduling: https://nodejs.org/en/learn/asynchronous-work/event-loop-timers-and-nexttick
- crontab(5) manual page: https://man7.org/linux/man-pages/man5/crontab.5.html
- Healthchecks self-hosted deployment documentation: https://healthchecks.io/docs/self_hosted/
- Prometheus Alertmanager documentation: https://prometheus.io/docs/alerting/latest/alertmanager/
- web.dev, Core Web Vitals and the p75 threshold: https://web.dev/articles/vitals
- ClickHouse documentation (analytical storage for run records): https://clickhouse.com/docs
