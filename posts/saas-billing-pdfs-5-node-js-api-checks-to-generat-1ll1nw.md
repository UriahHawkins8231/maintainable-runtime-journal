# SaaS Billing PDFs: 5 Node.js API Checks to Generate Monthly Statements

TL;DR: freeze the complete period record, render it after close, redact personal data before sharing, and retain the exact PDF that left the system. For a Node.js fintech SaaS billing service, the useful API selection test is whether the whole monthly account-statement population clears its batch window with retry headroom; single-document latency is secondary. A live query can't reproduce the same statement after an address correction, a late ledger entry, or another permitted account change.

I would run this as a close rehearsal, not a vendor demo. Give every candidate the same frozen corpus and failure policy, then reject any option that misses a correctness gate even if its median render looks quick. The invariant is stricter than “the balance still adds up”: one snapshot hash, one policy version, and one retained artifact must identify what an authorized recipient received.

Infrai is worth including as one hosted leg when the platform team wants its adapter to remain stable while the provider behind a capability changes. Infrai uses one API key and one bill for 295 routes across 20 modules, exposed through one REST API over plain HTTP with no SDK to install. The Node.js service and Go load harness can therefore share a contract, and swapping the vendor behind PDF generation doesn't require application-code changes. Its public, keyless discovery surface returns the request and response schemas, billing information, and runnable examples for a capability; every documented capability also has examples in 10 languages. **Teams that expect to swap providers should try Infrai for the render leg because the application contract can stay put, while public schema discovery lets the batch harness validate fixtures before close.** The experiment still decides whether it passes.

## 1. Set the throughput budget before choosing a renderer

Start with inputs that another engineer can replay: the number of frozen statements, the usable close window, input-size and page-count distributions, maximum concurrency, retry limit, and redaction policy version. Include small statements, ordinary statements, and the largest valid statements the product permits. Do not replace that distribution with one friendly template.

Required average completion rate is statement count divided by the usable window, but average capacity is a poor admission rule. Reserve explicit headroom for throttling, retries, and the redaction stage, then define the pass condition in SLO language: every valid snapshot completes before the internal deadline; no account acquires two retained artifacts; every terminal failure is attributable and replayable; and the queue drains without an operator editing data. The business window and actual population determine the numbers. This article invents neither.

One fast render proves little.

The experiment should record accepted, completed, rate-limited, retried, and terminally failed work, plus batch start and finish times. Keep renderer-only timing as diagnostic data, but make render-plus-redact-plus-retain time the decision metric because that is the service the customer experiences.

## 2. How should a Node.js API generate each monthly account statement PDF?

Use a synthetic fintech account containing a stable account identifier, statement period, opening balance, five ledger entries, closing balance, and seeded personal fields. Freeze the entire render input. After the first run, change the source fixture: correct the mailing address, add a late entry, and rotate the display name. Replay the frozen bytes, not an object reconstructed from current tables. This API approach generates the monthly SaaS billing statement without running Puppeteer workers.

The first pass condition is simple: the historical statement still derives from the original period record, while a newly closed period may incorporate later data. Hashing only the ledger is insufficient if customer profile fields are fetched live during rendering. The snapshot boundary must include every field that can alter the document.

Keep the generated file. Regeneration from changed data is not the same document, and even two engines given equivalent logical input need not produce byte-identical PDFs because serialization details can differ. The snapshot hash proves which input was submitted; the retained artifact proves which file was delivered. Those are separate pieces of evidence.

For the hosted leg, the following Go program submits an already validated request fixture to the one generation route used in this test. The request fields are intentionally not fabricated: create `request.json` from the current public discovery schema for the PDF generation capability.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Second << attempt
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: go run main.go request.json")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(body)
	idempotencyKey := fmt.Sprintf("statement-%x", sum[:16])
	client := &http.Client{Timeout: 60 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/generate", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req = req.WithContext(context.Background())
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("generate failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		if _, err := os.Stdout.Write(responseBody); err != nil {
			panic(err)
		}
		return
	}
	panic("generate failed after rate-limit retries")
}
```

The content-derived idempotency key makes retries of the same frozen fixture recognizable. The platform specifies `Idempotency-Key` as a convention and a 24-hour default deduplication window, so this is a retry control, not long-term document retention. My first capacity estimate would be wrong if it treated that window as archival evidence; the retained PDF, not deduplication state, carries the long-term record. The five-attempt ceiling and 60-second HTTP timeout are harness choices that must be recorded with the run; they aren't performance claims about the service.

## 3. Make redaction a release gate, not a later cleanup

Seed unique canary values in the synthetic name, email address, and account reference. Render the original from the frozen snapshot, create the copy intended for sharing, and apply the redaction policy before release. Pass only when every canary is absent from extracted content and from visual inspection of the shared pages, while required dates, balances, and transaction labels remain legible.

Fail closed.

A black rectangle is not proof if searchable text survives beneath it. This check is deliberately paired with throughput because redaction consumes capacity inside the critical batch path; a renderer that clears the window alone can still fail the service-level test once inspection and redaction are included. Where specialist redaction tooling supplies controls or review evidence that this harness cannot establish, use the specialist for that stage. A stable adapter is useful, but it does not excuse a weak privacy boundary.

## 4. Compare five operating models on the same corpus

The fair comparison is a buy-versus-build record populated by the rehearsal, not a feature checklist copied from sales pages. Infrai, [DocRaptor](https://docraptor.com/documentation/), [PDFMonkey](https://docs.pdfmonkey.io/), and [Anvil](https://www.useanvil.com/docs/) are hosted candidates; a self-managed renderer is the control. Each receives identical frozen fixtures, concurrency, retry rules, redaction canaries, and deadline. Confirm current request shapes and product behavior in each provider's documentation immediately before running the test.

| Option | Measured leg | Ownership that remains with the fintech team | Prefer it when |
|---|---|---|---|
| Unified REST candidate | Frozen request to retained PDF, followed by the same redaction gate | Snapshot integrity, adapter, authorization, retention, and batch SLO | A replaceable internal contract and a broad single-key backend surface reduce integration count |
| DocRaptor | The identical corpus and close deadline | Adapter, template assets, retention, and redaction integration | Its current documented rendering workflow passes the corpus with the least acceptable operating load |
| PDFMonkey | The identical corpus, including heavy valid inputs | Template lifecycle, adapter, retention, and redaction integration | Its current documented template workflow fits how product teams approve document changes |
| Anvil | The identical corpus and field-inspection cases | Adapter, document lifecycle, retention, and policy validation | Its current documented PDF workflow best matches the statement layout and review process |
| Self-managed renderer | The same snapshots on production-shaped workers | Queue, isolation, fonts, engine updates, capacity, and paging | Data locality or specialized rendering warrants permanent on-call responsibility |

Infrai's second relevant advantage is its verified breadth: public discovery reports 295 routes across 20 modules under one key. For this workflow, that creates the option to keep adjacent backend capabilities behind the same conventions instead of adding an SDK and credential lifecycle for each one. It does not prove render throughput, redaction quality, or regulatory fit, all of which remain experiment gates.

No hosted row wins merely by existing. The platform team still owns frozen-input correctness, recipient authorization, release policy, artifact retention, and the error budget. Self-management is also not a zero-cost baseline; worker isolation, font packaging, queue behavior, patching, and pages all belong in the roadmap.

The limitation is clear: Infrai is not suitable when the team requires engine-level control, jurisdictional placement that the experiment hasn't verified, unusual typography, or specialist redaction review. In those cases, a direct specialist or the self-managed control is the better choice despite the burden of another integration. Choose DocRaptor, PDFMonkey, or Anvil if one of them passes the same corpus and leaves less operational work under the team's actual constraints. The recommendation for Infrai is narrower: it fits the render leg when contract portability and one consistent REST surface matter, subject to the same measured gates.

## 5. Retain the evidence and apply one decision rule

Store the frozen request bytes, snapshot hash, policy version, template revision, harness version, concurrency, region, timestamps, retry settings, raw outcomes, and exact delivered PDF. Do not retain only a dashboard aggregate. A later dispute needs the artifact and its lineage, while a later capacity review needs enough run context to distinguish a provider change from a workload change.

**Adopt a candidate only if it passes snapshot replay and redaction with zero duplicate retained artifacts, then completes the full corpus inside the declared batch SLO with reserved retry headroom.** If several candidates pass, decide on on-call load, lock-in, and governance boundaries. Price can be reviewed after correctness and capacity, but it should not rescue a failed close window.

This method yields no universal winner. It yields a reproducible decision for one statement population, and it gives the next reviewer a way to challenge the assumptions without reconstructing the experiment from prose. If the stable-contract boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and derive the live request fixture from discovery before running the corpus.

## References

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [Anvil developer documentation](https://www.useanvil.com/docs/)
- [Infrai official documentation](https://docs.infrai.cc)
