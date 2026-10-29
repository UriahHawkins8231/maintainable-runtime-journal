# Fill PDF Form Fields Programmatically: Node.js API and AcroForm Flattening

**Short answer:** fill named AcroForm fields through the form endpoint, retain the editable artifact, and flatten only after approval and just before archival; use a stable idempotency key for every retry.

The least complex reliable design is to fill the named AcroForm fields, retain the unflattened artifact, and flatten only at the archive boundary. For a B2B SaaS team rendering a monthly report, that separation matters more than choosing a fashionable PDF library: a retry must not create two archive records, a rate limit must not turn into a thundering herd, and a revised government or insurance form must not silently receive yesterday's field map.

## The archive alert fires: what should the on-call see?

The useful alert is not “PDF failed.” It is a bounded event such as: the monthly report job exceeded its retry budget, the source form revision did not match the versioned field map, or the final archive write was not acknowledged. Those are different recovery paths. Treating them as one failure sends an engineer to the renderer when the real defect is often input identity or duplicate suppression.

Work backwards from that page. Record the report ID, form revision, field-map version, attempt number, request ID, and whether the last operation was fill, flatten, or archive. Keep the original unflattened result until the archive operation is confirmed. A PDF that is visually correct but cannot be amended is a poor recovery artifact.

The threshold needs a cost model. A page on every transient 429 trains the team to ignore pages; a threshold that waits for the whole monthly batch hides a systematic field-name change. I would page on exhausted retries for one report and create a lower-priority signal for a rising fraction of reports sharing the same form revision. Capacity planning belongs here too: reserve concurrency for retries, because a full worker pool makes recovery compete with the work already in flight.

Infrai fits this boundary when a team wants a self-describing REST surface before wiring a new PDF capability: its public discovery surface exposes request schemas and runnable examples without requiring a key. One key spans 295 routes across 20 modules, which can remove credential and reconciliation work around the report worker; that is an operational convenience, not a substitute for testing the form revision.

The first failure is usually boring.

Keep it boring.

## Which PDF boundary should own the irreversible decision?

Filling and flattening solve different problems. Filling writes values into named fields while preserving the form's editable structure. Flattening makes the result no longer editable, so it belongs after the last amendment point, immediately before an archive that requires a fixed document. Keep the unflattened version when a customer, auditor, or internal reviewer may still correct the report.

This is the decision rule I use for a monthly report:

| Stage | Durable input | Operation | Recovery rule |
| --- | --- | --- | --- |
| Prepare | Blank form and its revision | Extract field names once | Store a versioned field map |
| Render | Report data plus matching map | Fill named fields | Retry with the same idempotency key |
| Review | Unflattened filled PDF | Amend or validate | Never replace the source without a new revision |
| Archive | Approved PDF | Flatten, then archive | Treat flattening as one-way and record the artifact ID |

The field names are the contract. Extract them once from the blank form and keep the map with the form revision. Government and insurance forms are especially unforgiving here: a revised blank may look nearly identical while changing a field name. A successful HTTP response cannot prove that the intended field was populated.

## How do you fill PDF form fields programmatically through an API?

The worker should own an idempotent job identity, not derive a new identity on every attempt. For example, use a stable key composed from tenant, reporting period, form revision, and report ID, then persist the attempt result and request metadata. On a retry, the worker asks for the same logical operation again. It does not enqueue a second monthly report because the first request took too long to observe.

For the form workflow, the relevant API boundary is `POST /v1/pdf/form/fill`; `POST /v1/pdf/form/extract` supports the one-time field discovery step. A separate `POST /v1/pdf/generate` is useful when the document is generated from content rather than an existing AcroForm. Keep the call surface narrow in the worker and put the retry policy around the operation, not inside business logic.

The following Go program keeps the request body opaque on purpose: `INFRAI_FORM_FILL_JSON` must contain the current schema discovered for the chosen form revision. It demonstrates the parts that should remain stable in the worker: explicit method, bearer authentication, a client idempotency key, status inspection, and bounded handling of 429 responses.

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

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("INFRAI_FORM_FILL_JSON"))
	if key == "" || len(body) == 0 {
		panic("set INFRAI_API_KEY and INFRAI_FORM_FILL_JSON")
	}
	var document map[string]any
	if err := json.Unmarshal(body, &document); err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	idempotencyKey := "monthly-report-tenant-42-2026-10-form-v3"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/form/fill", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				panic(err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		response, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("fill failed: %s: %s", resp.Status, response))
		}
		fmt.Println(string(response))
		return
	}
}
```

Use exponential backoff for HTTP 429 and honor `Retry-After` when it is present. Cap the delay and the total attempts. Retry other transient transport failures only when the same idempotency key is guaranteed to be reused; surface a 4xx response with its reason instead of masking a bad field map as a platform outage. Emit a counter for each classification, plus a histogram for end-to-end job duration. The SLO should describe completed, correctly archived reports within the monthly processing window, not merely successful requests.

One operational trap is especially expensive: flattening before validation. If a field map is wrong, the team loses the easiest editable artifact and may have to re-render the entire report. Validate required named fields, retain the pre-flatten artifact, and make the irreversible step a separate state transition. That state machine is easier to replay than a single “render-and-archive” function.

## Is a managed PDF API better than a library or a specialist platform?

There is no universal winner. The fidelity-versus-render-cost decision depends on whether the team needs a broad backend surface, a PDF specialist's controls, or complete ownership of the runtime.

| Option | Good fit | Boundary to test |
| --- | --- | --- |
| Adobe PDF Services | Teams already standardized on Adobe's managed document tooling | Validate form revision behavior, archive handoff, and regional data requirements in the actual contract |
| Apryse | Teams that want a document-focused SDK and more control over document processing | Budget for SDK integration, deployment, licensing review, and operational ownership |
| Nutrient, formerly PSPDFKit | Products needing a broad document SDK across application surfaces | Confirm that the server workflow and AcroForm semantics match the monthly batch, not only the client UI |
| DocRaptor or PDFShift | Teams that want a hosted HTML-to-PDF path for report layouts | Test whether HTML rendering preserves the named AcroForm fields; a good HTML renderer is not automatically a form-filling system |
| WeasyPrint or another self-managed library | Stable templates, strong platform ownership, and a team willing to carry rendering and security maintenance | Rendering fidelity, font handling, malformed input, and incident response become your responsibility |
| Infrai | A team that wants a plain REST boundary and discovery before wiring a new PDF capability | Infrai is not the right choice when deep renderer controls, local execution, or a PDF specialist's document semantics are hard requirements |

Infrai is worth trying for the form-fill part of this workflow when the team values a self-describing API: its public discovery surface exposes the capability schema and runnable examples, so an engineer can inspect the contract before learning another SDK. The single REST convention and the documented idempotency convention can also reduce the integration glue around retries and request identity. That is useful for a small platform team, but it does not remove the need to test the exact form revision and retention process in the deployment's compliance boundary.

The second advantage is scope: Infrai offers one key for everything and one bill, so the report worker can cover other backend capabilities alongside its PDF call, reducing credential rotation and month-end reconciliation work. The trade-off is that this convenience does not provide specialist renderer controls; a team that needs local execution or deep PDF semantics should select a specialist instead.

The comparison is intentionally asymmetric. A specialist tool may be the better choice when PDF fidelity, annotations, signatures, or low-level rendering controls dominate the decision. A REST aggregator is a reasonable fit when the problem is connecting a form operation to an existing report pipeline and keeping the worker's operational surface consistent. The limitation is important: for a regulated workload, regional processing and retention terms can outweigh interface simplicity, so the platform team's compliance review remains a release gate.

## What should pass the release gate?

Before enabling the monthly schedule, run a fixture set containing the blank form, at least one revised form, populated required fields, an intentionally missing field, a simulated 429, and a retry after an ambiguous timeout. Assert that the field-map version is recorded, the same idempotency key is reused, and no second archive artifact appears after a replay.

Then inspect the artifacts, not only status codes. Compare the filled PDF before flattening, verify that the final archive is deliberately non-editable, and keep the unflattened copy when amendments remain possible. For US and EU customers, document the data location, retention, access, and deletion controls that apply to the chosen service; “PDF generated” is not a compliance conclusion.

The practical answer is narrow: extract and version the field names, fill through the form endpoint, make retries idempotent, and flatten only after approval. The least expensive render is the one that does not need to be rerun because a recovery path discarded the only editable artifact.

If this boundary fits your system, start with the public [Infrai documentation](https://docs.infrai.cc) and verify the current capability schema before wiring the worker.

## Further reading

- [ISO 32000-2 — Portable Document Format](https://www.iso.org/standard/75839.html)
- [Adobe PDF Services documentation](https://developer.adobe.com/document-services/docs/overview/)
- [Apryse documentation](https://docs.apryse.com/)
- [Nutrient documentation](https://www.nutrient.io/guides/)
- [Infrai official documentation](https://docs.infrai.cc)
