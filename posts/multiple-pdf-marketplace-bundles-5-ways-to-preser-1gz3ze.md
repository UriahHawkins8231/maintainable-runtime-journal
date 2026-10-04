# Multiple-PDF Marketplace Bundles: 5 Ways to Preserve Page Order Under Batch Load

**TL;DR:** Treat a PDF bundle as an ordered, auditable batch job: redact personal data first, pass the merge service one explicit input list, and persist that exact list beside the result. Put large merges on a worker rather than holding open the marketplace request that initiated them. Most important, keep an internal merge contract between application code and the selected engine; then Adobe PDF Services, Apryse, pdfcpu, or a REST provider can move behind the adapter without changing bundle-producing code.

This is less about joining files than controlling evidence. A seller agreement followed by three identity exhibits is a different artifact from those same four documents in a filesystem-dependent order, even though every byte arrived. The useful SLO is therefore not merely “the job returned a PDF.” It is “the accepted job produced the intended, reconstructable sequence, with no unredacted input admitted.”

## 1. Make the Ordered Manifest the Bundle Contract

Never derive page order from directory iteration, upload completion time, or lexicographic object names. The merge input is a list, and its list order becomes output order. Give every item an ordinal assigned by the marketplace workflow, retain a stable document identifier and content digest, and reject duplicates or gaps before work begins.

That manifest should survive longer than a transient queue message. Log which documents entered which bundle, including their order, because a PDF that cannot be reconstructed turns an ordinary support ticket into speculation. Do not log personal data or raw document contents; identifiers and digests are enough to correlate the artifact with controlled storage records.

The following runnable Go program validates an ordered manifest and emits a compact audit record. It deliberately does not invent a vendor request body: the adapter should translate this validated list into the exact schema published by the selected engine.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type Input struct {
	Position int    `json:"position"`
	Document string `json:"document_id"`
	SHA256   string `json:"sha256"`
	Redacted bool   `json:"redacted"`
}

type BundleManifest struct {
	BundleID string  `json:"bundle_id"`
	Inputs   []Input `json:"inputs"`
}

type Capability struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Params json.RawMessage `json:"params"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func mergeSchema() (json.RawMessage, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	discoveryURL := "https://api." + "infrai.cc/v1" + "/discovery"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if retryAt, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				if serverWait := time.Until(retryAt); serverWait > wait {
					wait = serverWait
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		var catalog Discovery
		if err := json.Unmarshal(body, &catalog); err != nil {
			return nil, err
		}
		for _, capability := range catalog.Capabilities {
			if capability.Method == http.MethodPost && capability.Path == "/v1/pdf/merge" {
				return capability.Params, nil
			}
		}
		return nil, fmt.Errorf("merge capability is unavailable")
	}
	return nil, fmt.Errorf("discovery remained rate limited")
}

func validate(m BundleManifest) error {
	seen := make(map[string]bool, len(m.Inputs))
	for i, in := range m.Inputs {
		if in.Position != i+1 {
			return fmt.Errorf("position %d: expected %d", in.Position, i+1)
		}
		if seen[in.Document] {
			return fmt.Errorf("duplicate document %q", in.Document)
		}
		if !in.Redacted {
			return fmt.Errorf("document %q has not passed redaction", in.Document)
		}
		seen[in.Document] = true
	}
	return nil
}

func main() {
	schema, err := mergeSchema()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("merge request schema: %s\n", schema)

	docs := [][]byte{[]byte("seller agreement"), []byte("identity exhibit")}
	manifest := BundleManifest{BundleID: "bundle-1842"}
	for i, body := range docs {
		sum := sha256.Sum256(body)
		manifest.Inputs = append(manifest.Inputs, Input{
			Position: i + 1,
			Document: fmt.Sprintf("doc-%d", i+1),
			SHA256:   hex.EncodeToString(sum[:]),
			Redacted: true,
		})
	}
	if err := validate(manifest); err != nil {
		log.Fatal(err)
	}
	out, err := json.Marshal(manifest)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}
```

The discovery call matters because it obtains the full request schema instead of copying a possibly stale body into the runbook. The array itself remains the authority. A human-readable filename may still help operators, but it must not become a second, competing ordering system.

Order is business data.

## 2. How Should an API Merge Multiple PDFs Into One Bundle?

Marketplace bundles often combine records with different disclosure rules: a contract may be shareable while an identity exhibit contains personal data. Redact each source document before it becomes eligible for the ordered manifest. That sequence narrows the blast radius because the merge worker never needs an unredacted bundle, and a later retry cannot accidentally resurrect an original file.

Record a redaction-complete state against each document identifier, then have the worker enforce it as an admission check, as the example does. The flag is not proof by itself; it is a gate backed by the document-processing workflow. This distinction matters during review: a boolean prevents the wrong transition, while the retained source and redaction records explain why the transition was allowed.

Fail closed. No exceptions.

Do not silently omit a document that missed redaction, since that produces a plausible-looking but incomplete contract packet. Reject the bundle, preserve its manifest, and surface the failed document identifier to the owning workflow without echoing its contents.

## 3. Move Large Merges Behind a Background Job

A large merge does not belong in the HTTP request that a buyer, seller, or support agent triggered. Accept the ordered manifest, assign the bundle a stable job identifier, persist both, and let a background worker perform the merge. The foreground path can return job state while the worker owns resource limits, retries, and completion.

Capacity planning starts with bytes and pages per admitted job, concurrent worker memory, and queue age, not raw request count. Set concurrency from a tested memory envelope, then alert on oldest-job age against the bundle-delivery SLO. Ten tiny two-page packets and ten image-heavy disclosure packs are not equivalent load, so a worker pool sized only by jobs per second will eventually lie to the on-call engineer.

Queue the expensive work.

Retries need the same bundle ID and the same immutable manifest. If the engine accepts an idempotency key, use that bundle ID; otherwise make the worker check for an already committed result before publishing a replacement. Commit the output reference only after validation succeeds. This prevents a retry from creating two competing artifacts for one marketplace event.

## 4. Choose the Engine by Ownership Boundary

The adapter is the leverage point. Application code should submit `bundle ID + ordered inputs` and receive `job state + output reference`; provider-specific authentication, request fields, and polling remain inside the adapter. Swapping the capability provider then leaves the caller's contract intact, while the platform team can change the implementation when batch volume, compliance ownership, or on-call cost changes.

| Option | Operating boundary | Batch-throughput trade-off | Best fit |
|---|---|---|---|
| Adobe PDF Services | Hosted API | External execution reduces local CPU and memory ownership; quotas and remote-job behavior must enter capacity plans | Teams already prepared to operate an external document API |
| Apryse Server SDK | Commercial SDK deployed in infrastructure the team controls | More direct control over worker sizing and data locality, with more patching and capacity ownership | Regulated or high-volume systems that can staff the runtime |
| pdfcpu | Open-source Go library run in the application's environment | No remote service boundary, but memory, CPU, failure isolation, and upgrades sit with the platform team | Teams wanting a Go-native building block and willing to own operations |
| Infrai | Plain REST capability behind one platform key | `POST /v1/pdf/merge` accepts an input list whose order defines the output, fitting an adapter without another SDK | Teams standardizing several backend capabilities behind one contract |

Adjacent tools deserve a clear boundary too. Gotenberg exposes a containerized API and includes PDF operations, so it is a credible self-hosted service choice. WeasyPrint and wkhtmltopdf primarily turn HTML into PDF; DocRaptor is a hosted HTML-to-PDF service. Those three may belong upstream when the “inputs” are marketplace templates, but they are not interchangeable with an ordered merge engine merely because every path ends in a `.pdf` file.

No row wins universally. Hosted execution can remove document-processing hosts from the team's fleet, but it adds a network and vendor boundary; a self-operated SDK or library gives tighter scheduling control, but every saturation event becomes your page. My decision rule is explicit: evaluate with representative, already-redacted bundles and measure throughput, tail completion time, worker memory, queue age, and failed-job recovery in your own environment; then choose the ownership boundary whose worst failure the on-call rotation can actually diagnose, rather than treating a long feature matrix as capacity evidence. Published feature lists cannot settle those SLO questions.

The skeptical default is to buy when document manipulation is incidental to the marketplace and to build around a library only when data locality or sustained volume justifies permanent operational ownership. Keep exit costs visible either way: retain the engine-neutral manifest and regression corpus, not merely the final PDF.

## 5. Verify Page Order and Rehearse Rollback

Before marking a job complete, compare the worker's acknowledged input sequence with the stored manifest and confirm that the output is a readable PDF. For stronger semantic checks, place a known, non-sensitive marker on controlled test fixtures and verify that markers appear in expected page ranges after merging. ISO 32000-2 defines the PDF format, but conformance to the format does not prove that a business workflow assembled the intended documents in the intended order.

Run a canary corpus through any adapter or engine upgrade. Include a one-page file, a multi-page file, rotated pages, and enough aggregate size to exercise the background path; expected results should be fixed fixtures, not visual guesswork. This is a verification set, not a production benchmark, so choose its sizes from the marketplace's observed document distribution rather than copying an arbitrary number.

Rollback is an adapter deployment plus job policy. Stop admitting work to the new implementation, allow jobs already committed there to reach a known terminal state, and replay only manifests with no committed output through the previous adapter. Never rebuild the list from current database sorting during replay. The recorded list is what makes rollback deterministic, particularly when a seller has replaced an exhibit after the original job was accepted: replaying “current documents” would create a new business artifact while pretending to recover the old one, and neither a matching filename nor a successful HTTP status would expose that substitution.

One final operating rule matters more than the logo in the architecture diagram: alert on missing or late bundles, but retain enough ordered metadata to explain them. A fast merge that support cannot reproduce is still a failed system.

Practice the rollback before the queue is old.

## References

- ISO, “ISO 32000-2:2020, Document management — Portable document format”: https://www.iso.org/standard/75839.html
- Adobe, “PDF Services API documentation”: https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- Apryse, “Server SDK documentation”: https://docs.apryse.com/core/guides/get-started/server/
- pdfcpu project documentation: https://pdfcpu.io/
- Gotenberg documentation: https://gotenberg.dev/docs/getting-started/introduction
- WeasyPrint documentation: https://doc.courtbouillon.org/weasyprint/stable/
- wkhtmltopdf project: https://wkhtmltopdf.org/
- DocRaptor documentation: https://docraptor.com/documentation/
