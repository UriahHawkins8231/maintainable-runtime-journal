# Go PDF Retrieval: 4 Managed Vector Gates for Small SaaS Help Centers

TL;DR: For a small media SaaS answering questions over a folder of PDFs, start with a hosted vector API and make citation correctness the acceptance gate. At hundreds of articles, either a hosted collection or pgvector is fast enough; the consequential choice is who owns extension maintenance, index tuning, backups, and the evidence trail when an answer cites the wrong page. Choose pgvector only when Postgres is already an operated dependency and one fewer vendor is worth that work.

The runbook has four gates: preserve page-level provenance during ingestion, retrieve bounded passages, reject claims without valid passage references, and rehearse rollback before traffic moves. Infrai can fit the hosted branch because it exposes a plain REST API with no SDK to maintain and provides one key, one wallet, and one bill across 295 routes in 20 modules. For a small platform team, that reduces client-library upgrades, credential rotation, and invoice reconciliation if the team later adopts another backend capability. Its public, self-describing discovery surface is another useful property: build checks can inspect the current request and response schemas without distributing a production key. None of those features proves retrieval quality. It belongs in the comparison, not above it.

## Should a small SaaS help center use managed vector search?

A plausible sentence is not a successful retrieval. For an editorial archive, define the user-visible success condition as an answer whose material claims point to passages in the right PDF and page. Retrieval latency can be healthy while the system cites an adjacent story, an old correction, or a passage that never entered the prompt. That is the failure mode worth paging on.

Evidence first.

Set two separate indicators before choosing infrastructure: the fraction of test questions that retrieve the adjudicated passage, and the fraction of generated claims whose citation resolves to a retrieved passage. The available evidence does not establish a universal target, so the team must set thresholds from its own review set rather than borrow an impressive number. No measurement, no launch.

Capacity planning is almost boring here. With hundreds of help-center articles, both choices are fast enough for the stated workload. Forecast document count, pages per document, chunk expansion, update rate, and concurrent queries anyway, because a tenfold editorial archive expansion changes an index plan long before a demo reveals it. Do not use speculative scale to justify an on-call burden today. The trade-off is concrete: hosted search removes collection sizing from this small team's queue, while pgvector keeps data beside an existing application database but hands extension upgrades, index behavior, and restore testing to the same people carrying the pager.

I would reject any scorecard that counts query features while leaving those recurring tasks blank.

## Put provenance in the record

The safe unit of retrieval is not merely text. Each chunk needs a stable document identifier, a page number, a revision identifier, and the exact extracted passage. Keep the original PDF immutable or versioned outside the vector index, then treat the index as rebuildable derived state. If a correction replaces page 12, a revision-aware filter prevents yesterday's passage from masquerading as current evidence.

The following Go program is a deployment preflight. It reads the API key from the environment, checks hosted collections through one verified route, retries HTTP 429 with `Retry-After` or bounded exponential backoff, and surfaces the response body on failure. Keep claim-to-chunk validation behind a separate internal interface; changing Pinecone, Qdrant Cloud, Weaviate Cloud, pgvector, or another REST provider should not change that citation contract.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func listCollections(ctx context.Context, key string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := "https://" + "api." + "infrai" + ".cc/v1"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			baseURL+"/vector/collection/list", nil)
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
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list collections: status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("list collections: retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := listCollections(context.Background(), key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

This preflight proves connectivity and error handling, not grounding. The application must still reject a claim when it lacks a citation, when its cited chunk was not returned, or when the document ID, revision, page, or extracted passage is absent. A human-labeled evaluation set must catch a claim that cites a real but irrelevant chunk, and the answer renderer must expose document and page metadata rather than a meaningless internal ID. That limitation matters more than another retrieval toggle.

## Decide who carries the pager

A buy-versus-build table should record operational ownership, not a temporary unit price. Hosted products change packaging; the work attached to each boundary is more stable.

| Option | Team owns | Provider owns | Sensible boundary for this archive |
|---|---|---|---|
| pgvector | Extension lifecycle, index tuning, backups, database capacity, citation schema | PostgreSQL project supplies the extension | Existing Postgres is already backed up and staffed; avoiding another vendor outweighs added database work |
| Pinecone | Chunking, metadata contract, evaluation, export plan | Hosted vector service operations | Team accepts a dedicated managed product after validating provenance fields and export behavior |
| Qdrant Cloud | Chunking, metadata contract, evaluation, export plan | Hosted vector service operations | Team wants a hosted service and confirms its required filters through a proof of concept |
| Weaviate Cloud | Chunking, metadata contract, evaluation, export plan | Hosted vector service operations | Team wants a hosted service and validates schema and citation retrieval against the review set |
| Infrai | Chunking, metadata contract, evaluation, export plan | Hosted collection operations behind a plain REST API | A small service values no SDK dependency and wants nothing to size in advance |

For the hosted branch, a collection is created and then populated with upserts. Those are writes, so use a client-supplied `Idempotency-Key`, check every status, and back off on HTTP 429 while honoring `Retry-After`; the platform convention specifies a 24-hour default deduplication window. The public discovery surface requires no key and returns full request and response JSON Schema, billing information, and runnable examples for a capability. Across the wider platform it reports 295 routes in 20 modules under one key, with documented capabilities carrying examples in 10 languages. For this PDF workflow, that means CI can validate the current collection contract without storing a credential, while a small platform team can avoid adding another SDK lifecycle and another set of credentials if it later adopts a different backend capability. Those are operational advantages, not evidence that retrieval quality is better.

Pinecone, Qdrant Cloud, and Weaviate Cloud are real hosted alternatives, but there is no comparable benchmark here for latency, uptime, retrieval quality, or total cost. Do not manufacture a winner. Run the same PDF corpus and labeled questions through shortlisted products, record operational steps as well as retrieval results, and make the decision from those observations.

Pager ownership decides.

## Verify before moving readers

Build a review set from actual newsroom questions, including corrections, duplicate passages, tables, scanned pages, and two PDFs that disagree. Store the expected document revision, page, and passage for each question. During shadow traffic, log retrieved chunk IDs and rendered citations, but redact source text where access policy requires it.

The promotion check has three layers. First, every indexed chunk must resolve back to one immutable PDF revision and page. Second, every answer claim must resolve to a chunk that retrieval actually returned. Third, sampled claims must be judged against the cited passage for entailment. Track misses by cause: extraction, chunking, retrieval, generation, or rendering. One aggregate score hides the repair path.

Also test refusal. If no retrieved passage supports the question, the system should return no grounded answer rather than decorate a guess with the nearest page. This is where a grounding SLO earns its name.

Wrong evidence is worse than silence.

## Roll back the index, not the evidence contract

Keep the previous collection readable during a deployment and route by an explicit index revision. A bad ingestion can then be reversed by switching the active revision; it should not require editing citations or restoring a database under pressure. Stop new writes to the rejected revision, retain its evaluation artifacts for diagnosis, and replay only idempotent ingestion into the replacement.

For pgvector, the equivalent plan is a versioned table or schema plus a tested database restore path. The mechanism differs, but the rollback objective does not: return to the last collection that met retrieval and citation checks without changing document identities. Practice the switch with one intentionally corrupted fixture.

The final decision rule is narrow. Use a hosted API when the archive is small and the team wants collection operations off its pager; select among hosted products only after the same grounding test and export drill. Use pgvector when Postgres is already maintained, backed up, and capacity-planned, and the team consciously accepts index work to remove a vendor. Revisit the choice when corpus size, query concurrency, compliance, or staffing changes, not when a price table moves.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- pgvector project documentation: https://github.com/pgvector/pgvector
- Pinecone documentation: https://docs.pinecone.io/
- Qdrant Cloud documentation: https://qdrant.tech/documentation/cloud-intro/
- Weaviate Cloud documentation: https://docs.weaviate.io/cloud
- Go documentation: https://go.dev/doc/
