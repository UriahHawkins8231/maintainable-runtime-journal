# Private Report Delivery in Europe: Signed URLs, Object Storage, or Database Blobs?

Short answer: for a European media SaaS serving generated reports to authenticated customers, keep report bytes and image thumbnails in object storage, keep authorization and metadata in the database, and issue short-lived signed URLs only after the application has checked the customer session. Store a database blob only when the payload is small, transactional with its row, and unlikely to become a delivery workload.

This is a delivery runbook, not a storage-product ranking. The important boundary is access control versus delivery simplicity: the application owns the decision about who may read a report, while a storage service moves the bytes without consuming an application worker and a database connection for the whole download. That arrangement also leaves room to resize report images into separate immutable variants.

## Start with the revocation test, not the storage diagram

A generated report often has an awkward shape: a modest metadata row, a large document, and several image thumbnails produced later. Putting all of it in one database transaction feels safe, particularly when the report must remain private, but it ties authorization queries to byte transfer. A slow customer download can occupy a connection long after the query that proved access has finished. Peak capacity then depends on report size, concurrent readers, connection-pool limits, and application memory, not just on requests per second.

The operational symptoms are easy to confuse. A report SLO can miss because the authorization query is slow, because a renderer has not produced the requested thumbnail, because the database is moving a blob, or because the browser is receiving bytes slowly. A restore and replication stream carry the media payload as well as the records. The database is still capable of storing blobs; the question is whether its failure domain matches this workload.

The first design-review question is blunt: if a customer's access is revoked now, what stops a link issued earlier? If the answer is only “the browser will ask again,” the capability is too broad or too long-lived. Make revocation and tenant isolation testable before debating storage throughput.

The safer model is deliberately boring. The database row is authoritative for tenant, customer, report state, content type, dimensions, retention, and object keys. Object storage holds the original report and generated variants. An object existing does not grant access, and a database row pointing at an absent object is a reconciliation condition, not permission to guess.

For thumbnails, use a key derived from server-side identifiers, such as `tenant-42/report-8f2/medium.webp`. Do not accept an arbitrary object key from the browser. Generate a new variant, verify its write, and update the row only when that variant is ready. This makes retries and cleanup reviewable: the database describes intent, while the binary artifacts can be replaced.

## Can a European SaaS app deliver private user images and thumbnails with signed URLs?

The request should pass through four decisions. The customer presents an authenticated session to the application. The application loads the report's tenant and authorization state. It derives the exact variant key and asks the storage control plane for a signed URL. The browser then fetches that URL directly, without receiving a storage credential or adding the application's bearer token to the signed request.

Short-lived access is the point.

The URL lifetime should cover a normal page load and its retry window, not an entire customer account session. I am not sure there is a universal duration: a thumbnail grid, a multi-megabyte export, and a background report viewer have different exposure and completion times. Resolve that value from observed download duration and expired-link failures, then put the result in an SLO review rather than hiding failures with a very long URL lifetime.

The application should return an authorization denial and a missing-object result as different operational events, even if both become a safe not-found response at the edge. Log the tenant, report ID, variant, decision, and correlation ID; never log the signed query string. A signed URL is a capability, so access logs and support tooling must treat it as sensitive for its entire lifetime.

Here is the small part worth making identical across upload, resize, presign, and deletion workers. It rejects path traversal and keeps the key format in one place; it does not make an authorization decision.

```go
package reportkey

import (
	"fmt"
	"regexp"
)

var segment = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func VariantKey(tenantID, reportID, variant string) (string, error) {
	for label, value := range map[string]string{
		"tenant":  tenantID,
		"report":  reportID,
		"variant": variant,
	} {
		if !segment.MatchString(value) {
			return "", fmt.Errorf("invalid %s", label)
		}
	}
	return fmt.Sprintf("%s/%s/%s.webp", tenantID, reportID, variant), nil
}
```

The example intentionally stops before a vendor SDK or an invented endpoint. Storage providers differ in presigning APIs, retention controls, consistency details, and replication behavior; a generic article cannot safely fill those gaps. The production adapter should be tested against the chosen provider's documented method and request schema, while the authorization service above it stays unchanged.

## Where should the report record end and the bytes begin?

Use the following as a design-review table, not as a universal rule. The deciding inputs are payload size, read concurrency, transaction boundaries, restore objectives, and the team's tolerance for provider-specific integration.

| Choice | Fits when | The catch |
| --- | --- | --- |
| Database blob | The payload is small, rarely downloaded, and must commit atomically with its row | Reads and backups couple binary traffic to database capacity and recovery time |
| Object storage | Reports and thumbnails are larger, read independently, or generated asynchronously | The database and object store need reconciliation, lifecycle rules, and a cleanup job |
| A managed delivery layer | The team wants a narrow application adapter and less storage-client code | The abstraction may hide provider-specific features needed for retention, migration, or audit |
| Self-hosted object storage | Data placement, control, or an existing platform standard outweighs operational effort | The team owns capacity, upgrades, durability design, and the on-call surface |

The catch is important: object storage is not suitable when the report must participate in a strongly atomic multi-row transaction, when the organization cannot operate a reconciliation path, or when the chosen service cannot meet the region, retention, audit, or recovery requirements. Stick with a database blob for a small, infrequently read artifact whose transaction semantics are the requirement. Choose a directly managed storage service when public, cacheable media or advanced object governance is central and a thin adapter would conceal necessary controls.

The test is revocation.

For a platform team balancing managed services against self-hosting, lock-in is a capacity concern as much as a licensing concern. Count the people needed to maintain an adapter, restore data, rotate credentials, answer audit questions, and migrate keys. Also count what becomes harder to inspect when a managed layer presents one simplified interface. A single REST contract can reduce SDK and language friction, but it does not erase the storage system's durability, retention, or regional constraints.

## How can a delivery SLO survive the renderer queue?

Roll out one tenant cohort and keep the old report pointer available until the new object and its thumbnail variants have passed verification. The rollout should have a reversible pointer change, not a last-minute byte migration under customer traffic.

Before expanding, test the positive path with an authenticated customer, then test the negative paths: a different tenant cannot obtain a URL; a browser cannot choose another report key; an expired URL no longer authorizes delivery; a report whose variant is still generating does not pretend to be ready; and deleting an ownership record prevents new URLs. Test duplicate resize messages too. The expected result is one logical variant, not several competing objects that the cleanup job must interpret. For one report, trace the state from `queued` to `rendering` to `ready`, then delete the database pointer while the object remains. The next authorization request must stop at the database decision, the old object must not become public by accident, and the reconciliation job must be able to identify an object with no live owner. Repeat the same exercise with a failed write and with two workers receiving the same resize message; the useful evidence is not that the happy path works, but that each partial state has one owner, one observable alarm, and one bounded cleanup action.

Measure separate stages. The useful signals are authorization latency, signed-URL issuance outcomes, renderer queue age, variant-ready lag, byte-delivery latency, download completion rate, and orphan counts in both directions. Set an SLO for the customer-visible path and supporting budgets for the renderer and authorization stages. A single `report_download_failed` counter cannot tell the on-call engineer which budget is burning.

Capacity planning should use the real report mix: concurrent viewers multiplied by bytes per report, plus thumbnail bursts when a customer opens a report index. Watch database connection usage and application memory while delivery traffic rises. The target is not a decorative benchmark; it is evidence that a slow download does not consume the same scarce resources needed to authorize a new customer or enqueue a new report.

Rollback is a state transition. Mark the new variant unavailable, stop issuing new URLs for it, and leave already-issued URLs to expire naturally unless the security response requires earlier revocation. Readers fall back to the last verified variant or the previous delivery pointer. Keep the object for investigation until the retention policy allows cleanup, and record the reason for the pointer change. If the migration cannot preserve a fallback, it is not yet a rollback plan.

The final decision should be recorded with the payload threshold, expected read rate, URL lifetime, region and retention requirements, restore target, and owner of the reconciliation job. Those values will change; the boundary and the evidence should not become tribal knowledge.

## References

- https://developers.cloudflare.com/r2/
- https://developers.cloudflare.com/workers/
