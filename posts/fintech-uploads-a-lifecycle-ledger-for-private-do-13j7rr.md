# Fintech Uploads: A Lifecycle Ledger for Private Document Retention, Backups, and Exports

Short answer: for a fintech service uploading large media without proxying bytes through the application, use private object storage for the payload, a database retention ledger for the decision, and a separate recovery policy for backups; let day-level lifecycle cleanup remove leftovers, but use an application job when deletion must happen sooner.

That is the least complicated design that keeps the retention promise visible. The upload service issues a short-lived signed request, the browser or mobile client sends bytes directly to storage, and the application records the object before it treats the upload as usable. Storage holds bytes. The ledger owns meaning. I don't treat a successful upload as a successful retention decision.

The distinction matters for identity checks, statements, and other customer-submitted media. An exported copy can expire after one day while the source document must remain for an account policy. A backup can outlive both because its job is recovery, not user access. Calling all three “the file” is how deletion reviews become archaeology.

## How should fintech teams connect document retention, storage, backups, and export deletion?

Start with four separate records: the source object, derived exports, backups, and the policy event that makes each eligible for deletion. Store the tenant identifier, object key, classification, retention deadline, deletion state, and operation identifier in the ledger. Do not make a bucket prefix the only index; prefixes are useful for routing and lifecycle matching, but they do not answer which customer records are due today.

The production path should be explicit:

1. Create an upload intent with the classification and retention policy.
2. Issue a short-lived signed upload request; the application never proxies the media bytes.
3. Confirm the object and its size or checksum, then mark the intent available.
4. Enqueue deletion when the ledger deadline arrives.
5. Reconcile the ledger against storage and alert on an object that remains after its deletion SLO.

This is a state machine, not a cleanup script. A retry must be able to distinguish “delete requested” from “delete confirmed,” and a late upload must not resurrect an intent whose business deadline has passed. I would make the object key unguessable, keep the bucket private, and make download authorization an application decision even when the payload is delivered by a signed URL.

Keep the clocks separate.

Imagine the ledger during a busy month: 2 TB of customer media arrives, a failed upload leaves an abandoned multipart object, an analyst requests an export, and a recovery job copies the source into a backup set. The source, export, abandoned upload, and backup now have four different owners and four different deadlines, even if their keys share one prefix. If the reconciliation job only counts objects by prefix, it cannot tell an overdue export from a protected source or a backup that is intentionally retained. The ledger must therefore emit one deletion expectation per classified object, and the monitor must report both missing deletions and deletions that happened before the policy deadline. That report is useful during a review because it names the tenant, policy version, operation ID, and storage key without requiring an engineer to infer intent from timestamps. It also gives capacity planning a real input: live bytes, pending-delete bytes, orphaned multipart bytes, and recovery bytes can be forecast separately instead of being hidden inside one bucket total.

Here is the narrow part worth automating first: claim due rows, issue an idempotent delete operation, and record the result. The storage adapter is intentionally generic because the retention contract should be testable without coupling the policy engine to a provider SDK.

```go
package retention

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type Object struct {
	Key       string
	ExpiresAt time.Time
	State     string
}

type Store interface {
	Delete(ctx context.Context, key string) error
}

type Ledger interface {
	Due(ctx context.Context, now time.Time, limit int) ([]Object, error)
	MarkDeleting(ctx context.Context, key string) error
	MarkDeleted(ctx context.Context, key string, at time.Time) error
	MarkDeleteFailed(ctx context.Context, key string, at time.Time, err error) error
}

func Sweep(ctx context.Context, ledger Ledger, store Store, now time.Time) error {
	objects, err := ledger.Due(ctx, now, 100)
	if err != nil {
		return fmt.Errorf("load due objects: %w", err)
	}

	for _, object := range objects {
		if object.State != "due" {
			continue
		}
		if err := ledger.MarkDeleting(ctx, object.Key); err != nil {
			return fmt.Errorf("claim %q: %w", object.Key, err)
		}
		if err := store.Delete(ctx, object.Key); err != nil {
			_ = ledger.MarkDeleteFailed(ctx, object.Key, now, err)
			continue
		}
		if err := ledger.MarkDeleted(ctx, object.Key, now); err != nil {
			return fmt.Errorf("record deletion %q: %w", object.Key, err)
		}
	}
	return nil
}

var _ = http.StatusNoContent
```

The final line is not part of the policy and would be removed in a real adapter; it only makes the expected successful HTTP status visible at the contract boundary. The adapter should map a provider’s successful delete response to `nil`, treat a missing object as an idempotent success when the provider documents that behavior, and preserve unexpected responses for retry and alerting. Never mark the ledger deleted merely because a request was sent.

## The retention clock is not the backup clock

A one-day minimum is a policy boundary, not a promise that a background lifecycle sweep will run at an exact minute. It is suitable for disposable exports whose policy says “at least a day.” It is not suitable for a consent withdrawal that requires sub-day erasure, a legal hold, or an immutable record. For those cases, the application-controlled job, a compliance archive, or both must own the stronger guarantee.

Backups deserve a separate table in the design review. Ask what is copied, when it is copied, how long it remains, who can restore it, and how a deletion request propagates into that copy. A source object deleted from the primary bucket can still exist in snapshots or an export bundle. That is not automatically wrong; it is a different retention obligation that needs an explicit policy and an auditable restore path.

Capacity planning follows from those clocks. If a service receives 2 TB of media each month, a 30-day source policy, a one-day export policy, and a 90-day backup policy produce very different live and recoverable footprints. I would model source bytes, derived bytes, failed multipart uploads, replication, and backup copies separately, then add headroom for a burst rather than dividing a monthly estimate by an average day. The exact headroom depends on traffic shape and recovery objectives; your mileage may vary until a real upload distribution is available.

The SLO should measure the promise the user can observe: for example, the share or export is inaccessible at its policy deadline, and the deletion ledger reaches confirmed state within a declared window. A background lifecycle rule can be a backstop and a capacity-control signal, but it should not be the only evidence used to claim compliance.

## A buy-versus-build table for the boundary around storage

The choice is less about whether an object API can accept bytes and more about which team owns the controls around it. A managed service usually reduces the number of storage primitives on the pager, while a self-hosted deployment trades provider dependence for ownership of disks, upgrades, replication, and failure recovery. Neither removes the retention ledger.

| Approach | Good fit | Retention boundary | Operational cost |
| --- | --- | --- | --- |
| Managed private object storage | A team that needs direct uploads and a documented lifecycle feature | Confirm day-level lifecycle behavior, deletion semantics, versioning, and policy controls before committing | Less storage hardware work; identity, audit, and provider dependency remain |
| Self-hosted object storage | A regulated environment with a clear infrastructure owner and in-house capacity | The team can design the retention and recovery controls, but must prove them during restore and deletion tests | More control, plus disks, upgrades, replication, monitoring, and on-call work |
| A database or filesystem as the media store | Small metadata or low-volume attachments with simple access patterns | Useful only when its backup and deletion semantics match the policy | Can be simple at first, then becomes an application and capacity bottleneck for large media |

The catch is that direct upload does not eliminate application responsibility. Signed request expiry, content-length limits, malware scanning, metadata validation, tenant isolation, and download authorization still belong in the service design. A private bucket is a boundary, not a complete threat model.

## Where this design is the wrong choice

Do not choose ordinary private object storage as the final authority when the requirement is WORM immutability, legal hold, or an exact sub-day deletion deadline. Choose a storage mode with the required governance controls or add a specialized archive; document who can release a hold before writing a deletion worker.

It is also not suitable when the product needs a public media CDN, browser-managed cross-origin upload configuration, or automatic cross-region recovery that the selected storage layer does not provide. Stick with a platform that supplies those capabilities, or put a dedicated delivery and replication layer beside the retention system. Forcing every responsibility into one bucket creates a convenient diagram and an untestable promise.

I would reject the design if the team cannot run a restore test, a tenant-isolation test, and a deletion reconciliation test in a staging environment. I am not sure a retention number written in a policy document means much until those tests show where the bytes, keys, snapshots, and audit records actually remain. The useful question is not “which store is cheapest?” It is “which system can prove that the right copy was inaccessible or deleted at the right time?”

## Further reading

- https://aws.amazon.com/s3/pricing/
- https://docs.digitalocean.com/products/spaces/
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://www.rfc-editor.org/rfc/rfc7231
