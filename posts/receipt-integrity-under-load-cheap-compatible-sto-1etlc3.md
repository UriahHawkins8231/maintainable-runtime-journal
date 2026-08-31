# Receipt Integrity Under Load: Cheap Compatible Storage Rules for 30/90-Day App Retention

Large-file throughput changes the storage decision before price does. Short answer: for a developer tool that preserves original receipts for audit, test private object storage against explicit 30-day and 90-day retention classes, then approve a US or EU location only after a real restore measures the largest file against the recovery target. A daily schedule and a lifecycle rule are useful controls, but neither is proof that an archive can be recovered.

This is a runbook problem disguised as a storage comparison. The system receives receipts, preserves the original bytes, and later has to produce a defensible copy for an auditor. A transformed thumbnail or parsed JSON record is not the original. The intake path therefore needs a durable object key, a checksum, the source receipt ID, its region, and the retention class in a small catalog that can be queried without scanning the bucket.

Keep the write path boring.

## What must a daily app backup restore prove before rollout?

Verification starts with a private restore. Use an authorized server-side read or a narrowly scoped presigned URL whose lifetime matches the operator's procedure; a presigned URL grants temporary access, so its creation and use belong in the restore audit trail. Confirm the response length and checksum before handing the file to the auditor. The public documentation for presigned URLs is a useful reference for that boundary.

Then exercise the failure paths: stop a worker after multipart creation, retry the same receipt, deny a read, and simulate a lifecycle policy change in a staging bucket. The expected outcome is a catalog entry that distinguishes pending, complete, and rejected work, with no silent replacement of the original. I’m not sure any generic benchmark can predict your recovery SLO; only a representative restore under the expected concurrency can resolve that.

Rollback is a retention operation. Keep the previous read path and its copies until the replacement path has passed a full backup-and-restore cycle and the old retention window has closed. If the new path misses its throughput target, pause new routing, preserve the catalog, restore from the previous path, and record the decision. Do not delete the old copies as part of a failed cutover.

That is the gate.

## How should daily app backups handle lifecycle rules across US and EU?

Start with two policies, not one clever expression. A routine copy can expire after 30 days; an audit copy can expire after 90 days. Put the class in the backup or receipt job's configuration, and make the bucket lifecycle policy part of the reviewed deployment. The catalog remains the source of truth for what was written, while the storage policy provides the durable cleanup mechanism.

The one-day boundary matters. If the requirement is “delete this object six hours after creation,” a daily lifecycle rule is the wrong control; use an application-level queue or a scheduled deletion process with an owner and an audit trail. If the requirement is 30 or 90 days, the policy is a better authority than a cleanup worker that can disappear during a deployment.

For US and EU workloads, a region label is only an input to the review. Confirm the actual placement, transfer path, retention interpretation, and restore route for each workload. Keep the catalog and the object key together in the same change record. A backup that exists in the wrong jurisdiction still fails the requirement, even when its checksum is perfect.

The retention class should be immutable after the intake decision unless an explicit policy change records who approved it. This protects against a retry that creates a second object with a shorter expiry, and it gives an operator something concrete to inspect during an incident. It also makes the capacity forecast honest: daily object count, average and maximum receipt size, retained byte-days, request volume, and expected restore reads belong in the model.

## Why does the largest receipt set the recovery SLO?

The most dangerous green dashboard is one that reports successful uploads while never exercising reads. For this workload, measure the largest original receipt and a representative batch, not just a small test file. Large-file throughput includes multipart setup, concurrent part transfers, finalization, checksum verification, and the time to stream the completed object to the consumer.

Measure it.

An incomplete multipart upload is a separate piece of state. Record its upload ID and creation time, alert on old incomplete uploads, and define who can clean them up. Lifecycle cleanup for completed objects does not automatically make an abandoned upload part of your recovery story. No mystery bill.

Here is the failure I would expect to find in a first rehearsal. A 12 GB receipt bundle uploads successfully, the worker writes “complete” to its database, and the restore job later reads a shorter object because the final checksum was never compared with the catalog. The dashboard is green: writes succeeded, the object exists, and the lifecycle policy is present. The audit request still fails. The repair is not a more ambitious retention rule. It is a transaction boundary that records completion only after length and checksum verification, plus a restore alarm that samples the path before an auditor finds it.

The write worker should be restartable. It should derive a stable object key from the receipt identity, avoid overwriting a verified original, and record completion only after the object length and checksum match the catalog entry. Retries need bounded backoff and an idempotency decision; a retry that creates an indistinguishable second copy is an accounting problem, not merely a network problem.

Here is a small policy model that keeps retention and placement visible in code. It does not pretend to be a provider SDK, so the storage adapter can be tested against any S3-compatible implementation and the policy review does not depend on one client library.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

type Receipt struct {
	ID       string
	Region   string
	Retention time.Duration
	Size     int64
	Checksum string
}

func archiveKey(receiptID string) string {
	return "receipts/" + receiptID + "/original"
}

func checksum(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func validateReceipt(r Receipt) error {
	if r.ID == "" || (r.Region != "US" && r.Region != "EU") {
		return fmt.Errorf("receipt identity or placement is not approved")
	}
	if r.Retention != 30*24*time.Hour && r.Retention != 90*24*time.Hour {
		return fmt.Errorf("retention must be 30 or 90 days")
	}
	if r.Size <= 0 || r.Checksum == "" {
		return fmt.Errorf("original object metadata is incomplete")
	}
	return nil
}

func main() {
	receipt := Receipt{ID: "rcpt_1042", Region: "EU", Retention: 90 * 24 * time.Hour}
	fmt.Println(archiveKey(receipt.ID), validateReceipt(receipt))
}
```

The capacity-planning reflex is deliberate: set an SLO for successful archival and a separate recovery-time objective for retrieving the maximum object. Track upload duration, part retry count, checksum mismatches, incomplete uploads, lifecycle deletions, and restore duration by region. An SLO without those labels tells the on-call engineer that something is wrong but not whether the problem is storage, network, or the receipt worker.

The rehearsal should be uncomfortably specific. Take the p95 and maximum receipt sizes from the intake stream, build a batch that preserves the expected US/EU split, and run it through the same concurrency limit as production; then interrupt a transfer near finalization, replay the job with the same receipt IDs, and restore the largest completed object while another batch is still uploading. Record the wall-clock time, bytes read, checksum result, queue delay, and operator actions. A provider that looks fine for 10 MB originals can behave very differently when a daily export is several gigabytes and the restore competes with normal traffic. Repeat the run at the 30-day and 90-day retention boundaries, because an expiry test that only inspects a fresh object has not tested the policy that matters.

## Which storage decision protects the audit path under load?

Build the comparison around a measured workload rather than a per-gigabyte headline. For seven days, replay the largest expected receipt sizes and the daily arrival pattern into isolated buckets. Then perform a restore from the 30-day class and the 90-day class, verify bytes against the recorded checksum, and capture the operator time. Your mileage may vary: compression ratio, concurrency, and the distribution of file sizes will move the result more than a toy upload.

| Evaluation area | Evidence to collect | Reject the option when |
|---|---|---|
| Large-file path | Multipart behavior, maximum object size, part retry behavior, end-to-end upload time | The largest original cannot meet the archival SLO with headroom |
| Retention | 30-day and 90-day policy tests, deletion records, catalog reconciliation | Expiry cannot be explained to an auditor or operator |
| Placement | US/EU location, transfer paths, access controls, and contract terms | The intended workload cannot satisfy its location requirement |
| Recovery | Private read, checksum verification, restore time, and a written runbook | A successful upload is the only evidence of recoverability |
| Operations | Metrics, alerts, incomplete-upload cleanup, and rollback ownership | The platform team inherits an alert with no actionable owner |

The cheap option is not automatically the operationally cheap option. Count retained bytes, requests, multipart residue, transfer, planned restores, and the people-hours needed to investigate a missing original. Pricing pages are useful inputs, but they are not a restore test; the referenced pricing page is one example of the kind of current source that must be checked during the estimate rather than copied into a permanent promise.

Keep the interface narrow: put object creation, multipart operations, private reads, and lifecycle-policy application behind an adapter, then test the adapter with a disposable bucket. That leaves the application responsible for the receipt contract and the storage system responsible for bytes and retention execution. It also makes a provider change a controlled migration instead of a rewrite scattered across every backup worker.

The boundary is clear: this design is unsuitable when the requirement is immutable legal hold, automatic cross-region replication, public static delivery, or a recovery objective tighter than the tested transfer path. Choose an arrangement with those native controls when they are part of the SLO. Stick with the simpler lifecycle design when the requirement is private daily preservation with explicit 30/90-day expiry and a tested restore.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://www.backblaze.com/cloud-storage/pricing
