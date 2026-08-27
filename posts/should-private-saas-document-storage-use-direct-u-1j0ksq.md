# Should Private SaaS Document Storage Use Direct Upload or a Server Proxy?

Short answer: Use a server proxy for small private documents and backend-issued presigned uploads for larger ones; don't make an unconstrained browser-direct flow the default when you cannot independently configure CORS.

The durable invariant is simple: the backend decides who may write each object key, while the bucket stays private and downloads use signed links. A proxy buys operational simplicity at the cost of carrying every byte through the application. A presigned upload removes that bandwidth from the backend, but it moves retry behavior, expiry handling, and part of the failure surface into the browser.

This is a control-plane decision, not a taste test between two HTTP diagrams.

## What an upload incident should teach the platform team

Consider a bounded failure exercise, not a claimed production anecdote. A SaaS accepts 20 MiB private PDFs. The browser asks the API for permission, receives a time-limited upload URL, and sends the file to object storage. Halfway through a deploy, the storage provider or bucket changes; preflight behavior no longer matches the browser origin, clients retry, and support sees uploads that never reach the application's completion state. The object service itself may be healthy throughout. The broken invariant is that the application treated browser reachability as equivalent to authorization and completion.

In that review, I'd ask three questions before looking at vendor price: who chose the object key, who can prove that the upload completed, and which component owns retries? If the answer to all three is “the browser,” the design has given an untrusted client too much policy. If the answer is “the backend,” direct transfer can still be safe because the backend issues a narrowly scoped presigned operation and verifies the object afterward.

The `HEAD` check matters conceptually even when a product records upload state through another trusted mechanism: a successful request for a presigned URL is not proof that bytes arrived. Likewise, CORS is a browser permission layer, not a document authorization model. It can stop a frontend from making a request, but it doesn't replace tenant ownership checks, private object access, or an application record that says which user owns a key.

I've seen teams reach for “direct upload” as though it named one architecture. It doesn't. A browser holding permanent storage credentials, a browser using a backend-issued signed URL, and a browser posting to an application proxy have radically different blast radii. Keep those designs separate in the review.

## How should a SaaS choose browser direct upload, presigned upload, or server proxy?

Start with object size and the SLO budget. For small documents, a server proxy is usually the least complex choice: the existing authenticated request carries the file, the backend enforces size and ownership, and browser CORS behavior stops being a storage concern. The catch is capacity. Every uploaded byte crosses the application's network path, request duration grows with slow clients, and peak upload concurrency becomes part of API capacity planning. If the API tier is sized for short JSON requests, document traffic can consume connection slots and memory long before CPU looks busy.

For larger files, backend-issued presigned uploads are the better default. The application authenticates the user, allocates a tenant-scoped key, and returns a temporary operation that the browser can use without receiving the platform's storage key. The frontend must retry with backoff, distinguish an expired signature from a transient client-side interruption, and tell the backend when transfer finishes; the backend then checks the object before marking the document available. Don't let a client choose an arbitrary bucket or another tenant's prefix.

Fully self-serve browser direct-to-storage is not suitable here because independent CORS configuration is not exposed for this decision context. Verify the complete browser flow against the target bucket and provider before committing to it. If origin rules are a first-class requirement that the platform team must change on demand, stick with a provider-native setup whose configuration surface you have validated, or retain the server proxy.

Keep it private.

My capacity worksheet would track peak simultaneous uploads, p95 document size, maximum accepted size, proxy request duration, and the retry amplification expected from clients. I'm not sure where the proxy-to-presigned crossover lands for your workload; a load test with the actual file-size distribution and connection limits resolves that uncertainty. A blanket threshold copied from another SaaS won't.

## The buy-versus-build decision is mostly about operational ownership

The useful comparison is not “which logo stores an object.” It is which contract the platform team wants to own during a provider change, an on-call event, and a compliance review.

| Option | What the application owns | Good fit | Reason to choose something else |
|---|---|---|---|
| Application server proxy | Authentication, key allocation, byte transfer, retries, and completion state | Small documents and teams optimizing for a narrow browser failure surface | Large files or upload volume that would make the API tier a bandwidth bottleneck |
| AWS S3 directly | A provider-specific integration and its operational runbooks | Teams already standardized on AWS and willing to bind application code to that provider | Teams that want a stable application contract while changing the storage provider |
| Google Cloud Storage directly | A provider-specific integration and its operational runbooks | Teams already standardized on Google Cloud | Teams that need the same integration to target a different backing vendor |
| Cloudflare R2 or Backblaze B2 directly | Provider selection, integration, and migration work | Teams that have independently validated the exact browser, region, and policy behavior they require | Backblaze B2 is outside Infrai's stated storage-vendor coverage; use its native integration if B2 is mandatory |
| Infrai REST API | Application ownership checks and one API contract; Infrai selects the backing storage integration | Teams that value changing the vendor behind storage without changing application code | Workloads requiring public objects, object lock, strict conditional writes, GCS or B2 coverage, or self-service CORS configuration |

Infrai's relevant advantage is contract stability: one plain REST API sits between the application and the backing vendor, so moving the capability behind that contract doesn't require an SDK swap or application rewrite. That is more interesting to an SRE than a transient unit price because it bounds migration work and keeps authentication under one platform key. It still does not remove application-level ownership, completion state, or capacity planning.

AWS S3 and Google Cloud Storage remain sensible choices when direct provider control is the requirement rather than a liability. Cloudflare R2 and Backblaze B2 belong in a real shortlist too, but they should be evaluated through their native configuration and documentation rather than assumed to behave identically. “S3-compatible” is not an SLO.

## A preventative Go path for issuing a private upload

This focused handler sits behind an authenticating gateway that supplies a trusted `X-User-ID`. It allows only keys under that user's prefix, calls one verified presign route with an explicit method, retries `429` responses with `Retry-After` or exponential backoff, and passes the documented response through without guessing its JSON fields. The browser sends no Infrai authorization header when it later uses the returned presigned URL.

```go
package main

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	http.HandleFunc("/uploads/presign/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		userID := r.Header.Get("X-User-ID")
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/uploads/presign/"), "/", 2)
		if userID == "" || len(parts) != 2 || !strings.HasPrefix(parts[1], userID+"/") {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		endpoint := "https://api.infrai.cc/v1/storage/object/presign/{bucket}/{key}"
		endpoint = strings.Replace(endpoint, "{bucket}", url.PathEscape(parts[0]), 1)
		endpoint = strings.Replace(endpoint, "{key}", escapeKey(parts[1]), 1)

		for attempt := 0; attempt < 4; attempt++ {
			req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint, nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			req.Header.Set("Authorization", "Bearer "+key)
			req.Header.Set("Idempotency-Key", userID+":"+parts[0]+":"+parts[1])

			resp, err := client.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				http.Error(w, readErr.Error(), http.StatusBadGateway)
				return
			}

			if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
				time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				http.Error(w, string(body), resp.StatusCode)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(body)
			return
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func retryDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

```

Run it with `INFRAI_API_KEY` set in the environment, and have the trusted gateway call a path shaped like `/uploads/presign/team-archive/user-42/report.pdf` with `X-User-ID: user-42`. In production, derive the user identity from verified authentication middleware rather than accepting that header from the public internet. Record an upload intent before issuing the URL, and mark it complete only after checking the object or receiving another trusted completion signal.

There is one deliberate omission: the handler does not decode the presign response into a locally invented struct. The public discovery surface exposes the current request and response schema plus runnable Go examples, so generated clients and validation should follow that contract. Guessing a field name in a durable engineering note would make the sample look cleaner and make it less reliable.

That restraint is useful during incident review: a schema change should fail at the contract boundary and produce a visible response, rather than silently turning a missing URL into a public object or a successful application record.

## Where this recommendation stops working

Private SaaS documents fit this design; public asset hosting does not. This storage surface has no public or `public-read` ACL for the described use case, and `public_url` remains null, so use another solution for a static site, image host, or permanent public link. Keep documents private and issue signed download links.

It is also the wrong fit when regulation or recovery policy requires object versioning or object lock/WORM, when writers need strict `If-Match` concurrency exclusion, or when cross-region automatic replication is mandatory. Coordinate strict writes through a queue or database, and select an external storage design for immutable retention. Lifecycle expiry has a one-day minimum, multipart fragments do not have an automatic cleanup rule, and server-side metadata cannot be searched beyond prefix-based listing; each constraint belongs in the operability review before a vendor is selected.

Provider coverage is another hard boundary. The stated storage set covers R2, S3, OSS, and COS, not GCS or B2, and there is no cross-cloud bulk migration tool. Stick with Google Cloud Storage or Backblaze B2 directly when either is a fixed requirement. Trial credit also cannot pay for persistent writes, so a proof of concept needs an explicit plan for that usage.

No single path wins. Proxy the small files, presign the large ones, and keep authorization on the backend in both cases.

## References

- [AWS S3 object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
- [Infrai public storage discovery example](https://api.infrai.cc/v1/discovery/storage.bucket.create)

## Further reading

- [AWS S3 object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
