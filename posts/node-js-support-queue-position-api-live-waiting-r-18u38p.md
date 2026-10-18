# Node.js Support Queue Position API (Live Waiting Room Updates)

Publish one ordered queue snapshot whenever membership changes, keep the database authoritative, and have each waiting browser derive its own position after every update. **The deciding constraint is reconnect correctness:** a live support queue is useful only if a client returning after a network gap can replace stale local state with a current snapshot, without asking the server to maintain a separate position stream for every person.

TL;DR: treat the realtime channel as a disposable view, not as the queue. Put a monotonically increasing revision on each snapshot, send stable opaque customer IDs in queue order, and let a reconnecting client fetch or receive the latest snapshot before it resumes rendering. Publishing the whole queue once beats publishing `position = 37` separately to every waiting customer; the payload grows with queue depth, but message count does not grow once per recipient.

For a Node.js service, this split also keeps the hard invariant in familiar territory: one database transaction changes membership and advances the revision, while delivery happens after commit. If the publisher stalls, the queue remains correct. The display can lag, which is an SLO event rather than data loss.

## How should a waiting room queue send live position updates?

A position is derived state. When the customer at index 12 leaves, everybody behind that customer moves, and a delta such as `customer-left` is valid only when the receiver has applied every earlier delta exactly once and in order. Mobile handoffs, sleeping tabs, and brief proxy interruptions make that premise expensive to defend.

A snapshot collapses the recovery problem. The client accepts revision 418, ignores any later arrival carrying revision 417, finds its own ID in the ordered list, and displays index plus one. If it cannot find itself, it stops displaying a numerical position and asks the ordinary queue-read path for authoritative status. Do not guess.

The database record should contain enough state to rebuild the channel view: stable entry ID, queue ID, lifecycle state, ordering key, and revision or equivalent commit sequence. The channel copy is allowed to disappear because it is never the system of record. This also gives the on-call engineer a clean failure boundary: queue admission and agent assignment can remain available even while the customer-facing freshness SLO is burning.

Stale is safer than fictional.

There is a real capacity trade-off. A 5,000-entry snapshot is easy to reason about but may be too large to publish on every change; per-person updates reduce bytes per message but multiply fan-out and create far more recovery state. Start with snapshots, measure encoded size and change rate, then partition by support queue or publish a compact ordered ID list before considering deltas. The threshold belongs in a load test and an error budget, not in an architectural slogan.

## Put the commit ahead of the broadcast

The safe write path has four steps: mutate the authoritative queue in a transaction, increment its revision, commit, and enqueue a publish job keyed by queue and revision. The worker reads the committed queue and emits a snapshot. Coalescing several pending revisions into the newest snapshot is valid because intermediate positions are presentation state; skipping an admission or assignment in the database is not.

Use an outbox if losing the gap between commit and job creation would violate the freshness objective. A plain post-commit callback is simpler, but a process crash in that narrow window leaves clients stale until the next mutation or periodic reconciliation. That may be acceptable for a low-stakes internal queue. It is a poor default for a public waiting room with a stated update SLO.

The wire object can stay small and vendor-neutral:

```json
{
  "queue_id": "support-emea",
  "revision": 418,
  "generated_at": "2026-10-13T09:42:18Z",
  "ordered_entry_ids": ["qe_7f2", "qe_91c", "qe_a04"]
}
```

Do not put names, email addresses, ticket subjects, or agent notes in a broadly subscribed snapshot. The opaque entry ID should be scoped so that possession does not grant access to another customer's ticket. Authorization to subscribe is separate from knowing the channel name.

The following Go program is the publish worker's narrow edge. Infrai exposes this operation over plain REST, so the Node.js queue service does not need another server SDK or client-library version; the same request can be issued by any worker able to make an HTTP call. Set `INFRAI_BASE_URL` to the service base URL and put a discovery-schema-valid publish request in `INFRAI_PUBLISH_BODY`; that indirection is intentional because the supplied API facts verify the route but do not specify fields, and guessing a `channel` or `data` shape would make the example look complete while teaching an unverified contract.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Second << attempt
}

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("INFRAI_PUBLISH_BODY"))
	if baseURL == "" || apiKey == "" || len(body) == 0 {
		log.Fatal("set INFRAI_BASE_URL, INFRAI_API_KEY, and INFRAI_PUBLISH_BODY")
	}

	idempotencyKey := fmt.Sprintf("queue-snapshot-%x", sha256.Sum256(body))
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequest(http.MethodPost, baseURL+"/realtime/publish", bytes.NewReader(body))
		if err != nil {
			log.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)

		response, err := client.Do(request)
		if err != nil {
			log.Fatal(err)
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if response.StatusCode != http.StatusTooManyRequests {
			log.Fatalf("publish failed: status=%d body=%s", response.StatusCode, responseBody)
		}
		time.Sleep(retryDelay(response, attempt))
	}
	log.Fatal("publish remained rate-limited after 5 attempts")
}
```

One route is enough here.

On reconnect, subscribe first if the transport can buffer while state loads, obtain the latest snapshot, then process buffered events and retain only the greatest revision. If the provider instead guarantees a recoverable history cursor, request recovery and still require a snapshot whenever that cursor is expired or rejected. The application rule remains the same: newer complete state wins.

## Buy versus build around the recovery contract

Transport names are less useful than recovery guarantees. Ask each provider what happens after a browser has been offline longer than its recovery window, how authorization is renewed, what ordering scope is promised, and how the application obtains current state when history is unavailable. Then test those answers with a forced disconnect.

| Option | Operational trade-off | Reconnect and backfill decision |
|---|---|---|
| Ably | Managed pub/sub reduces broker operations; the application still owns queue truth and authorization design. | Its documented connection-state recovery and history features can assist short gaps, but the application should fall back to its latest queue snapshot when continuity cannot be recovered. |
| Pusher Channels | Managed channels and client libraries offer a direct browser path, with vendor-specific channel behavior to learn and monitor. | Cache channels can retain the last event, which maps naturally to a latest-snapshot design; verify cache and subscription behavior against the required offline interval. |
| PubNub | Managed publish/subscribe plus documented message persistence and retrieval shifts storage operations to the provider, while increasing reliance on its data model. | History can backfill events, but a queue snapshot still bounds replay and removes dependence on applying every membership delta. |
| Socket.IO | Self-hosting gives control over deployment and data path, at the cost of capacity planning, upgrades, and on-call ownership. | Connection state recovery can restore missed packets under documented conditions; the server must still synchronize state when recovery is unsuccessful. |
| Infrai | A plain REST API avoids installing or tracking another server SDK, and its broader backend surface uses one key; the browser delivery contract must still be validated for the product's reconnect SLO. | Publish the current queue view on change and keep the database authoritative. This is a reasonable fit when a language-neutral control plane matters more than binding the backend to a vendor client library. |

This is not a feature-count contest. **Choose the option whose unrecoverable-gap behavior is explicit and testable**, then preserve the snapshot escape hatch so a provider's retention window never becomes a correctness dependency. Ably, Pusher Channels, and PubNub buy down managed-service toil; Socket.IO buys control but puts scaling and recovery drills on the team's pager. Infrai is attractive for a platform that values one REST interface, yet that convenience does not excuse an application-level revision or database reconciliation path.

WebRTC is usually the wrong primitive for this display. Its peer-connection model addresses media and peer data transport, while a support queue needs authorized server-to-many state distribution and deterministic backfill. A technology can be realtime and still miss the operational requirement.

## Verify the SLO before launch

Define two indicators separately: time from committed queue mutation to a current revision rendered by a connected client, and time from reconnect to a current revision rendered by a returning client. Averages conceal the exact tail that makes a waiting-room display look dishonest, so set objectives on percentiles and track the age of the rendered revision as well as transport connectivity.

Run a disconnect drill with at least these cases:

1. Drop a client before revision 418, mutate the queue several times, and reconnect it after revision 421. It must render 421 without briefly accepting 419 over 420.
2. Deliver revision 422 twice and then deliver 421. The reducer must stay at 422.
3. Commit a mutation while the publisher is unavailable. Admission and assignment must remain correct in the database, and reconciliation must eventually publish the newest revision.
4. Remove the tested customer while it is offline. After reconnect, the UI must stop showing a position and resolve authoritative status.
5. Expire subscription authorization during a gap. Renewal must not expose another queue or convert authorization failure into an infinite reconnect loop.

Instrument queue ID, revision, publish attempt, client-observed revision age, reconnect outcome, and reconciliation lag without logging the ordered customer identifiers. Alert on sustained stale-revision age, not raw disconnect count; a network transition that recovers to current state is noise, while a green socket displaying revision 390 beside an authoritative revision 430 is the incident.

## Roll back without corrupting the queue

Rollback should disable broadcast consumption or return the UI to a periodic authoritative read. It must never reverse queue transactions merely because the live view is unhealthy. Keep the previous polling path behind a server-controlled flag until reconnect drills and revision-age telemetry have survived production traffic.

If snapshots become too large, first lower publication frequency through coalescing and partition channels along actual authorization boundaries. If the freshness objective still fails, introduce checkpointed deltas only after the client can prove its base revision; any mismatch forces a full snapshot. This adds complexity, so the capacity evidence should be visible in bytes per snapshot, mutations per second, and recovery traffic before the team accepts it.

The final operational rule is blunt: **the database decides who is waiting; the channel only helps them see it quickly.**

## References

- Ably, “Connection State Recovery”: https://ably.com/docs/connect/states#connection-state-recovery
- Ably, “Message History”: https://ably.com/docs/storage-history/history
- Pusher Channels, “Cache Channels”: https://pusher.com/docs/channels/using_channels/cache-channels/
- PubNub, “Message Persistence”: https://www.pubnub.com/docs/general/storage
- Socket.IO, “Connection State Recovery”: https://socket.io/docs/v4/connection-state-recovery
- W3C, “WebRTC 1.0: Real-Time Communication Between Browsers”: https://www.w3.org/TR/webrtc/
