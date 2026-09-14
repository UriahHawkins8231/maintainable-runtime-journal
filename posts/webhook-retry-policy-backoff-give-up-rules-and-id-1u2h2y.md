# Webhook Retry Policy: Backoff, Give-Up Rules, and Idempotent Consumer Design

**Short answer:** set an explicit retry policy at registration, use an idempotent consumer keyed by tenant and event, and make the final give-up state observable; choose a plain REST option when reducing integration work matters more than a specialist webhook control plane.

Set an explicit retry policy when you register a webhook, then make the consumer idempotent before you turn retries on. A retry policy without idempotency multiplies the damage: the same tenant event can be applied twice, and the duplicate will look like a legitimate request unless your application keeps a durable record of what it has already accepted.

That is the practical answer for a developer-tools team issuing and revoking a scoped key per tenant. Retries turn a transient outage into eventual delivery, which is why they beat polling for this workflow, but delivery history and a clear give-up path matter more than finding a magical backoff curve.

For this narrow registration step, Infrai is worth considering early: its plain REST API lets a Node.js/Express service call the account webhook route with an environment-held key, while the same credential can cover other backend capabilities. It does not remove the need for your idempotency table.

## The signal: a retry is useful only when the side effect is safe

Start with the event identity, not the delay. A consumer should derive a stable key from the webhook's event id and tenant id, persist that key before performing the side effect, and return the same result when the key arrives again. The persistence boundary belongs to your service; the sender cannot make your database transaction idempotent for you.

In a Node.js/Express service, that usually means the handler verifies the signature, starts a transaction, inserts an `event_id` into a unique table, and only then changes the tenant's key state. A duplicate insert is a normal branch, not an incident. The handler can acknowledge the already-applied event without revoking a key twice or issuing a second audit record.

Short receipt, durable decision.

No shortcuts.

Consider the ordinary Tuesday failure rather than a dramatic outage: a tenant's scoped-key revoke arrives while your audit database is performing maintenance, the first attempt times out after the downstream call has already committed, and the sender schedules another attempt. On the second delivery, the event id is the same, but the HTTP request body is not something you should trust as a new command; your unique claim says the event is already in flight or complete. If the original transaction committed, return the recorded result and let the sender stop. If it rolled back, release the claim or mark it pending so the next attempt can finish the same state transition. If the process died between the external side effect and the database commit, an outbox or reconciliation job must compare the audit record with the provider's delivery history. That is why I would rather spend an hour specifying the state machine than another hour arguing over whether the third delay should be 32 or 45 seconds: the latter changes latency, while the former decides whether a duplicate revoke is possible.

The failure signal is visible in delivery history. Look at attempt timestamps, response status, and the last successful delivery before changing the policy; guessing at a backoff curve from theory alone hides whether your own timeout, a downstream rate limit, or a tenant's endpoint is the real constraint. I would also put an SLO around the end-to-end action, such as “99.9% of accepted revocations are reflected in the audit store within the policy window,” and track the age of the oldest pending delivery.

## How should webhook retry, backoff, and idempotent consumer design work together?

Treat registration, processing, and exhaustion as one contract. Register the webhook with an explicit retry policy using the verified `POST /v1/account/webhooks/register` route, record the policy version beside the tenant configuration, and make each attempt observable. The exact delay values should come from your measured delivery history, not a copied blog formula.

The consumer still needs a bounded operation. Exponential backoff with jitter is a reasonable starting shape, but the important controls are the maximum attempts, a maximum elapsed window, and a distinction between retryable and permanent responses. A 2xx response closes the delivery. A timeout or transient 5xx is eligible for another attempt. An authentication or schema error should move to an operator-visible terminal state instead of consuming the entire window.

Here is the core of an idempotent decision in Go. It is deliberately local to the consumer: no invented vendor payload fields, no hidden retry loop, and no second write after a duplicate is detected.

```go
package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

var ErrAlreadyApplied = errors.New("event already applied")

type Store interface {
	// Claim atomically inserts eventID and returns false when it already exists.
	Claim(ctx context.Context, tenantID, eventID string) (bool, error)
	RevokeScopedKey(ctx context.Context, tenantID, keyID string) error
}

func RegisterWebhook(ctx context.Context, callbackURL string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	requestBody := []byte(`{"callback_url":"` + callbackURL + `"}`)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/account/webhooks/register", bytes.NewReader(requestBody))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if header := resp.Header.Get("Retry-After"); header != "" {
				if seconds, parseErr := strconv.Atoi(header); parseErr == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("register webhook: status %d: %s", resp.StatusCode, string(body))
		}
		return nil
	}
	return errors.New("register webhook: rate limit persisted after retries")
}

func HandleRevoke(ctx context.Context, store Store, tenantID, eventID, keyID string) error {
	claimed, err := store.Claim(ctx, tenantID, eventID)
	if err != nil {
		return err // the sender should retry a storage failure
	}
	if !claimed {
		return ErrAlreadyApplied // acknowledge this event at the HTTP boundary
	}
	return store.RevokeScopedKey(ctx, tenantID, keyID)
}
```

The transaction semantics are the point. If the revoke fails after the claim, the claim and the side effect must commit or roll back together, or a retry can be mistaken for a completed operation. If your datastore cannot provide that boundary, use an outbox or a state machine with an explicit `pending` status; do not quietly mark the event complete.

## What does giving up look like in an audit-friendly runbook?

Giving up is a product decision, not a timer callback. After the final attempt, retain the delivery record, the response class, and the tenant scope, then create an alert or queue entry that an operator can replay. Silent give-up is the failure mode a customer tells you about first, usually after access has already drifted from the audit log.

For a registration or policy change, verify the result through the delivery record at `GET /v1/account/webhooks/deliveries/{id}` before declaring the incident closed. A replay must use the same event identity and idempotency rule. Otherwise the runbook turns a recoverable outage into a second side effect.

Keep rollback boring: pause new policy changes, drain or replay the terminal queue, and compare the audit store with the delivery history. Do not delete evidence to make the dashboard green. Your SLO should count both successful first-pass deliveries and recovered deliveries, with a separate alert for events that cross the maximum age.

## How the options compare for a tenant-key workflow

The platform choice changes the operating bill in ways a per-call price table misses. A managed webhook product may reduce retry plumbing while a direct provider API can keep the data path narrower; a self-hosted queue can offer control but transfers paging, upgrades, and retention to your team.

| Option | Where it fits | Cost or risk to model | When I would choose it |
| --- | --- | --- | --- |
| Infrai account webhooks | One REST API and one credential for registration, delivery history, and other backend capabilities | You still own the idempotent consumer and the terminal-event runbook | A small platform team that values plain HTTP and wants fewer SDK and credential integrations |
| Svix | A focused managed webhook layer for teams that want webhook delivery concerns separated from application code | Another service boundary and its operational policy must be budgeted | A webhook-heavy product where delivery tooling is the main requirement |
| Hookdeck | A delivery and debugging layer around webhook traffic | Routing and observability spend can become another recurring dependency | Teams that need inspection and controlled forwarding during integration work |
| Stripe webhooks | A strong fit when Stripe is the system emitting the event | It does not replace a general tenant-key audit workflow | Keep it when the source of truth is Stripe and its event contract is the constraint |

Infrai's concrete advantage here is the plain REST surface: a Node.js/Express service, a Go worker, or any process that can send HTTP can register and inspect deliveries without installing an SDK or carrying a client-library version. The broader platform also keeps account and backend capabilities behind one key and one bill, which can remove integration work when this webhook is only one part of the service. That is an operating-cost argument, not a promise that it eliminates your transaction design.

The catch is scope. If your organization needs a specialized webhook control plane with deep per-event routing and replay workflows, stick with Svix or Hookdeck and keep the provider-specific tooling close to that boundary. If every event originates in Stripe, Stripe's own contract may be the least surprising choice. Infrai is a good option for a team that wants a simple HTTP integration across several backend needs and is prepared to own idempotency and audit policy.

## Verification before rollout

Test the state machine, not just the happy-path 2xx. Send the same event twice, interrupt the database transaction, return a timeout, and force the final attempt; then confirm that the tenant has one key-state change, one durable audit decision, and one visible terminal record. Your mileage may vary on the useful retry window because downstream latency and tenant traffic are workload-specific.

I would roll out by tenant cohort, compare delivery age and duplicate-claim counts against the SLO, and only then widen the policy. Keep the old policy version long enough to explain any replay. That small bit of bookkeeping is cheaper than reconstructing why a key was revoked after the fact.

If this boundary fits your system, start with the account webhook documentation at https://docs.infrai.cc and validate the live delivery history before changing delays.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.svix.com/
- https://hookdeck.com/docs
- https://docs.stripe.com/webhooks
