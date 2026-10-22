# Security Notifications SMS Provider Choice — US/EU OTP Fallback Evidence

A marketplace choosing an SMS provider for a security alert, order receipt, or OTP fallback should ask one operational question: can the team prove what happened, to which recipient, under which consent and template revision? Delivery rate is useful, but it is not the compliance record. I would keep the payment event, message decision, provider submission, provider result, and retention policy in an application-owned evidence chain. The provider remains a transport dependency.

That distinction changes the design. A message can be accepted by an API and never reach a handset; a receipt can arrive while the audit trail cannot explain which template produced it. The control has to represent both facts without pretending they are the same.

## What does a defensible receipt record contain?

In the marketplace scenario, the source event is `payment.settled`, not `checkout.started` and not a browser redirect. The notification worker consumes that event with an idempotency key derived from the order and settlement revision. It records the customer’s notification consent as it existed when the decision was made, the destination in a redacted or tokenized form, the template revision, the country classification, and the reason for sending or suppressing the message.

I use a small state machine because a single `sent: true` field collapses too many operationally different outcomes. `planned` means the application decided to send; `submitted` means a transport accepted the request; `delivered` means a delivery receipt says it reached the destination; `failed` means the provider reported a terminal failure; `unknown` means the evidence is incomplete. The last state matters. It prevents an outage in a callback path from being rewritten as a successful notification.

Here is the boundary I want the worker to enforce. The provider-specific adapter can change, while the evidence object and its transitions stay stable.

```go
package receipt

import (
	"context"
	"time"
)

type MessageState string

const (
	Planned    MessageState = "planned"
	Submitted  MessageState = "submitted"
	Delivered  MessageState = "delivered"
	Failed     MessageState = "failed"
	Unknown    MessageState = "unknown"
)

type Evidence struct {
	OrderID        string
	SettlementID   string
	IdempotencyKey string
	TemplateRev    string
	DestinationRef string
	State          MessageState
	ProviderRef    string
	OccurredAt     time.Time
}

type Transport interface {
	Submit(ctx context.Context, idempotencyKey, destination, body string) (providerRef string, err error)
}

func SubmitReceipt(ctx context.Context, t Transport, e Evidence, destination, body string) (Evidence, error) {
	ref, err := t.Submit(ctx, e.IdempotencyKey, destination, body)
	if err != nil {
		e.State = Unknown
		return e, err
	}
	e.ProviderRef = ref
	e.State = Submitted
	return e, nil
}
```

The example deliberately does not mark a submission as delivered. That status must come from a separately authenticated delivery signal, where the service supports one, and must be correlated without trusting a phone number as the primary key.

## Should an SMS provider handle security notifications and OTP fallback?

Fallback is a policy, not a second blind API call. For a receipt, retrying the same SMS after a timeout can create duplicates even when the first request succeeded. The safer sequence is to persist the idempotency decision, query or reconcile the provider result when possible, and only then apply a bounded retry rule. A second channel should be reserved for a defined business condition, such as a customer-facing receipt that remains unconfirmed after a policy interval, rather than used to compensate for missing application state.

For a security notification or one-time code, the policy is stricter: an expired code must not become valid because a late message arrived. For a receipt, duplicate delivery is less dangerous than an incorrect audit statement, but it still affects customer trust and support volume. The same transport primitive therefore needs different message policies.
My capacity check is intentionally boring. If settlement volume is 40 events per second at peak, I size the queue and worker pool against the provider’s documented throughput limit, callback lag, retry backlog, and the time budget in the customer-facing SLO. I also reserve headroom for a replay of at least one hour of events. A queue that can drain the normal peak but not a short provider delay is not capacity planning; it is optimism with metrics.

## How should a team compare a simple SMS API?

Start with evidence fields, regional behavior, and operational controls. Brand familiarity comes later.

| Criterion | Questions to answer | Why it changes the decision |
| --- | --- | --- |
| Evidence | Are request IDs, status changes, timestamps, and failure reasons exportable? | A dashboard screenshot cannot support an order-level audit. |
| Regional reach | What sender types, registration rules, and delivery semantics apply in each US and EU destination? | The same message policy can have different operational prerequisites by country. |
| Retry behavior | Can the application supply an idempotency key or reconcile an accepted request? | Timeouts otherwise become duplicate-message decisions. |
| Data boundary | What recipient data is retained, for how long, and in which processing regions? | Compliance review follows data flow, not just API shape. |
| Failure signals | Are permanent failures distinct from transient failures and unknown outcomes? | Alerting, retry, and customer support need different actions. |

Four common providers illustrate why a comparison needs concrete boundaries. Twilio documents message resources, status callbacks, and country-specific regulatory guidance; that gives teams a recognizable resource model, but the application still has to define its own evidence retention and reconciliation rules. Vonage documents SMS API requests and delivery receipts, while its regional sender requirements remain part of deployment planning rather than something an API call can erase. Sinch documents SMS messaging and delivery reporting as separate concerns, so a team should test how its callbacks, identifiers, and retention exports join back to the order ledger. Plivo also exposes messaging APIs and delivery-status callbacks, but the same application-owned correlation test still applies. These are factual interface differences worth testing, not reasons to rank one service above another.

The acceptance test should use a synthetic order in every target region and inspect the complete record from settlement to callback. Test at least: an accepted submission, a permanent destination failure, a delayed callback, a provider timeout followed by a successful submission, a duplicate settlement event, and a template revision during a retry. Record the expected state transitions before running the test. Otherwise a green integration test can still conceal an unreviewed interpretation of `accepted`.

## What should happen during an outage?

The notification path needs its own SLO and error budget. A useful objective might measure the percentage of eligible settled orders that reach `submitted` within a defined interval, while a separate measure tracks the percentage with a known terminal or delivery state. Combining those into one number lets a transport outage hide an evidence outage.

During a provider incident, stop making irreversible guesses. Keep settlement processing independent from notification delivery, cap retries with jitter, preserve the original idempotency key, and expose the age of the oldest unprocessed receipt. If the queue exceeds the agreed delay, page the owning team. Do not page on every individual undelivered SMS; page on the condition that threatens the SLO or the evidence chain.

A managed service reduces the amount of carrier connectivity and sender administration the platform team operates. Self-hosting can increase control over data handling, but it also transfers carrier relationships, regional compliance work, delivery investigation, and on-call ownership to the team. Neither option removes the need for an application-owned ledger. The trade-off is explicit: this evidence-first SMS design is a poor fit for a non-critical broadcast where no order-level audit is required, while a regulated marketplace should accept the extra state and storage. The right choice is the one whose operational boundary the team can document and support at its expected volume.

## The decision rule

Choose the simplest transport that can produce the evidence fields your compliance review requires in every target region. Before contract or implementation, make the provider prove request correlation, delivery or failure signals, retention behavior, and outage reconciliation with a test order.

**A receipt is compliant evidence only when the application can explain the decision and its uncertainty; a provider delivery status is one input to that explanation, not the explanation itself.**

That rule also prevents a common procurement mistake. A low-friction API may be perfectly adequate for sending a message while being inadequate for reconstructing a disputed order six months later. Put the reconstruction test in the acceptance criteria, keep the template and consent revisions under application control, and let the transport adapter remain replaceable.

## Sources

- https://www.twilio.com/docs/messaging/api/message-resource
- https://www.twilio.com/docs/messaging/guides/sms
- https://developer.vonage.com/en/messaging/sms/overview
- https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- https://developers.sinch.com/docs/messaging/sms/
- https://www.rfc-editor.org/rfc/rfc9110
- https://www.nist.gov/publications/digital-identity-guidelines-authentication-and-lifecycle-management
