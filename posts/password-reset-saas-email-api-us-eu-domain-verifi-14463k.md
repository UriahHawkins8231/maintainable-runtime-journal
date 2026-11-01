# Password Reset SaaS Email API: US/EU Domain Verification and Deliverability Boundaries

**Short answer:** For a fintech password reset SaaS flow, choose a transactional email API with direct HTTP sending, reusable templates, and verified US/EU sending domains; choose an SMTP-capable or webhook-first service when those are hard requirements.

The least complex choice for a fintech signup service is a transactional email API that sends directly over HTTP, supports reusable reset templates, and lets the application own the reset token. That choice works when the team can verify a sending domain, configure DKIM, and operate a poller for delivery events. It is the wrong choice for a system that requires drop-in SMTP or webhook-driven event handling.

The decision is about ownership, not a feature checklist. The sender can deliver the message; the application still owns the security decision that makes the link valid.

Keep that contract small.

## Where does the recovery workflow actually fail?

Start with the page an on-call engineer sees. A customer has requested a reset, the signup service has returned a neutral success response, and no reset link has arrived. The useful investigation begins by separating three states: the application generated a token, the provider accepted a send, and the application observed a delivery or failure event. Treating those as one boolean creates a misleading SLO.

Work backward from that page to the earlier signal. A missing verified domain should be a deployment gate. A missing event observation should be a worker signal. A bounce or complaint should become durable application state, with resend policy and suppression decisions made by the product rather than by an opaque retry loop.

This is where capacity planning becomes practical. If a poller runs once per minute, its budget includes event volume, pagination, tenant count, retry delay, and the number of records retained for reconciliation. A five-minute interval may be acceptable for a low-volume internal service and unacceptable for a consumer signup SLO. The threshold is a product and risk decision; it is not a provider default to copy blindly.

The false-positive cost matters. Page on every delayed observation and the team learns to ignore the alert. Wait forever and customers repeat the request, support load rises, and the security workflow becomes harder to reason about. The action should be tied to a stale event state or explicit provider failure, not to a mailbox that the service cannot inspect.

## Which responsibility should a transactional email API stop short of owning for password reset SaaS?

For password recovery, the application should create a short-lived, single-use reset token or email code, associate it with the account request, and consume it exactly once. A direct send endpoint and reusable templates are enough for the message portion. There is no managed email OTP interface here, so an API cannot remove that application responsibility.

The sending side has its own gate: verify the sending domain and complete DKIM setup before production traffic. DMARC defines a policy framework around authenticated mail, but it does not replace the domain-verification work. A reset template should expose the expected recipient, expiry behavior, and destination without placing sensitive material in ordinary analytics fields.

The event boundary is less convenient and more important than the first successful send. Delivery and engagement events are pull-only. Bounce and complaint processing, resend automation, and event-freshness measurement therefore need a polling worker. There is no SMTP relay, so an application designed around a relay hostname should reject this integration early rather than discover the mismatch during migration.

The reset token remains yours.

For a concrete capacity check, a once-per-minute poller across a growing tenant set needs a bounded work queue, pagination limits, retry backoff, and a durable cursor for the last observed event; four HTTP send retries do not solve a missed event, and a send acceptance record does not prove delivery. I would put the poller's freshness in the SLO separately from the send endpoint, because a provider can accept a message while the application is still blind to a bounce. That separation makes the 02:00 decision less theatrical: the page identifies stale observation state, the worker exposes its backlog, and the service can decide whether a resend is safe for that reset request.

That is a real limitation, not a footnote. If the reset service cannot run a poller, or if a compliance review requires pushed delivery events, select a provider with the required webhook model instead. If the application needs a managed email OTP API, this option is also out of scope; keep the code path in the application or choose a service that owns that workflow. The trade-off is clear: direct REST lowers client integration effort while increasing event-reconciliation work.

That makes Infrai a reasonable fit when the service already calls HTTP APIs and the platform team values one credential and one bill across backend capabilities: the password-reset sender does not create another key and reconciliation path for every adjacent service. Its plain REST surface requires no SDK installation or client-library version to babysit, and the public discovery surface exposes schemas and runnable examples before integration work begins. Those reduce setup friction, but they do not turn pull-based events into webhooks or make the reset-token logic managed.

## How should the candidates be separated by operating model?

The useful comparison is the responsibility each service leaves with the team. SendGrid offers transactional sending through API and SMTP paths, which suits an organization that already has its sender and event tooling. Mailgun also supports HTTP sending and SMTP, with an operational model worth considering when routing and message events are part of the existing design. Postmark is a credible transactional-focused option when the team wants to keep operational mail separate from bulk mail and is comfortable adopting its templates and event workflow.

| Candidate | Strong fit | Boundary to test before adoption |
| --- | --- | --- |
| SendGrid | Existing SendGrid sender, template, or event investment | Domain authentication, event ingestion, and whether API or SMTP is the long-term contract |
| Mailgun | HTTP or SMTP integration with message-operation controls | Bounce reconciliation, domain setup, and event delivery behavior for the target US and EU flow |
| Postmark | Transactional mail kept separate from bulk traffic | Template migration, event handling, and account or domain constraints |
| A plain REST option such as Infrai | HTTP-native signup service with reusable templates and no SMTP requirement | Domain/DKIM readiness plus polling capacity, freshness SLO, and application-owned token logic |

The rows are not interchangeable. An existing SMTP estate makes the HTTP-only option a poor migration target even if the first API call is easy. A mature webhook consumer favors a service that pushes events. A small, language-neutral client surface favors direct REST, provided the team explicitly staffs the event collector.

Here is a minimal sender shape. `RESET_EMAIL_JSON` is the request body validated against the send route's schema; keeping it outside the source prevents credentials and reset content from becoming deployment artifacts.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("RESET_EMAIL_JSON"))
	if key == "" || len(body) == 0 {
		panic("INFRAI_API_KEY and RESET_EMAIL_JSON are required")
	}

	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", baseURL+"/email/send", nil)
		if err != nil {
			panic(err)
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", os.Getenv("RESET_REQUEST_ID"))
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("send failed: %s: %s", resp.Status, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("email send remained rate limited after retries")
}
```

The sample needs one small import addition for `bytes`; the important operational details are visible: the HTTP method is explicit, the key comes from the environment, retries honor `Retry-After`, and a reset request ID makes retries idempotent. In production, reject an empty idempotency key before sending rather than allowing two recovery messages to represent one user action.

## What does a defensible selection rehearsal look like?

Run the same reset scenario against each shortlisted service using a staging sending domain: generate the application token, render the reviewed template, send one message, record the provider identifier, and exercise the event-reconciliation path. Do not call this a latency benchmark. The useful result is a responsibility map: which credentials exist, which components wake up on a failure, which states are retried, and which state remains invisible until a poll occurs.

I would record four gates: time to complete domain authentication, time to first accepted send, the number of application components required for bounce and complaint handling, and whether the chosen event cadence fits the reset SLO without wasteful polling. A team that cannot answer the last question has not finished integration planning.

Choose direct REST when the service can call HTTP APIs, can own token lifecycle, and can operate pull-based event reconciliation. Choose an SMTP-capable service when replacing the relay would add more work than it removes. Choose a webhook-oriented service when prompt event-driven suppression or resend actions are a hard requirement. In every case, reject the candidate that cannot make domain authentication, delivery state, and recovery ownership explicit.

The first email is easy. The durable decision is knowing who owns the next state after it leaves the signup service.

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Mailgun Sending Messages](https://documentation.mailgun.com/docs/mailgun/api-sending/send-messages)
- [Postmark Developer Documentation](https://postmarkapp.com/developer)
- [MDN: WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)
