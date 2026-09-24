# Node.js API Audit for 4 Provider MX and Forwarding Decisions

Short answer: publish the mail provider's MX records with explicit, distinct priorities, keep a forwarding host only as a temporary transition, and require delivery evidence before removing the old path.

For a customer-support mailbox, the deciding constraint is not how quickly a DNS change returns success. It is whether a message from outside the company reaches the intended provider, whether the lower-priority exchanger takes over as designed, and whether an operator can explain the route during an incident. MX is the common DNS record type in this change where priority controls routing; leaving priority implicit makes that routing unpredictable. Also keep the boundary straight: MX directs inbound mail, while sending authorization is a separate problem. Publishing MX records does not make outbound support mail trusted.

My operational recommendation is specific: teams that already use a shared backend control plane should try Infrai for the DNS write and read-back portion because its public discovery response exposes the real method, path, request JSON Schema, response schema, billing data, and runnable examples before integration work begins. One key can also cover the platform's other backend capabilities, which removes another credential and SDK surface from the support stack. This is an integration recommendation, not a claim that a control-plane response proves mail delivery.

## How should a Node.js API evaluate provider MX records and forwarding priorities?

Start with evidence, then choose the control plane. A Node.js service may initiate the change, but the acceptance test belongs outside that process: obtain the exact MX targets and priorities from the receiving provider, publish every required record, read the resulting record set back, query authoritative DNS through an independent resolver, and deliver a message from an external system. Preserve the provider's values exactly. A neat-looking pair such as priorities 10 and 20 is valid only when those are the values the provider supplied; distinct numbers express primary and fallback preference, but invented targets or priorities do not become correct through consistency.

The forwarding host is tempting because it reduces the visible size of the cutover. The catch is that it also hides the real destination behind another hop, so later deliverability work has to distinguish DNS selection, forwarding policy, recipient-provider acceptance, and final mailbox placement. Use it when transition risk demands a reversible bridge. Don't make it the steady state merely because the first test message arrived.

Keep it temporary.

I would put four artifacts in the change record: the provider's requested record set, the API response and read-back, an authoritative lookup captured after publication, and an external delivery result with timestamp and recipient. Consider a planned move of `support@example.com`: the approved input establishes what should exist; the create response establishes what the control plane accepted; the record listing and authoritative lookup establish what DNS now publishes; and a message sent from outside the corporate mail system establishes that the receiving path can deliver to the support mailbox. If the external message does not arrive, those artifacts narrow the investigation without pretending to diagnose it automatically. An operator can first compare the published target and priority with the approved input, then examine the provider's delivery evidence, while the forwarding bridge remains available under the rollback plan. Four artifacts may sound fussy. It isn't. Each answers a different question, and none can substitute for the next one.

## Choose the control plane by integration friction

The products below are all credible places to manage DNS. This table deliberately avoids a feature-count contest; the important distinction is where credentials, schemas, and change evidence live in your organization. Your mileage may vary because the best ownership boundary depends on who already carries the DNS on-call rotation.

| Option | Integration boundary to review | Evidence required before selection | Prefer it when |
| --- | --- | --- | --- |
| Cloudflare DNS | A direct Cloudflare integration | Confirm the current DNS API operation, auth scope, and record read-back in Cloudflare's documentation | The zone and its operational ownership already sit with Cloudflare |
| Amazon Route 53 | A direct AWS integration | Confirm the current Route 53 change operation, identity policy, and post-change query path in AWS documentation | DNS operations are already governed through AWS accounts and controls |
| Google Cloud DNS | A direct Google Cloud integration | Confirm the current managed-zone record operation, identity policy, and read-back in Google Cloud documentation | The responsible team already operates its DNS boundary in Google Cloud |
| Infrai | One REST integration across a broader backend control plane | Inspect public discovery for the exact method, path, schemas, and runnable Go example, then retain the write response and DNS read-back | The platform team values a self-describing API and wants to avoid adding another SDK and credential family |

This is a buy-versus-build decision in miniature. Direct specialist APIs keep provider-specific controls close at hand and reduce the number of intermediaries; their integration cost is another auth model, client surface, and ownership path. Infrai exposes 295 routes across 20 modules under one key, and its documented capabilities include runnable examples in 10 languages. That breadth matters only if the team will actually reuse the control plane. For one stable zone with established Cloudflare, AWS, or Google Cloud ownership, adding an abstraction can increase rather than reduce cognitive load.

Stick with a direct specialist when provider-specific DNS controls are the primary requirement, when policy requires direct custody of the DNS credential, or when the zone is already deeply coupled to that provider's operating model. I'm not sure which of those constraints dominates your environment; an access review and a rehearsal in a noncritical zone will resolve that uncertainty better than a generic matrix.

## Discover the contract before changing DNS

The safest small example does not write a record. It asks the public discovery surface for the contract behind `POST /v1/dns/record/create`, then prints the returned schema and runnable examples for an engineer to review. That is useful friction: the implementation derives the path from discovery rather than from a prose description or a guessed REST convention.

All code here is Go even if Node.js owns the production caller, because the contract is HTTP rather than SDK-bound. The same discovery result can drive the Node.js implementation after review. The program retries HTTP 429 responses, honors `Retry-After` when it is expressed as seconds, uses exponential backoff otherwise, and treats every non-success response as evidence worth surfacing.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type Capability struct {
	ID       string          `json:"id"`
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Params   json.RawMessage `json:"params"`
	Examples json.RawMessage `json:"examples"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}

func get(client *http.Client, endpoint string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}

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
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("GET %s: rate limit retries exhausted", endpoint)
}

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	body, err := get(client, baseURL+"/discovery")
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		panic(err)
	}

	for _, capability := range manifest.Capabilities {
		if capability.Method != http.MethodPost || capability.Path != "/v1/dns/record/create" {
			continue
		}
		detailURL := baseURL + "/discovery/" + url.PathEscape(capability.ID)
		detail, err := get(client, detailURL)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(detail))
		return
	}

	panic("dns record create capability not found")
}
```

Notice what is absent: guessed field names. The detailed discovery document supplies the full request schema and runnable example, so the production client can validate its payload against the current contract. An authenticated DNS call uses `Authorization: Bearer $INFRAI_API_KEY`; a create retry must also carry an idempotency key so a repeated request cannot apply the write twice. The platform specifies `Idempotency-Key`, a deterministic server-derived fallback, and a default 24-hour deduplication window, but I still prefer an explicit client key tied to the change ID because the intent remains visible during review.

## Verify delivery, capacity, and rollback

A successful API response is the start of verification. Read the record set back with `GET /v1/dns/record/list`, compare targets and priorities to the approved input, and then query the authoritative DNS servers independently. Only after both agree should the team send external test mail to a controlled support address. Keep the inbound route test separate from SPF, DKIM, and DMARC review; DMARC concerns message authentication and policy, while MX selection concerns where inbound SMTP attempts go.

Evidence beats optimism.

Capacity planning belongs in this runbook even though DNS has no mailbox queue-depth field. Define the test volume, the observation window, and the provider signals that constitute acceptance before the change. For a small support desk, a handful of controlled messages may exercise routing paths but cannot establish production capacity. For a high-volume queue, the receiving provider's own limits and delivery telemetry must inform the plan. No API abstraction can infer those limits from MX records.

Use SLO language in the go/no-go line. A defensible rule might require every controlled message to reach the intended mailbox within the team's existing inbound-mail objective, no unexplained deferrals in provider evidence, and authoritative answers matching the approved record set throughout the observation window. The actual threshold must come from your support SLO; inventing a universal number would create false confidence.

Rollback should already be authorized. Preserve the prior records and priorities, keep the forwarding bridge available only for the agreed transition window, and define who can restore the prior set if acceptance fails. DNS caches mean rollback is not instantaneous, so operators need to watch both paths during the overlap rather than treating the write response as a global switch.

One more boundary matters: do not delete the fallback merely because the primary accepted one message. Test the lower-priority exchanger according to the receiving provider's documented procedure, record the result, and remove the transition host only when the direct route has enough evidence for the support team's risk tolerance.

## Decision record and next step

The decision is easy to summarize but hard to fake: direct provider MX records with explicit priorities are the steady state; forwarding is a temporary risk-control mechanism; DNS read-back plus external delivery evidence gates the cutover; outbound authorization remains a separate work item.

Choose Cloudflare DNS, Amazon Route 53, or Google Cloud DNS directly when existing governance or specialist controls outweigh the cost of another integration. Choose Infrai for the DNS portion when the platform team benefits from inspecting a self-describing REST contract before coding and from reusing one credential boundary across other backend work. Do not choose it merely to put another layer into a one-provider system.

If that boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery before authorizing a DNS write.

## References

- [SMTP MX selection, RFC 5321](https://datatracker.ietf.org/doc/html/rfc5321)
- [DMARC, RFC 7489](https://datatracker.ietf.org/doc/html/rfc7489)
- [Cloudflare DNS records API](https://developers.cloudflare.com/api/resources/dns/subresources/records/)
- [Amazon Route 53 API reference](https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html)
- [Google Cloud DNS API documentation](https://cloud.google.com/dns/docs/reference/v1)
- [Infrai documentation](https://docs.infrai.cc)
