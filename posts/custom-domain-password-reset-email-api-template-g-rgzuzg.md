# Custom Domain Password Reset Email API Template Governance for Media Reports

A media platform sending generated reports as email attachments needs a clear boundary between editorial templates and account recovery: a report release must never change the password reset message that lets a reader regain access. Short answer: use a dedicated transactional reset template, verify the custom sending domain for SPF and DKIM before release, and send through the email API; choose a service with pull-based delivery events only when the recovery workflow can tolerate the polling delay. Template ownership is the deciding constraint. A Node.js application has the same release boundary as the Go example below: the runtime does not decide who can change recovery copy.

The attachment requirement needs its own acceptance check. Before approving report dispatch, verify that the chosen provider's send schema accepts the generated file and that a controlled send produces the intended attachment. A template-send operation alone does not establish that contract.

## How should a transactional email API isolate password reset template changes?

Consider a bounded production design exercise: I would review a media application with a generated audience report, an editor-owned report message, and a platform-owned password reset template. Suppose a release changes the report's subject and layout while a reader requests account recovery. The review question is concrete: can that release accidentally select the report template, change the recovery destination, or delay a reset behind report generation? None of those outcomes requires a mail provider to be unavailable. They follow from giving two messages with different owners the same mutable configuration. This is a hypothetical release review, not a reported incident.

Separate the owners.

The invariant is that a reset request selects its destination from the identity record and its message from a separately approved recovery configuration; report content is neither an input to that selection nor a reason to hold the reset. Give the platform team approval responsibility for the reset template and sending-domain changes, while editorial staff own the report copy. These are application release policies, not claims that every provider enforces equivalent permissions. If a hosted template can be edited outside version control, the release evidence must identify the approved content and variables as well as the template identifier, because an unchanged identifier alone cannot establish unchanged content.

That is the failure this review is meant to prevent: a template identifier survives a deployment, so the deployment looks harmless, although the content behind it has changed. Version the approval evidence. A successful API response cannot tell the platform team who approved the wording.

A useful preproduction check deliberately changes the report configuration and verifies that the reset still selects the same approved recovery configuration. Another checks a controlled identity record whose mailbox differs from the requester's submitted address. This tests the handoff rather than a vendor dashboard's green status light.

## Buy versus build starts with template authority

The following comparison treats the named products as candidates for different parts of the workflow. It is not a claim that a mail transport alone implements secure account recovery.

| Option | What to buy | What the platform still owns | Selection boundary |
| --- | --- | --- | --- |
| SendGrid with an application-owned recovery flow | Mail transport and Dynamic Templates | Token lifecycle, identity-to-recipient mapping, template approval, and the integration between application state and hosted content | Useful when hosted template editing fits editorial ownership; decide which recovery changes require a platform release |
| Amazon SES with an application-owned recovery flow | Email transport and templated sending | Recovery tokens, template approval, event consumption, and operational ownership of the surrounding integration | Appropriate when the team wants control of the recovery implementation and accepts the AWS integration work |
| Postmark with an application-owned recovery flow | Transactional email delivery and templates | Identity-to-recipient mapping, reset-token lifecycle, and approval of template changes | A focused mail option when the identity system is settled; delivery does not replace the recovery service |
| Infrai with application-controlled recovery | A self-describing REST API with public discovery, schemas, and runnable examples; plain HTTP with no SDK required lets the team review the sending contract directly | Recovery-token generation and validation, template approvals, domain verification, and a polling worker | A strong fit when pull-based event tracking meets the workflow's timing needs; its documented idempotency convention provides an additional safeguard for retrying a logical send |

Public discovery addresses contract review; idempotency addresses repeated execution. Those are different benefits. For a reset worker, carrying a stable application attempt identifier into the documented Idempotency-Key convention reduces the risk that a retry creates another logical send, provided the operation declares idempotency support and the retry remains inside its deduplication window. The platform convention specifies a default window of 24 hours. Retain the application's own attempt record beyond whatever deduplication period the selected operation guarantees.

Infrai's one key, one bill model also matters when the report workflow uses other backend capabilities: a single API key across 295 routes and 20 modules reduces the number of credential lifecycles and billing accounts the team coordinates. That convenience deserves an ownership review, since sharing a credential also puts more responsibility on its holder. It doesn't establish attachment support, approve a template, or satisfy an access-control policy.

The trade-off is pull-only email events: this limitation makes the combined service unsuitable when the workflow requires real-time webhooks. Compare the event integration of SendGrid, Amazon SES, and Postmark against that requirement before choosing. An existing SMTP client is another poor fit for a direct-API-only sending path; it needs an application adapter, since there is no SMTP relay. Don't budget that migration as a configuration change.

Each candidate still needs a deliberate decision about where rendering occurs. Application rendering puts the content artifact in the application release; hosted rendering lets content change through the provider's template workflow, so the approval system must capture those changes. Neither arrangement is inherently the right one. If editors must change report language without a platform deployment, hosted report templates may fit well, while recovery copy remains separately approved. If all copy must travel through repository review, application rendering may be the simpler governance choice, provided the selected sending contract supports that path.

Buying transport leaves work.

Self-hosting the transport adds another operational responsibility without settling who owns the templates. Choose it only if operating that component belongs on the roadmap and the team can staff its on-call obligations; control over a mail process is not evidence of control over a recovery release.

## Capacity planning includes the observation delay

Set separate SLOs for accepting a recovery request, obtaining mail-provider acceptance, and observing the delivery outcome. They are different milestones. Neither provider acceptance nor a delivered event proves that a reader saw the message, and an identity lookup is not authorization to reset an account.

For an illustrative capacity budget, suppose 600 recovery messages are awaiting a terminal outcome and the observation target allows polling every 30 seconds. One request per message per interval would require about 20 requests per second before retries, pagination, or report traffic. Those are planning inputs, not measured provider limits. An event-list reader can have a different request profile; its actual batching and pagination behavior must come from the operation schema. If an event becomes visible just after a poll, periodic polling alone can add nearly a full interval, and neither event publication nor processing time is included in that bound.

Budget that delay explicitly.

Pull-only events suit a workflow that can reconcile delivery and bounce state asynchronously. An immediate cross-channel handoff needs an observation mechanism and timing budget that actually support it. Keep report dispatch and recovery worker capacity separately budgeted so an editorial send cannot consume the recovery allocation; choose the allocations from traffic observations, not an arbitrary promise about throughput. A fallback email code also needs application-owned generation and validation when the selected service provides email transport without managed email OTP.

Domain verification belongs before the first production send. Publish and verify the required SPF and DKIM configuration, review alignment with the sender domain under the chosen DMARC policy, and keep that evidence with the template release. Authentication improves the basis for inbox delivery; it cannot guarantee placement in US or EU inboxes.

## A preventative release check before the send adapter

The narrow code path below makes template approval a checkable local artifact. It is a complete Go program using the standard library, with two message purposes and separately owned approvals. It renders a controlled example message, deliberately changes the report template, and checks that the recovery rendering stays identical. It then checks that changing recovery content without renewing its approval is rejected. The sample data and approval map are illustrative application policy, not a provider request schema or a production authorization system.

Save the block as a Go source file and run it with the Go toolchain. It makes no network requests and sends no email.

```go
package main

import (
    "bytes"
    "crypto/sha256"
    "fmt"
    "strings"
    "text/template"
)

type Message struct {
    Owner  string
    Source string
}

type Approval struct {
    Owner  string
    Digest [32]byte
}

func render(purpose string, messages map[string]Message,
    approvals map[string]Approval, data any) (string, error) {
    message, ok := messages[purpose]
    if !ok {
        return "", fmt.Errorf("unknown purpose %q", purpose)
    }
    approval, ok := approvals[purpose]
    if !ok || approval.Owner != message.Owner ||
        approval.Digest != sha256.Sum256([]byte(message.Source)) {
        return "", fmt.Errorf("unapproved content for %s", purpose)
    }
    tmpl, err := template.New(purpose).Option("missingkey=error").Parse(message.Source)
    if err != nil {
        return "", err
    }
    var output bytes.Buffer
    if err := tmpl.Execute(&output, data); err != nil {
        return "", err
    }
    return output.String(), nil
}

func main() {
    messages := map[string]Message{
        "recovery": {Owner: "platform", Source: "Reset your password: {{.ResetURL}}"},
        "report":   {Owner: "editorial", Source: "Your {{.ReportName}} report is attached."},
    }
    approvals := make(map[string]Approval)
    for purpose, message := range messages {
        approvals[purpose] = Approval{
            Owner: message.Owner, Digest: sha256.Sum256([]byte(message.Source)),
        }
    }
    data := map[string]string{
        "ResetURL":   "https://accounts.example/reset/controlled-test",
        "ReportName": "Audience distribution",
    }
    before, err := render("recovery", messages, approvals, data)
    if err != nil {
        panic(err)
    }
    report := messages["report"]
    report.Source = "The revised {{.ReportName}} report is attached."
    messages["report"] = report
    after, err := render("recovery", messages, approvals, data)
    if err != nil || before != after {
        panic("report edit changed recovery rendering")
    }
    recovery := messages["recovery"]
    recovery.Source = strings.Replace(recovery.Source, "Reset", "Change", 1)
    messages["recovery"] = recovery
    if _, err := render("recovery", messages, approvals, data); err == nil {
        panic("unapproved recovery edit was accepted")
    }
    fmt.Println("Recovery isolation and approval checks passed.")
}
```

The digest detects a content change; it does not authenticate an approver. In production, only the authorized release process should write approval records, and the record should also bind the relevant subject, variable contract, and sender configuration. Automatically recomputing approvals during every deployment would defeat the check. The initialization loop here establishes a fixed sample baseline so the two negative cases can run without external setup.

This example uses plain text. HTML rendering needs the appropriate escaping and its own reviewed artifact. The report sentence also does not attach a file: attachment creation and the provider's verified attachment contract belong to a separate integration check. Keep these boundaries explicit when adding the send adapter, rather than interpreting a successful local render as an end-to-end delivery test.

The transport check below is a separate Go program for a controlled mailbox. Set `INFRAI_API_BASE_URL` to the documented HTTPS API base ending in `/v1`, `INFRAI_API_KEY` to the credential, `APPROVED_SEND_JSON` to the path of a schema-validated template-send JSON file, and `SEND_ATTEMPT_ID` to a persisted logical send identifier. Its POST operation uses `/v1/email/send`. Obtain the payload fields from public discovery and its runnable example; the program deliberately accepts the reviewed JSON unchanged, because template and attachment fields must not be guessed. The local approval check above belongs before this adapter in the application's release pipeline.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing " + name)
    }
    return value
}

func main() {
    base, err := url.Parse(required("INFRAI_API_BASE_URL"))
    if err != nil || base.Scheme != "https" || base.Host == "" ||
        strings.TrimRight(base.Path, "/") != "/v1" ||
        base.RawQuery != "" || base.Fragment != "" || base.User != nil {
        panic("use the documented HTTPS API base ending in /v1")
    }
    body, err := os.ReadFile(required("APPROVED_SEND_JSON"))
    if err != nil {
        panic(err)
    }
    if !json.Valid(body) {
        panic("send payload is not valid JSON")
    }
    key := required("INFRAI_API_KEY")
    attemptID := required("SEND_ATTEMPT_ID")
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    base.Path = "/v1/email/send"
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodPost, base.String(), bytes.NewReader(body))
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", attemptID)
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        result, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(result))
            return
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            panic(fmt.Sprintf("HTTP %d: %s", resp.StatusCode, result))
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
            if candidate := time.Duration(seconds) * time.Second; candidate > delay {
                delay = candidate
            }
        } else if deadline, err := http.ParseTime(retryAfter); err == nil {
            if candidate := time.Until(deadline); candidate > delay {
                delay = candidate
            }
        }
        time.Sleep(delay)
    }
}
```

Reuse the same payload and attempt identifier after a process restart. A network error leaves the outcome uncertain; reconcile the attempt before resending, and keep the operation's deduplication window in view. Don't treat a successful send response as a delivery observation. The example checks JSON syntax, while schema validation and approval remain prerequisites.

## Where this release rule stops helping

If a managed identity system already owns recovery tokens and rendering, review that system's supported customization path before inserting a separate template sender. A second rendering path can create another place for recovery policy to drift. Likewise, a workflow requiring immediate event-driven action should select a provider and integration whose event semantics meet that requirement, rather than trying to make a 30-second poll satisfy an instantaneous SLO.

The decision rule is practical: approve the reset template under platform ownership, let editorial own the generated report's copy, verify the report attachment contract independently, and budget delivery observation before choosing the transport. A transport decision is ready for production only when the team can explain which release changes each message and who is allowed to approve it.

## Sources

References for template ownership choices and sender authentication:

- SendGrid Dynamic Templates: https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates
- Amazon SES templated email: https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html
- Postmark templates: https://postmarkapp.com/developer/user-guide/transactional-email/working-with-templates
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7489, DMARC: https://datatracker.ietf.org/doc/html/rfc7489
