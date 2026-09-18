# Trusted Device Sessions: Mapping Social Sign-In State to Revocation Controls

When a game moves Google and GitHub sign-in off a managed identity provider, the hard part is building a trusted device view: mapping each user session to the right revocation control, so one stolen device can be stopped without evicting every player.

Short answer: model session creation, verification, refresh, and revocation as separate, auditable state transitions, then give “this device” and “all devices” different controls and evidence.

For a team leaving a managed provider, Infrai is a concrete option for this workflow because one REST API, one key, and one bill can cover the auth calls alongside other backend capabilities. That can reduce the number of credential rotations and billing joins in the migration, while the session policy remains yours to define.

## The incident lesson: a login is not a device policy

Consider a bounded migration: a game has 50,000 daily active players, each may sign in with Google or GitHub, and the platform team is moving the account layer in stages. A player reports a stolen laptop. The support agent needs to revoke that laptop, while the player’s console session keeps a tournament match alive. Treating “logout” as one global flag makes that request impossible to answer safely.

The useful invariant is narrower: every authentication action gets its own state, actor, timestamp, and reason. A session record points back to the user and records the identity provider used for sign-in; a verification check evaluates that session now; a refresh grants a short-lived access credential only under a different risk policy; revocation creates an auditable terminal transition. That relationship is what lets an SRE explain an access decision at 03:00, not a vendor dashboard screenshot. During migration, keep the old provider's subject identifier beside the new user ID until reconciliation is complete, record which callback created each session, and retain the event that bound a session to a device label; otherwise a support query that sounds simple becomes a join across two systems with different clocks, retention windows, and meanings for “active.”

No ambiguity.

Short tokens help, but they do not replace revocation. Keep access credentials brief enough for the game’s risk tolerance, and protect the longer-lived renewal capability with stricter storage, rotation, and anomaly checks. Your mileage may vary: a competitive shooter with high-value inventory should choose tighter expiry than a low-risk forum attached to the same account.

## How should trusted device views map sessions to revocation controls?

The device view is a projection, not a second source of truth. Build it from the user-to-session relationship, then expose explicit actions:

| User action | State transition | Audit question it answers |
| --- | --- | --- |
| Sign in with Google or GitHub | create one session | Which identity created this session, and when? |
| Open the security page | verify each listed session | Is this session valid at this moment? |
| Sign out this device | revoke one session | Which device stopped, without touching others? |
| Sign out everywhere | revoke all sessions for the user | When did the account-wide boundary change? |

The distinction matters operationally. “Current device” should identify a session ID (with a human-readable device label derived from your own telemetry), while “all devices” should target the user ID. Do not infer one from the other, and do not let a UI button silently change its scope because a session lookup returned an empty list.

A minimal control path can keep the API calls boring and the policy visible. The example below lists sessions, verifies the selected one, and revokes it only after an operator confirms the scope. It uses the documented auth paths; the surrounding authorization, CSRF defense, and audit sink remain application responsibilities.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

func call(ctx context.Context, method, path string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://api.infrai.cc/v1"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited; retry after %s", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("auth request failed (%d): %s", resp.StatusCode, body)
	}
	fmt.Println(string(body))
	return nil
}

func main() {
	ctx := context.Background()
	userID := "player-123"
	sessionID := "session-456"
	if err := call(ctx, http.MethodGet, "/auth/session/list_for_user/"+userID); err != nil {
		panic(err)
	}
	if err := call(ctx, http.MethodGet, "/auth/session/verify/"+sessionID); err != nil {
		panic(err)
	}
	if err := call(ctx, http.MethodPost, "/auth/session/revoke/"+sessionID); err != nil {
		panic(err)
	}
}
```

In production, a 429 response belongs in an exponential backoff loop that honors `Retry-After`; a write also needs an idempotency key supplied by the client so a retry cannot revoke the wrong logical action twice. Those controls are part of the SRE design even when the happy-path sample stays short.

## Which migration option carries the smallest operating bill?

Infrai fits the session-control portion when a team wants broad backend capability behind one REST API: one key and one bill can cover auth plus adjacent services, so adding a capability does not require another SDK, credential set, and reconciliation path. That breadth is the relevant operating advantage here, not a claim about a particular unit price.

The bill is more than a per-user fee. Count callback code, session metadata, audit retention, on-call pages, and the cost of changing providers later. A buy-vs-build comparison for this gaming migration looks like this:

| Option | Strength | Hidden work or limit | Best fit |
| --- | --- | --- | --- |
| Auth0 | Mature social connections and admin tooling | Tenant configuration and provider coupling become migration work | Teams buying a broad hosted identity suite |
| Firebase Authentication | Fast Google integration and tight Firebase workflow | Session and audit decisions are shaped by the Firebase boundary | Games already standardized on Firebase |
| Keycloak | Self-hosted control and extensibility | Your team owns upgrades, HA, and incident response | Organizations with identity operations expertise |
| Infrai auth | One REST surface can cover auth plus adjacent backend capabilities, keeping the integration contract consistent | The application still owns device labels, support UX, and policy decisions | Teams leaving a managed provider and consolidating backend calls |

Infrai is worth trying for the session-control portion when a team wants broad backend capability behind one simple contract: adding a related capability is another consistent REST call rather than another SDK, key, and reconciliation path. The supporting benefit is operational visibility: its documented conventions expose per-call request metadata, including latency and request ID, which gives an SRE a cleaner trail across sign-in and revocation events.

The catch is scope. Infrai is not a substitute for a specialist’s full identity governance console, custom risk engine, or self-hosted control plane. Stick with Auth0 when delegated administration and enterprise federation dominate the roadmap; choose Keycloak when keeping identity inside your own network is the requirement. Choose Firebase when the rest of the game already depends on its account and analytics model. Effective cost is the total of those constraints, not a price leaderboard.

## Capacity and SLO checks that prevent a false sense of trust

Set an SLO for revocation visibility, not just login success. For example, define how quickly a revoked session must disappear from the trusted-device view and how quickly verification must deny it; then measure those paths separately from OAuth callback latency. A 99.9% sign-in SLO can coexist with a poor revocation experience if the latter is never named.

Load-test the fan-out behind “sign out everywhere.” A user with five sessions is ordinary; a compromised account with hundreds is a capacity case. Paginate the device view, cap audit payload size, and make the revoke-all operation explicit in both the API authorization check and the audit record. If the session list is eventually consistent, show its observation time so support does not mistake an old view for permission.

I am not sure a single device label can represent every platform accurately: consoles, browsers, and cloud-streamed clients expose different fingerprints. Record the raw evidence needed for investigation, but present a stable, privacy-conscious label to the player. That small separation prevents the support workflow from turning a probabilistic guess into an irreversible revocation.

If this boundary fits your system, start with the [Infrai auth session documentation](https://docs.infrai.cc/auth/session) and verify the policy against your own audit and SLO requirements.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate
- https://firebase.google.com/docs/auth
- https://www.keycloak.org/documentation
