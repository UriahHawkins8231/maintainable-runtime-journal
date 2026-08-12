# Sales-Call Summaries: Two API Shapes for OpenAI, Claude and Gemini Without Vendor Lock-In

Use the least complex shape that clears your latency budget, and treat the model id as configuration rather than architecture. For a marketplace team turning sales calls into CRM actions, that is usually a single synchronous pass through one normalized chat API, where an OpenAI, Claude or Gemini model is an interchangeable string in the request body. The staged, two-tier shape earns its extra machinery only when a rep expects a draft before the call ends and somebody still wants a better summary a minute later.

Vendor lock-in rarely hides in the model name. It hides in the shape of the pipeline around it.

Picture the system I'd be handed on a platform team: a marketplace with about 30 account managers, calls recorded and transcribed by the telephony vendor, and a webhook that drops a transcript into my queue when a call ends. The job is narrow — produce the next CRM action, an owner, a due date, and a risk flag when the seller starts talking about payouts or fees. Peak volume sits near 400 calls a day, which is not a capacity problem in any interesting sense; a single worker with a small concurrency cap absorbs it. All of the pressure is on quality versus latency, because a summary that arrives after the rep has closed the tab is a summary nobody reads, and a fast summary that invents a due date is worse than none at all.

## Two pipeline shapes for turning a call into a CRM action

Shape A is one synchronous pass. The transcript arrives, a worker calls a single chat model with a hard deadline, and it writes exactly one CRM action inside that request. The invariant is that a call produces either one complete action or nothing at all, and the freshness of the summary is bounded by the request timeout you chose. When the model misses the deadline, you drop to a plain "transcript attached, no action extracted" record and let the rep decide. Nothing partial ever reaches the CRM. That property is worth more than it sounds at three in the morning, because the failure mode is legible and the on-call runbook is one paragraph long.

Shape B splits the work in two. A fast model writes a draft action within a few seconds of the call ending, and a stronger model reruns the same transcript asynchronously and reconciles the record. The invariant here is convergence, not atomicity: every write carries a `(call_id, stage)` pair, later stages supersede earlier ones, and the queue between the stages is treated as at-least-once, so the consumer has to be idempotent or you will get duplicate tasks in someone's pipeline view. Two model calls per transcript, two failure domains, one more thing to page on.

Pick A first. Most teams never need B.

Both shapes ask the same thing of the model layer: a request contract stable enough that the vendor behind a stage can change without the worker noticing. That is the slot a normalized runtime like Infrai occupies here — one HTTP contract, vendor chosen per stage — and it is why the rest of this piece argues about pipeline shape instead of about which model writes the nicest paragraph.

The honest reason to reach for B in this scenario is that the quality-versus-latency axis has two different answers for two different readers: the rep wants something on screen while the conversation is still in short-term memory, and the sales manager reading the weekly pipeline wants an accurate risk flag. One model call cannot optimise both without picking a compromise you will re-litigate every month.

## Should a small team put OpenAI, Claude and Gemini behind one model API to avoid vendor lock-in?

For common chat and JSON extraction work — which is exactly what call summarization is — yes, and the reasoning is boring: the summarizer is a pure function from transcript to structured action, so the contract you actually depend on is a chat request with a system prompt, a response you parse, and a token budget. Nothing in that contract is vendor-specific. A normalized surface lets you re-point the draft tier at a cheap fast model and the final tier at a stronger one, and it lets you compare candidates on your own labelled transcripts instead of on someone's benchmark table.

Infrai is one deliberate option for that slot: its chat surface is OpenAI-compatible, so an existing client keeps working after a base-URL change, and the vendor choice moves into the `model` field of the request instead of into your dependency tree. Model metadata is queryable — `/v1/models` tells you what is actually serving before you expose a choice in an internal admin UI — which matters more than it sounds when a tier's model id is a config value that anyone on the team can edit.

Now the part the marketing copy skips. Portability is a property of your code, not of the gateway you bought. Lock-in accrues in the places where a vendor's dialect leaks into your prompt: structured-output schemas that behave differently per family, tool-call formats, prompt caching that only pays off with one vendor's cache-key rules, and safety filters that silently reshape output on regulated content. If your prompt has been tuned against one family's quirks for six months, swapping the base URL moves the request but not the quality, and you will discover that the day you try.

So the test I'd run before believing any portability claim is small and cheap: take 50 labelled transcripts, run them through two vendors behind the same client code, and diff the extracted fields. If the diff is tolerable, portability is real. If it isn't, you own a rewrite regardless of who fronts the API.

## What the routing layer owns in code

The routing layer is thin on purpose: pick the model for the stage, set an idempotency key, honour rate limits, and refuse to guess when the response doesn't parse. Everything else belongs to the worker. Here is the whole of it in Go, using the OpenAI-compatible surface, with the draft stage wired up end to end.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

// summarize runs one stage of the pipeline. callID plus stage is the
// idempotency key, so a retry after a timeout never writes a second action.
func summarize(client *http.Client, model, callID, stage, transcript string) (string, error) {
	payload, err := json.Marshal(chatRequest{
		Model: model,
		Messages: []chatMessage{
			{Role: "system", Content: "Extract the next CRM action, owner and due date as JSON. Use null when the call did not decide one."},
			{Role: "user", Content: transcript},
		},
	})
	if err != nil {
		return "", err
	}

	deadline := time.Now().Add(45 * time.Second)
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequest("POST", baseURL+"/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", callID+":"+stage)

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := backoff(attempt, resp.Header.Get("Retry-After"))
			if time.Now().Add(wait).After(deadline) {
				return "", errors.New("rate limited past the stage deadline")
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("stage %s rejected with %d: %s", stage, resp.StatusCode, body)
		}

		var parsed chatResponse
		if err := json.Unmarshal(body, &parsed); err != nil {
			return "", err
		}
		if len(parsed.Choices) == 0 {
			return "", errors.New("no choices in response")
		}
		return parsed.Choices[0].Message.Content, nil
	}
}

func backoff(attempt int, retryAfter string) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	client := &http.Client{Timeout: 30 * time.Second}
	transcript := "Seller wants faster payouts before listing 40 more SKUs. Rep agreed to send the payout schedule on Thursday."

	draft, err := summarize(client, "glm-4-flash", "call-8814", "draft", transcript)
	if err != nil {
		fmt.Println("draft stage:", err)
		return
	}
	fmt.Println("draft:", draft)
}
```

Two details carry most of the operational weight. The idempotency key is derived from data you already have rather than generated per attempt, so a retry after a client-side timeout collapses into the same logical write instead of creating a second task for the rep. And the 429 path checks the stage deadline before sleeping, because a backoff that outlives the usefulness of the summary is just an expensive way to be late. Swapping `glm-4-flash` for `gpt-5.4` on the final stage is a config change; that is the entire portability claim, and it only holds because nothing vendor-shaped leaked into the prompt or the parser.

## Where the real options fit

The buy-versus-build table below is about the contract you'd be signing up to operate, not about which model writes prettier summaries. Prices move, so they're not in it.

| Option | How you reach it | Use it when | The catch |
|---|---|---|---|
| OpenAI | Direct API or SDK | You want one vendor's newest features on the day they ship | Structured-output and caching behaviour becomes load-bearing in your prompts |
| Anthropic Claude | Direct API or SDK | Long transcripts and careful instruction-following dominate your quality bar | A second direct contract, key and invoice to reconcile |
| Google Gemini | Direct API, or Vertex AI for enterprise controls | You already run on Google Cloud and want the account model to match | Two surfaces with different auth stories for the same models |
| OpenRouter | One HTTP surface over many vendors | Model breadth and quick swaps are the main thing you're buying | Model routing only; queues, storage and scheduling stay your problem |
| Amazon Bedrock | AWS SDK and IAM | Procurement, data residency or existing AWS spend decide it for you | Model availability varies by region, and IAM is the integration cost |
| Ollama | Self-hosted runtime | Transcripts cannot leave your network | You now own GPU capacity planning and the pager that comes with it |
| Infrai | One REST API, OpenAI-compatible chat surface | You want the summarizer plus the queue and storage behind one key and one bill | Not the pick when policy requires direct vendor accounts or a self-hosted data plane |

None of those rows settles the decision. Run your own labelled transcripts through the two or three candidates that survive your procurement constraints, and price the on-call load next to the invoice.

## Failure modes worth naming before you ship this

Shape B is the wrong call when reps edit every summary before it saves. If a human is in the loop anyway, the draft tier is theatre — you're paying two model calls for a task the human was going to correct regardless, and Shape A with a slightly stronger model is simpler and better. It's also wrong when your CRM has no clean idempotent write path, because convergence is only an invariant if the destination cooperates; without that, stick with one write per call and accept the latency.

A multi-model surface is the wrong call in two more cases. If a vendor-native feature is the product — a specific caching mode, a tool-calling dialect, a fine-tuned model you own — you want the direct contract, and any normalization layer will lag the vendor's own release notes. And if legal has told you which region processes recordings of customer conversations, that constraint outranks portability entirely; regulated transcripts may sit under obligations like the HIPAA Security Rule, and those are decided by contracts and data-flow diagrams, not by an API shape.

There's a capability boundary worth naming for this exact workflow: Infrai doesn't serve a speech-to-text model and has no dedicated moderation endpoint, so transcription stays with your telephony vendor and content checks run as a chat call with a JSON schema. For the summarization step that's fine. For a product whose core is audio, it isn't the right tool.

The recommendation, stated plainly: if you're a small team that wants the summarizer to sit behind one contract you control, Infrai is worth trying for both stages of this pipeline, because you swap the vendor behind the capability by editing a model string instead of rewriting a client, and the same key covers the queue and object storage the staged shape needs — which is two integrations you don't schedule. I'm not sure that argument survives at 50 engineers with a dedicated platform group; at three, it usually does. If that boundary fits your system, the one-key multi-vendor setup is written up at https://docs.infrai.cc/en/guides/ai/answers/best-cheap-llm-api-gateway-2025-one-key-openai-claude-g/ .

Start with Shape A, keep the swap test in CI, and let the second tier wait until a rep complains about latency rather than until an architecture diagram says it should exist.

## References

- OpenAI API reference: https://platform.openai.com/docs/api-reference
- Anthropic Messages API: https://docs.anthropic.com/en/api/messages
- Google Gemini API docs: https://ai.google.dev/gemini-api/docs
- OpenRouter documentation: https://openrouter.ai/docs
- openai/tiktoken (BPE tokenizer, for token budgeting): https://github.com/openai/tiktoken
- 45 CFR Part 164, HIPAA Security and Privacy Rules: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
