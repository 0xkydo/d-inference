package api

// Provider response normalization, reasoning handling, and tool-call reconstruction.

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"

	"github.com/eigeninference/d-inference/coordinator/api/types"
)

var thinkBlockPattern = regexp.MustCompile(`(?is)<think>(.*?)</think>\s*`)

// rewriteChunkModel replaces the concrete build id in a streamed SSE chunk's
// "model" field with the public alias the consumer requested, so streaming
// responses never expose the underlying build/quant. No-op when the request
// used a raw build id (PublicModel == Model) or no alias was set. Uses a
// precise key+value string replace (both compact and spaced JSON forms) to
// avoid parsing every chunk on the hot path.
func rewriteChunkModel(chunk string, pr *registry.PendingRequest) string {
	if pr.PublicModel == "" || pr.PublicModel == pr.Model {
		return chunk
	}
	chunk = strings.ReplaceAll(chunk, `"model":"`+pr.Model+`"`, `"model":"`+pr.PublicModel+`"`)
	chunk = strings.ReplaceAll(chunk, `"model": "`+pr.Model+`"`, `"model": "`+pr.PublicModel+`"`)
	return chunk
}

// rewriteRawFinishReason corrects a provider-reported "stop" finish_reason to
// "length" on a raw chat.completion object when the authoritative token counts
// show generation consumed the entire max-tokens budget.
func rewriteRawFinishReason(obj map[string]any, usage protocol.UsageInfo, requestedMax int) {
	if !truncatedByMaxTokens(usage, requestedMax) {
		return
	}
	choices, ok := obj["choices"].([]any)
	if !ok {
		return
	}
	for _, rawChoice := range choices {
		if choice, ok := rawChoice.(map[string]any); ok {
			if fr, _ := choice["finish_reason"].(string); fr == "stop" {
				choice["finish_reason"] = "length"
			}
		}
	}
}

func normalizeCompleteChatResponse(obj map[string]any, requestedModel string) {
	if requestedModel != "" {
		obj["model"] = requestedModel
	}
	for _, key := range []string{"system_fingerprint"} {
		if v, ok := obj[key]; ok && v == nil {
			delete(obj, key)
		}
	}
	choices, ok := obj["choices"].([]any)
	if !ok {
		return
	}
	for choicePosition, rawChoice := range choices {
		choice, ok := rawChoice.(map[string]any)
		if !ok {
			continue
		}
		choiceIndex := normalizedChoiceIndex(choice["index"], choicePosition)
		if message, ok := choice["message"].(map[string]any); ok {
			normalizeCompleteMessage(message, choiceIndex)
		}
		if delta, ok := choice["delta"].(map[string]any); ok {
			normalizeCompleteMessage(delta, choiceIndex)
		}
	}
}

func canonicalReasoningDetails(reasoning string, choiceIndex int) []types.ReasoningDetail {
	return []types.ReasoningDetail{{
		Type:   "reasoning.text",
		Text:   reasoning,
		ID:     "reasoning-text-" + strconv.Itoa(choiceIndex),
		Format: "unknown",
		Index:  0,
	}}
}

func normalizedChoiceIndex(raw any, fallback int) int {
	switch index := raw.(type) {
	case int:
		if index >= 0 {
			return index
		}
	case int64:
		converted := int(index)
		if index >= 0 && int64(converted) == index {
			return converted
		}
	case float64:
		intLimit := math.Ldexp(1, strconv.IntSize-1)
		if math.IsNaN(index) || math.IsInf(index, 0) || index < 0 || index >= intLimit || math.Trunc(index) != index {
			break
		}
		return int(index)
	case json.Number:
		if parsed, err := strconv.ParseInt(index.String(), 10, strconv.IntSize); err == nil && parsed >= 0 {
			return int(parsed)
		}
	}
	return fallback
}

func normalizeCompleteMessage(message map[string]any, choiceIndex int) {
	var extractedReasoning string
	if content, ok := message["content"]; !ok || content == nil {
		message["content"] = ""
	} else if contentText, ok := content.(string); ok {
		cleaned, reasoning := stripThinkBlocks(contentText)
		message["content"] = cleaned
		extractedReasoning = reasoning
	}

	if rc, ok := message["reasoning_content"]; ok {
		if rcText, ok := rc.(string); ok && rcText != "" {
			mergeReasoningField(message, rcText)
		}
		delete(message, "reasoning_content")
	}
	if reasoning, ok := message["reasoning"]; ok && reasoning == nil {
		delete(message, "reasoning")
	}
	if extractedReasoning != "" {
		mergeReasoningField(message, extractedReasoning)
	}
	if reasoning, ok := message["reasoning"].(string); ok && reasoning != "" {
		message["reasoning_content"] = reasoning
		if _, hasDetails := message["reasoning_details"]; !hasDetails {
			message["reasoning_details"] = canonicalReasoningDetails(reasoning, choiceIndex)
		}
	}
	for _, key := range []string{"tool_calls", "refusal"} {
		if v, ok := message[key]; ok && v == nil {
			delete(message, key)
		}
	}
}

func mergeReasoningField(message map[string]any, reasoning string) {
	reasoning = strings.TrimSpace(reasoning)
	if reasoning == "" {
		return
	}
	if existing, ok := message["reasoning"].(string); ok && strings.TrimSpace(existing) != "" {
		if existing != reasoning && !strings.Contains(existing, reasoning) {
			message["reasoning"] = existing + "\n\n" + reasoning
		}
		return
	}
	message["reasoning"] = reasoning
}

func stripThinkBlocks(text string) (string, string) {
	matches := thinkBlockPattern.FindAllStringSubmatch(text, -1)
	reasoningParts := make([]string, 0, len(matches)+1)
	found := len(matches) > 0
	for _, match := range matches {
		if len(match) > 1 {
			if part := strings.TrimSpace(match[1]); part != "" {
				reasoningParts = append(reasoningParts, part)
			}
		}
	}
	cleaned := thinkBlockPattern.ReplaceAllString(text, "")
	lower := strings.ToLower(cleaned)
	if idx := strings.Index(lower, "<think>"); idx >= 0 {
		found = true
		if part := strings.TrimSpace(cleaned[idx+len("<think>"):]); part != "" {
			reasoningParts = append(reasoningParts, part)
		}
		cleaned = cleaned[:idx]
	}
	if !found {
		return text, ""
	}
	return strings.TrimSpace(cleaned), strings.Join(reasoningParts, "\n\n")
}

// normalizeSSEChunk fixes fields in SSE chunks to match the OpenAI spec.
// Some backends emit "content":null instead of "content":"",
// and include "usage":null which strict parsers (ForgeCode, Codex) reject
// because they expect usage to be either absent or a full object.
func normalizeSSEChunk(chunk string) string {
	line := strings.TrimPrefix(chunk, "data: ")
	// Only trigger the expensive JSON parse for fields we actually fix.
	// "finish_reason":null appears on every chunk but we don't touch it, so
	// the gates scan for the fixable `"<key>":null` shapes and the reasoning
	// aliases in a pass each (sse_normalize_gate.go) instead of one
	// strings.Contains per field.
	if !sseChunkNeedsNullFix(line) && !sseChunkHasReasoningField(line) {
		return chunk
	}
	return rewriteSSEChunkFields(chunk, line)
}

// rewriteSSEChunkFields is normalizeSSEChunk's slow path: the JSON round-trip
// that rewrites null delta fields, drops null top-level fields, mirrors the
// reasoning aliases and synthesises reasoning_details. line is chunk without
// its "data: " prefix. Returns chunk unchanged when nothing needed fixing or
// the payload is not a JSON object.
func rewriteSSEChunkFields(chunk, line string) string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return chunk
	}

	changed := false

	// Remove top-level null fields (usage, system_fingerprint, etc.)
	// ForgeCode expects usage to be absent or a full object, not null.
	for _, key := range []string{"usage", "system_fingerprint"} {
		if v, ok := raw[key]; ok && string(v) == "null" {
			delete(raw, key)
			changed = true
		}
	}

	// Fix null fields inside choices[].delta
	if choicesRaw, ok := raw["choices"]; ok {
		var choices []map[string]json.RawMessage
		if err := json.Unmarshal(choicesRaw, &choices); err == nil {
			for i, choice := range choices {
				if deltaRaw, ok := choice["delta"]; ok {
					var delta map[string]json.RawMessage
					if err := json.Unmarshal(deltaRaw, &delta); err == nil {
						deltaChanged := false
						for _, field := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
							if v, ok := delta[field]; ok && string(v) == "null" {
								delta[field] = json.RawMessage(`""`)
								deltaChanged = true
							}
						}
						if v, ok := delta["tool_calls"]; ok && string(v) == "null" {
							delta["tool_calls"] = json.RawMessage(`[]`)
							deltaChanged = true
						}
						// reasoning_content is provider-canonical for streaming deltas.
						// If either alias is present, emit both with the same value, even
						// when that value is empty; empty values never produce details.
						reasoningRaw, hasReasoning := delta["reasoning"]
						reasoningContentRaw, hasReasoningContent := delta["reasoning_content"]
						reasoning := nonEmptyRawString(reasoningRaw)
						reasoningContent := nonEmptyRawString(reasoningContentRaw)
						chosenReasoning := reasoningContent
						chosenRaw := reasoningContentRaw
						if chosenReasoning == "" && reasoning != "" {
							chosenReasoning = reasoning
							chosenRaw = reasoningRaw
						} else if chosenReasoning == "" && !hasReasoningContent {
							chosenRaw = reasoningRaw
						}
						if hasReasoning || hasReasoningContent {
							if !hasReasoning || !bytes.Equal(reasoningRaw, chosenRaw) {
								delta["reasoning"] = chosenRaw
								deltaChanged = true
							}
							if !hasReasoningContent || !bytes.Equal(reasoningContentRaw, chosenRaw) {
								delta["reasoning_content"] = chosenRaw
								deltaChanged = true
							}
						}
						if _, hasDetails := delta["reasoning_details"]; !hasDetails && chosenReasoning != "" {
							choiceIndex := normalizedRawChoiceIndex(choice["index"], i)
							details, err := json.Marshal(canonicalReasoningDetails(chosenReasoning, choiceIndex))
							if err == nil {
								delta["reasoning_details"] = details
								deltaChanged = true
							}
						}
						if deltaChanged {
							choices[i]["delta"], _ = json.Marshal(delta)
							changed = true
						}
					}
				}
			}
			if changed {
				raw["choices"], _ = json.Marshal(choices)
			}
		}
	}

	if !changed {
		return chunk
	}

	out, err := json.Marshal(raw)
	if err != nil {
		return chunk
	}
	return "data: " + string(out)
}

func nonEmptyRawString(raw json.RawMessage) string {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func normalizedRawChoiceIndex(raw json.RawMessage, fallback int) int {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '"' {
		return fallback
	}
	var index json.Number
	if json.Unmarshal(raw, &index) == nil {
		return normalizedChoiceIndex(index, fallback)
	}
	return fallback
}

// maxLogicalToolCalls caps how many logical tool calls a single stream may
// reconstruct. Chunks come from providers, which are only semi-trusted: a
// buggy or malicious provider could otherwise stream an unbounded number of
// distinct-id tool-call deltas and grow the reconstruction (and its wire-index
// tracking) without limit. Real parallel-tool-call fan-out is tiny (typically
// <= 4 calls), so 128 is far above anything legitimate while bounding the
// worst case. Past the cap, deltas that would START a new logical call are
// dropped (counted + logged); argument fragments for already-kept calls still
// accumulate, so kept calls are never truncated or corrupted.
const maxLogicalToolCalls = 128

// toolCallWireIndex reads an accumulated tool-call entry's wire index.
// Entries are always created with an int "index" (toolCallAccumulator.apply),
// so the comma-ok is defensive only: a corrupt entry cannot panic the request
// path — it sorts last, keeping arrival order relative to other corrupt
// entries.
func toolCallWireIndex(tc map[string]any) int {
	idx, ok := tc["index"].(int)
	if !ok {
		return math.MaxInt
	}
	return idx
}

// toolCallAccumulator reconstructs logical tool calls from streamed deltas.
// Logical calls are kept in ARRIVAL order (calls) because a wire index is
// NOT unique: legacy provider builds emit every parallel call with index 0
// (E6; the engine at this branch's pin assigns distinct indices, but the
// fleet updates slowly), so index-keyed storage alone would let a second
// call overwrite the first's id/name and concatenate both argument streams
// into one corrupted call.
// activeByIndex maps each wire index to the position (in calls) of its
// CURRENT logical call — the one still receiving that index's id-less
// argument fragments; a delta whose non-empty id DIFFERS from that entry's
// non-empty id starts a NEW logical call, so well-behaved indexed streams
// are unchanged.
type toolCallAccumulator struct {
	calls         []map[string]any
	activeByIndex map[int]int
	// droppedDeltas counts deltas swallowed past maxLogicalToolCalls
	// (dropped new logical calls and their argument fragments).
	droppedDeltas int
}

func newToolCallAccumulator() *toolCallAccumulator {
	return &toolCallAccumulator{activeByIndex: map[int]int{}}
}

// apply folds one streamed delta into the accumulator. Past
// maxLogicalToolCalls, a delta that would start a new logical call is
// dropped and its wire index forgotten, so the dropped call's later id-less
// fragments can never accumulate onto a kept call; kept calls keep receiving
// their own fragments as usual.
func (a *toolCallAccumulator) apply(tc streamToolCallDelta) {
	pos, ok := a.activeByIndex[tc.Index]
	if ok && tc.ID != "" {
		if existingID, _ := a.calls[pos]["id"].(string); existingID != "" && existingID != tc.ID {
			// A NEW id on an already-active wire index is a new logical
			// call, not a continuation (the all-index-0 engine shape) —
			// never merge two calls into one.
			ok = false
		}
	}
	if !ok {
		if len(a.calls) >= maxLogicalToolCalls {
			// Cap reached: drop the new logical call. The kept call at
			// this wire index (if any) is complete as-is. This also
			// bounds activeByIndex: keys are only ever added alongside a
			// kept call, so both structures stay <= maxLogicalToolCalls
			// entries no matter how many distinct ids or sparse wire
			// indices are streamed.
			a.droppedDeltas++
			delete(a.activeByIndex, tc.Index)
			return
		}
		entry := map[string]any{
			"index": tc.Index,
			"function": map[string]any{
				"arguments": "",
			},
		}
		a.calls = append(a.calls, entry)
		pos = len(a.calls) - 1
		a.activeByIndex[tc.Index] = pos
	}
	entry := a.calls[pos]
	if tc.ID != "" {
		entry["id"] = tc.ID
	}
	if tc.Type != "" {
		entry["type"] = tc.Type
	}
	fn, fnOK := entry["function"].(map[string]any)
	if !fnOK {
		// Defensive: entries are always created with a function map —
		// never panic the request path on a corrupt entry.
		fn = map[string]any{}
		entry["function"] = fn
	}
	if tc.Function.Name != "" {
		fn["name"] = tc.Function.Name
	}
	args, _ := fn["arguments"].(string)
	fn["arguments"] = args + tc.Function.Arguments
}

// finalize returns the reconstructed calls (nil when there are none),
// ordered by wire index for well-behaved indexed streams; the stable sort
// preserves ARRIVAL order among equal indices (the all-index-0 shape), and —
// unlike the old dense 0..n-1 map walk — sparse indices are never dropped.
// The internal "index" key is stripped from the returned maps.
func (a *toolCallAccumulator) finalize() []map[string]any {
	if len(a.calls) == 0 {
		return nil
	}
	sort.SliceStable(a.calls, func(i, j int) bool {
		return toolCallWireIndex(a.calls[i]) < toolCallWireIndex(a.calls[j])
	})
	out := make([]map[string]any, 0, len(a.calls))
	for _, tc := range a.calls {
		delete(tc, "index")
		out = append(out, tc)
	}
	return out
}

// extractedMessage holds the reconstructed assistant message from SSE chunks,
// including text content, reasoning, and any tool calls.
type extractedMessage struct {
	Content                 string           `json:"content"`
	Reasoning               string           `json:"reasoning,omitempty"`
	ReasoningDetails        any              `json:"reasoning_details,omitempty"`
	ReasoningDetailsPresent bool             `json:"-"`
	ToolCalls               []map[string]any `json:"tool_calls,omitempty"`
	FinishReason            string           `json:"-"`
}

// extractMessage retains the historical reasoning-first behavior used by
// Responses and generic endpoint fallbacks.
func extractMessage(chunks []string) extractedMessage {
	return extractMessageWithReasoningPolicy(chunks, false)
}

func extractMessageWithReasoningPolicy(chunks []string, preferReasoningContent bool) extractedMessage {
	var contentBuilder strings.Builder
	var reasoningBuilder strings.Builder
	finishReason := ""
	var reasoningDetails any
	reasoningDetailsPresent := false
	reasoningDetailsNull := false
	// See toolCallAccumulator for the logical-call model (arrival order,
	// non-unique wire indices, the maxLogicalToolCalls cap).
	acc := newToolCallAccumulator()

	for _, chunk := range chunks {
		line := strings.TrimPrefix(chunk, "data: ")
		line = strings.TrimSpace(line)
		if line == "" || line == "[DONE]" {
			continue
		}

		var parsed map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			continue
		}

		choicesRaw, ok := parsed["choices"]
		if !ok {
			continue
		}
		var choices []struct {
			Delta struct {
				Content          string                `json:"content"`
				Reasoning        string                `json:"reasoning"`
				ReasoningContent string                `json:"reasoning_content"`
				ReasoningDetails json.RawMessage       `json:"reasoning_details"`
				ToolCalls        []streamToolCallDelta `json:"tool_calls,omitempty"`
			} `json:"delta"`
			Message struct {
				Content          string                `json:"content"`
				Reasoning        string                `json:"reasoning"`
				ReasoningContent string                `json:"reasoning_content"`
				ReasoningDetails json.RawMessage       `json:"reasoning_details"`
				ToolCalls        []streamToolCallDelta `json:"tool_calls,omitempty"`
			} `json:"message"`
			FinishReason *string `json:"finish_reason"`
		}
		if err := json.Unmarshal(choicesRaw, &choices); err != nil {
			continue
		}

		for _, c := range choices {
			if c.FinishReason != nil && *c.FinishReason != "" {
				finishReason = *c.FinishReason
			}
			if c.Delta.Content != "" {
				contentBuilder.WriteString(c.Delta.Content)
			} else if c.Message.Content != "" {
				contentBuilder.WriteString(c.Message.Content)
			}
			if preferReasoningContent {
				if c.Delta.ReasoningContent != "" {
					reasoningBuilder.WriteString(c.Delta.ReasoningContent)
				} else if c.Delta.Reasoning != "" {
					reasoningBuilder.WriteString(c.Delta.Reasoning)
				} else if c.Message.ReasoningContent != "" {
					reasoningBuilder.WriteString(c.Message.ReasoningContent)
				} else if c.Message.Reasoning != "" {
					reasoningBuilder.WriteString(c.Message.Reasoning)
				}
			} else if c.Delta.Reasoning != "" {
				reasoningBuilder.WriteString(c.Delta.Reasoning)
			} else if c.Delta.ReasoningContent != "" {
				reasoningBuilder.WriteString(c.Delta.ReasoningContent)
			} else if c.Message.Reasoning != "" {
				reasoningBuilder.WriteString(c.Message.Reasoning)
			} else if c.Message.ReasoningContent != "" {
				reasoningBuilder.WriteString(c.Message.ReasoningContent)
			}
			for _, rawDetails := range []json.RawMessage{c.Delta.ReasoningDetails, c.Message.ReasoningDetails} {
				trimmed := bytes.TrimSpace(rawDetails)
				if len(trimmed) == 0 {
					continue
				}
				if trimmed[0] == '[' {
					var details []json.RawMessage
					if json.Unmarshal(rawDetails, &details) != nil {
						continue
					}
					if !reasoningDetailsPresent || reasoningDetailsNull {
						reasoningDetails = make([]json.RawMessage, 0, len(details))
						reasoningDetailsPresent = true
						reasoningDetailsNull = false
					}
					if accumulated, ok := reasoningDetails.([]json.RawMessage); ok {
						reasoningDetails = append(accumulated, details...)
					}
				} else if !reasoningDetailsPresent || reasoningDetailsNull {
					reasoningDetails = json.RawMessage(append([]byte(nil), rawDetails...))
					reasoningDetailsPresent = true
					reasoningDetailsNull = bytes.Equal(trimmed, []byte("null"))
				}
			}
			toolCalls := c.Delta.ToolCalls
			if len(toolCalls) == 0 {
				toolCalls = c.Message.ToolCalls
			}
			for _, tc := range toolCalls {
				acc.apply(tc)
			}
		}
	}

	content := contentBuilder.String()
	reasoning := reasoningBuilder.String()
	if cleaned, extractedReasoning := stripThinkBlocks(content); extractedReasoning != "" {
		content = cleaned
		if strings.TrimSpace(reasoning) != "" {
			reasoning += "\n\n" + extractedReasoning
		} else {
			reasoning = extractedReasoning
		}
	}
	msg := extractedMessage{
		Content:                 content,
		Reasoning:               reasoning,
		ReasoningDetails:        reasoningDetails,
		ReasoningDetailsPresent: reasoningDetailsPresent,
		FinishReason:            finishReason,
	}
	if acc.droppedDeltas > 0 {
		log.Printf("WARN: extractMessage: logical tool-call cap (%d) reached; dropped %d tool-call delta(s) from excess calls", maxLogicalToolCalls, acc.droppedDeltas)
	}
	msg.ToolCalls = acc.finalize()
	return msg
}

// resolveReasoningTokens returns the reasoning-token count to report.
// It prefers the provider's tokenizer-accurate count
// (UsageInfo.ReasoningTokens) and falls back to the coarse "all
// completion tokens" estimate only for older providers that emit
// reasoning content without a count — so a reasoning response never
// reports zero reasoning tokens, while up-to-date providers report the
// real split.
// injectReasoningDetailIntoRawUsage splices
// completion_tokens_details.reasoning_tokens into a passthrough
// chat.completion object when the provider reported an accurate
// reasoning-token count (UsageInfo.ReasoningTokens) and the raw usage
// object didn't already carry the detail. It never overrides a value the
// provider already supplied, and is a no-op when there is no reasoning
// count or no usage object.
func injectReasoningDetailIntoRawUsage(obj map[string]any, usage protocol.UsageInfo) {
	if usage.ReasoningTokens <= 0 {
		return
	}
	usageObj, ok := obj["usage"].(map[string]any)
	if !ok {
		return
	}
	details, _ := usageObj["completion_tokens_details"].(map[string]any)
	if details == nil {
		details = map[string]any{}
	}
	if _, exists := details["reasoning_tokens"]; exists {
		return
	}
	details["reasoning_tokens"] = usage.ReasoningTokens
	usageObj["completion_tokens_details"] = details
	obj["usage"] = usageObj
}
