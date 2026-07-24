// go-chat: end-to-end test app for the Go agent-kit, driven by the real
// @inngest/use-agent React hook.
//
// Runs in DURABLE mode: the network executes inside an Inngest function, so
// every inference and tool call is a memoized step (kill the process
// mid-run and the replay skips completed work). Streaming chunks are
// published to Inngest realtime — the transport use-agent actually
// subscribes to.
//
//	                     POST /api/chat  ──send event──┐
//	web (:5173) ──────►  GET  /api/realtime/token      │
//	use-agent hook        GET/POST /api/threads…       ▼
//	     ▲                                    Inngest dev (:8299)
//	     └──── realtime WS ◄── realtime.Publish ◄── fn on /api/inngest
//
// Prereqs:
//
//	npx inngest-cli@latest dev -p 8299 -u http://localhost:8484/api/inngest
//	ANTHROPIC_API_KEY=sk-... go run .      # or put it in .env
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngestgo"
	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/anthropic"
	"net/url"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/durable"
)

const (
	appID = "go-chat"
	// chatEvent triggers the durable chat function.
	chatEvent = "go-chat/chat.requested"
	// streamTopic is the realtime topic carrying AgentMessageChunks. The
	// token minted for the browser grants exactly this channel+topic.
	streamTopic = "agent_stream"
	// defaultInngestAPI is the Inngest server. It MUST stay 8288: the
	// browser's @inngest/realtime hard-defaults its WebSocket host to
	// localhost:8288 (its env lookup can't see import.meta.env inside
	// Vite's pre-bundled deps, so every override falls through). Point the
	// Go side anywhere else and the UI connects to a server nothing
	// publishes to — it reads "live" and no chunk ever arrives.
	// Override with INNGEST_BASE_URL only if you also configure the client.
	defaultInngestAPI = "http://localhost:8288"
)

// inngestAPI is the resolved dev-server base URL (see defaultInngestAPI).
var inngestAPI = firstNonEmpty(os.Getenv("INNGEST_BASE_URL"), defaultInngestAPI)

// publishURL is the realtime publish endpoint on that server.
func publishURL() string { return inngestAPI + "/v1/realtime/publish" }

// chatState is the network's typed state. Title is mutated by the
// set_conversation_title tool — exercising the memoized state-patch
// re-apply path across replays (the pattern Clevix's real tools use).
type chatState struct {
	Title  string `json:"title,omitempty"`
	UserID string `json:"userId,omitempty"`
}

// chatRequest is the Inngest event payload.
type chatRequest struct {
	ThreadID   string `json:"threadId"`
	ChannelKey string `json:"channelKey"`
	UserID     string `json:"userId"`
	MessageID  string `json:"messageId"`
	Content    string `json:"content"`
}

// historyMessage is the shape use-agent's ThreadManager.formatRawHistoryMessages
// consumes (packages/use-agent/src/core/services/thread-manager.ts):
// message_id + createdAt on every row, then either type:"user" with
// content, or an assistant row whose data.output carries agent-kit Messages.
type historyMessage struct {
	MessageID string    `json:"message_id"`
	Type      string    `json:"type"` // "user" | "assistant"
	CreatedAt time.Time `json:"createdAt"`
	Content   string    `json:"content,omitempty"`
	Data      *struct {
		Output    []agentkit.Message `json:"output"`
		ToolCalls []agentkit.Message `json:"toolCalls,omitempty"`
	} `json:"data,omitempty"`
}

// newID mints identifiers for threads and messages.
func newID() string { return uuid.NewString() }

// --- agent + network ---

func buildAssistant() *agentkit.Agent[chatState] {
	getWeather := agentkit.NewTool[chatState]("get_weather",
		"Get the current weather for a city.",
		func(ctx context.Context, in struct {
			City string `json:"city" jsonschema:"description=The city to check"`
		}, opts agentkit.ToolOptions[chatState]) (any, error) {
			// Canned data — this app tests the framework, not a weather API.
			// Logged so a replay is visibly NOT re-executing it.
			log.Printf("  [tool] get_weather(%s) EXECUTED", in.City)
			return map[string]any{
				"city": in.City, "temperature_c": 21, "conditions": "sunny",
				"note": "canned test data from the go-chat example server",
			}, nil
		})

	setTitle := agentkit.NewTool[chatState]("set_conversation_title",
		"Set a short title for this conversation based on the user's request. Call it once early in a new conversation.",
		func(ctx context.Context, in struct {
			Title string `json:"title" jsonschema:"description=A concise 3-6 word title"`
		}, opts agentkit.ToolOptions[chatState]) (any, error) {
			log.Printf("  [tool] set_conversation_title(%q) EXECUTED", in.Title)
			opts.State.Data.Title = in.Title // memoized state-patch path
			return map[string]any{"ok": true, "title": in.Title}, nil
		})

	return agentkit.NewAgent(agentkit.AgentConfig[chatState]{
		Name:   "assistant",
		System: "You are a concise, friendly assistant. Use get_weather when asked about weather. For a brand-new conversation, call set_conversation_title once with a short title, then answer.",
		Tools:  []agentkit.Tool[chatState]{getWeather, setTitle},
	})
}

func buildNetwork(assistant *agentkit.Agent[chatState], model provider.LanguageModel, history *agentkit.HistoryConfig[chatState]) *agentkit.Network[chatState] {
	return agentkit.NewNetwork(agentkit.NetworkConfig[chatState]{
		Name:         "go-chat",
		Agents:       []*agentkit.Agent[chatState]{assistant},
		DefaultModel: model,
		// Extended thinking, so the UI has reasoning to stream. Keys are
		// AI-SDK camelCase (`budgetTokens`), and max output tokens MUST
		// exceed the thinking budget or the provider rejects the call.
		DefaultModelOptions: []agentkit.AgenticModelOption{
			agentkit.WithCallOptions(
				goai.WithMaxOutputTokens(4096),
				goai.WithProviderOptions(map[string]any{
					"thinking": map[string]any{
						"type": "enabled", "budgetTokens": 1024,
					},
				}),
			),
		},
		MaxIter: 5,
		// AgentKit's persistence seam (store.go): the framework drives
		// thread creation, the user-turn write, history hydration and
		// result writes itself — each inside its own durable step.
		History: history,
		// The network is the iteration authority (each agent call is exactly
		// one inference), so the router drives the tool round: keep routing
		// back to the assistant while its last result contains tool calls,
		// stop once it answered with plain text.
		Router: &agentkit.Router[chatState]{
			Fn: func(ctx context.Context, args agentkit.RouterArgs[chatState]) (*agentkit.RouterResult[chatState], error) {
				if args.CallCount == 0 {
					return agentkit.RouteTo(assistant), nil
				}
				if last := args.LastResult; last != nil && len(last.ToolCalls) > 0 {
					return agentkit.RouteTo(assistant), nil
				}
				return nil, nil
			},
		},
	})
}

func main() {
	loadDotEnv(".env")
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		log.Fatal("ANTHROPIC_API_KEY is required (export it, or put it in examples/go-chat/server/.env)")
	}
	// A signing key means a real/self-hosted server (`inngest start`), so
	// leave dev mode off and let INNGEST_BASE_URL carry the origin.
	// Otherwise assume `inngest dev` and flip the SDK into dev mode.
	if os.Getenv("INNGEST_SIGNING_KEY") == "" && os.Getenv("INNGEST_DEV") == "" {
		_ = os.Setenv("INNGEST_DEV", inngestAPI)
	}

	modelID := os.Getenv("MODEL")
	if modelID == "" {
		modelID = "claude-sonnet-4-5"
	}
	model := anthropic.Chat(modelID) // key auto-resolves from env

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "go-chat.db"
	}
	db, err := openStore(dbPath)
	if err != nil {
		log.Fatalf("open %s: %v", dbPath, err)
	}
	defer db.Close()

	assistant := buildAssistant()
	network := buildNetwork(assistant, model, db.historyConfig())

	// Point every SDK endpoint at this example's dev server explicitly.
	// Setting INNGEST_DEV to a URL is NOT enough — the SDK still resolves
	// its API/event origins from the defaults and the sync then fails with
	// "Expected server kind cloud, got dev".
	client, err := inngestgo.NewClient(inngestgo.ClientOpts{AppID: appID})
	if err != nil {
		log.Fatalf("inngest client: %v", err)
	}

	// Durable variant of the same turn, kept as the worked example of
	// agent-kit's Inngest integration: every inference and tool call is a
	// memoized step and every chunk publishes exactly once. It only runs
	// when the Inngest server will sync this app — i.e. `inngest dev`, not
	// the signed `inngest start` this example otherwise targets, which is
	// why POST /api/chat runs the network inline instead.
	_, err = inngestgo.CreateFunction(client,
		inngestgo.FunctionOpts{ID: "chat", Name: "go-chat turn"},
		inngestgo.EventTrigger(chatEvent, nil),
		func(ctx context.Context, in inngestgo.Input[chatRequest]) (any, error) {
			req := in.Event.Data
			log.Printf("[run] thread=%s msg=%q", req.ThreadID, truncate(req.Content, 40))

			state := agentkit.NewState(agentkit.StateConfig[chatState]{
				Data:     chatState{UserID: req.UserID},
				ThreadID: req.ThreadID,
			})
			durableStep := durable.Inngest()
			publish := agentkit.DurablePublish(durableStep,
				func(ctx context.Context, chunk agentkit.AgentMessageChunk) error {
					return publishChunk(ctx, req.ChannelKey, chunk)
				})
			if _, err := network.Run(ctx, req.Content, &agentkit.NetworkRunOptions[chatState]{
				State: state,
				UserMessage: &agentkit.UserMessage{
					ID: req.MessageID, Content: req.Content, Role: agentkit.RoleUser,
				},
				Step: durableStep,
				Streaming: &agentkit.StreamingConfig{
					Publish:          publish,
					StreamReasoning:  true,
					SimulateChunking: true,
					ChunkSize:        24,
				},
			}); err != nil {
				return nil, err
			}
			return map[string]any{"threadId": req.ThreadID}, nil
		},
	)
	if err != nil {
		log.Fatalf("create function: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/inngest", client.Serve())

	mux.HandleFunc("POST /api/chat", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		var req struct {
			UserMessage struct {
				ID      string `json:"id"`
				Content string `json:"content"`
			} `json:"userMessage"`
			ThreadID   string `json:"threadId"`
			UserID     string `json:"userId"`
			ChannelKey string `json:"channelKey"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserMessage.Content) == "" {
			http.Error(w, "userMessage.content required", http.StatusBadRequest)
			return
		}
		if req.ThreadID == "" {
			req.ThreadID = uuid.NewString()
		}
		if req.UserMessage.ID == "" {
			req.UserMessage.ID = uuid.NewString()
		}
		channel := firstNonEmpty(req.ChannelKey, req.UserID, req.ThreadID)

		// No manual bookkeeping: NetworkConfig.History (see store.go) makes
		// agent-kit create the thread, persist the user's turn, hydrate
		// prior context and save new results itself.
		go func() {
			ctx := context.Background()
			state := agentkit.NewState(agentkit.StateConfig[chatState]{
				Data:     chatState{UserID: req.UserID},
				ThreadID: req.ThreadID,
			})
			if _, err := network.Run(ctx, req.UserMessage.Content, &agentkit.NetworkRunOptions[chatState]{
				State: state,
				UserMessage: &agentkit.UserMessage{
					ID: req.UserMessage.ID, Content: req.UserMessage.Content, Role: agentkit.RoleUser,
				},
				Streaming: &agentkit.StreamingConfig{
					Publish: func(ctx context.Context, chunk agentkit.AgentMessageChunk) error {
						if err := publishChunk(ctx, channel, chunk); err != nil {
							log.Printf("  [publish] %s seq=%d ERR %v", chunk.Event, chunk.SequenceNumber, err)
							return err
						}
						return nil
					},
					StreamReasoning:  true,
					SimulateChunking: true,
					ChunkSize:        24,
				},
			}); err != nil {
				log.Printf("run failed: %v", err)
				_ = publishChunk(ctx, channel, agentkit.AgentMessageChunk{
					Event: agentkit.EventError,
					Data:  map[string]any{"error": err.Error(), "threadId": req.ThreadID},
				})
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "threadId": req.ThreadID})
	})

	// getRealtimeToken: mint a subscription JWT scoped to this channel +
	// the stream topic. Mirrors @inngest/realtime's api.getSubscriptionToken
	// (POST /v1/realtime/token with [{channel,name,kind}]).
	mux.HandleFunc("POST /api/realtime/token", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		var req struct {
			UserID     string `json:"userId"`
			ThreadID   string `json:"threadId"`
			ChannelKey string `json:"channelKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		channel := firstNonEmpty(req.ChannelKey, req.UserID, req.ThreadID)
		if channel == "" {
			http.Error(w, "channel required", http.StatusBadRequest)
			return
		}
		token, err := mintRealtimeToken(r.Context(), channel)
		if err != nil {
			log.Printf("token mint failed: %v", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		// Shape required by @inngest/realtime's TokenSubscription (which
		// use-agent hands this straight to): channel name, the topic list,
		// and the JWT under `key`. `token` is included for use-agent's own
		// RealtimeToken typing.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"channel": channel,
			"topics":  []string{streamTopic},
			"key":     token,
			"token":   token,
		})
	})

	// fetchThreads → ThreadsPage {threads: Thread[], hasMore, total}.
	mux.HandleFunc("GET /api/threads", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		threads, err := db.listThreads(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]map[string]any, 0, len(threads))
		for _, t := range threads {
			title := t.Title
			if title == "" {
				title = "New conversation"
			}
			out = append(out, map[string]any{
				"id": t.ID, "title": title,
				"messageCount": t.Messages,
				// Token totals for this thread. Not part of use-agent's
				// Thread type — the UI reads them off the same payload.
				"usage": map[string]int{
					"inputTokens":  t.Usage.InputTokens,
					"outputTokens": t.Usage.OutputTokens,
					"cacheRead":    t.Usage.CacheRead,
					"cacheWrite":   t.Usage.CacheWrite,
				},
				"lastMessageAt": t.UpdatedAt.UTC().Format(time.RFC3339),
				"createdAt":     t.CreatedAt.UTC().Format(time.RFC3339),
				"updatedAt":     t.UpdatedAt.UTC().Format(time.RFC3339),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"threads": out, "hasMore": false, "total": len(out),
		})
	})

	mux.HandleFunc("POST /api/threads", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		id := newID()
		if err := db.createThread(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"threadId": id, "title": ""})
	})

	// fetchHistory → rows for ThreadManager.formatRawHistoryMessages.
	mux.HandleFunc("GET /api/threads/{id}", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		rows, err := db.messages(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		history := make([]historyMessage, 0, len(rows))
		for _, m := range rows {
			h := historyMessage{MessageID: m.ID, CreatedAt: m.CreatedAt}
			if m.AgentName == userAgentName {
				h.Type, h.Content = "user", m.Content
			} else {
				h.Type = "assistant"
				h.Data = &struct {
					Output    []agentkit.Message `json:"output"`
					ToolCalls []agentkit.Message `json:"toolCalls,omitempty"`
				}{Output: m.Output, ToolCalls: m.ToolCalls}
			}
			history = append(history, h)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(history)
	})

	mux.HandleFunc("DELETE /api/threads/{id}", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		if err := db.deleteThread(r.Context(), r.PathValue("id")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	addr := ":8484"
	log.Printf("go-chat server on %s (model %s, durable via Inngest dev)", addr, modelID)
	log.Fatal(http.ListenAndServe(addr, withCORS(mux)))
}

// publishChunk sends one chunk to Inngest realtime over plain HTTP.
//
// inngestgo's realtime.Publish requires an Inngest function context
// (sdkrequest.Manager) and hardcodes the production URL. The wire call is
// just a POST, so we make it directly — which lets the network run inline
// and still stream to the browser.
func publishChunk(ctx context.Context, channel string, chunk agentkit.AgentMessageChunk) error {
	body, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	u := fmt.Sprintf("%s/v1/realtime/publish?channel=%s&topic=%s",
		inngestAPI, url.QueryEscape(channel), url.QueryEscape(streamTopic))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("INNGEST_SIGNING_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("realtime publish %d", res.StatusCode)
	}
	return nil
}

// mintRealtimeToken asks the Inngest server for a subscription JWT.
func mintRealtimeToken(ctx context.Context, channel string) (string, error) {
	body, _ := json.Marshal([]map[string]any{
		{"channel": channel, "name": streamTopic, "kind": "run"},
	})
	base := inngestAPI
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/realtime/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("INNGEST_SIGNING_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	// The dev server answers 201; production may answer 200.
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return "", fmt.Errorf("token endpoint %d: %s", res.StatusCode, truncate(string(raw), 200))
	}
	var out struct {
		JWT string `json:"jwt"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	return out.JWT, nil
}

// loadDotEnv reads KEY=VALUE lines so `go run .` works without exporting.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), `"'`)
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func cors(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
}

// withCORS answers preflights for every route. A mux-level "OPTIONS /api/"
// pattern is rejected by Go 1.22+ routing as conflicting with the more
// specific /api/inngest, so it lives in middleware instead.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			cors(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
