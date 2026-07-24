package agentkit

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/inngest/inngestgo"
)

// AgentKitEvent is the event payload for served agents and networks:
// {data: {input: "..."}}.
type AgentKitEvent struct {
	Input string `json:"input"`
}

// RegisterAgent exposes an agent as an Inngest function on the client,
// triggered by "<appID>/agent-<slug>" with event.data.input as the prompt.
func RegisterAgent[T any](client inngestgo.Client, agent *Agent[T]) (inngestgo.ServableFunction, error) {
	slug := Slugify(agent.Name)
	id := "agent-" + slug
	return inngestgo.CreateFunction(client,
		inngestgo.FunctionOpts{ID: id, Name: agent.Name},
		inngestgo.EventTrigger(client.AppID()+"/"+id, nil),
		func(ctx context.Context, input inngestgo.Input[AgentKitEvent]) (any, error) {
			return agent.Run(ctx, input.Event.Data.Input, nil)
		},
	)
}

// RegisterNetwork exposes a network as an Inngest function on the client,
// triggered by "<appID>/network-<slug>" with event.data.input as the prompt.
func RegisterNetwork[T any](client inngestgo.Client, network *Network[T]) (inngestgo.ServableFunction, error) {
	slug := Slugify(network.Name)
	id := "network-" + slug
	return inngestgo.CreateFunction(client,
		inngestgo.FunctionOpts{ID: id, Name: network.Name},
		inngestgo.EventTrigger(client.AppID()+"/"+id, nil),
		func(ctx context.Context, input inngestgo.Input[AgentKitEvent]) (any, error) {
			run, err := network.Run(ctx, input.Event.Data.Input, nil)
			if err != nil {
				return nil, err
			}
			return run.State.Results(), nil
		},
	)
}

// NewServer creates an Inngest client and an http.Handler serving it. The
// register callback creates functions on the client (RegisterAgent /
// RegisterNetwork / inngestgo.CreateFunction):
//
//	handler, err := agentkit.NewServer("my-app", func(c inngestgo.Client) error {
//		_, err := agentkit.RegisterNetwork(c, network)
//		return err
//	})
//	http.ListenAndServe(":3000", handler)
func NewServer(appID string, register func(client inngestgo.Client) error) (http.Handler, error) {
	if appID == "" {
		appID = "agent-kit"
	}
	client, err := inngestgo.NewClient(inngestgo.ClientOpts{AppID: appID})
	if err != nil {
		return nil, fmt.Errorf("agentkit: create inngest client: %w", err)
	}
	if register != nil {
		if err := register(client); err != nil {
			return nil, err
		}
	}
	return client.Serve(), nil
}

// Slugify mirrors the TS slugify used for function ids: lowercase, with
// runs of non-alphanumerics collapsed to single hyphens.
func Slugify(s string) string {
	var b strings.Builder
	lastHyphen := true // trim leading separators
	for _, r := range strings.ToLower(s) {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlnum {
			b.WriteRune(r)
			lastHyphen = false
		} else if !lastHyphen {
			b.WriteByte('-')
			lastHyphen = true
		}
	}
	return strings.TrimSuffix(b.String(), "-")
}
