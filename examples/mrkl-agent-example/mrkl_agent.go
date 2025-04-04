package main

import (
	"context"
	"fmt"
	"os"

	"github.com/averikitsch/langchaingo/agents"
	"github.com/averikitsch/langchaingo/chains"
	"github.com/averikitsch/langchaingo/llms/openai"
	"github.com/averikitsch/langchaingo/tools"
	"github.com/averikitsch/langchaingo/tools/serpapi"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	llm, err := openai.New()
	if err != nil {
		return err
	}
	search, err := serpapi.New()
	if err != nil {
		return err
	}
	agentTools := []tools.Tool{
		tools.Calculator{},
		search,
	}

	agent := agents.NewOneShotAgent(llm,
		agentTools,
		agents.WithMaxIterations(3))
	executor := agents.NewExecutor(agent)

	question := "Who is Olivia Wilde's boyfriend? What is his current age raised to the 0.23 power?"
	answer, err := chains.Run(context.Background(), executor, question)
	fmt.Println(answer)
	return err
}
