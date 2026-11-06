// Command events demonstrates the event bus: emit/bail/once, waterfall
// middleware (including veto), and the asynchronous lane.
//
// It mirrors upstream examples/events.rs.
package main

import (
	"context"
	"fmt"

	cordis "github.com/zeokeel/cordis-go"
)

func main() {
	root := cordis.Root()

	// --- sync lane: emit + once -------------------------------------------
	if _, err := root.On("greet", func(args []cordis.Value) cordis.EventResult {
		fmt.Println("listener A received greet with", args)
		return cordis.Continue()
	}); err != nil {
		panic(err)
	}
	if _, err := root.Once("greet", func(args []cordis.Value) cordis.EventResult {
		fmt.Println("listener B (once) bails with 'stop'")
		return cordis.Bail("stop")
	}); err != nil {
		panic(err)
	}
	root.Emit("greet", "world")
	fmt.Println("emit never propagates bail results; bail() does:")

	// --- bail lane: first non-null, non-false value stops the chain -------
	if _, err := root.On("check", func([]cordis.Value) cordis.EventResult {
		fmt.Println("  check listener 1: continue")
		return cordis.Continue()
	}); err != nil {
		panic(err)
	}
	if _, err := root.On("check", func([]cordis.Value) cordis.EventResult {
		fmt.Println("  check listener 2: bail 42")
		return cordis.Bail(42)
	}); err != nil {
		panic(err)
	}
	if _, err := root.On("check", func([]cordis.Value) cordis.EventResult {
		fmt.Println("  check listener 3: must not run")
		return cordis.Continue()
	}); err != nil {
		panic(err)
	}
	if value, ok := root.Bail("check", nil); ok {
		fmt.Println("  bail result:", value)
	}

	// --- waterfall: middleware around an explicit continuation ------------
	if _, err := root.OnWaterfall("request", func(args []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		fmt.Println("  waterfall outer middleware, remaining =", next.Remaining())
		return fmt.Sprintf("outer(%v)", next.Next())
	}); err != nil {
		panic(err)
	}
	if _, err := root.OnWaterfall("request", func(args []cordis.Value, next *cordis.WaterfallNext) cordis.Value {
		fmt.Println("  waterfall inner middleware")
		return fmt.Sprintf("inner(%v)", next.Next())
	}); err != nil {
		panic(err)
	}
	value := root.Waterfall("request", nil, func([]cordis.Value) cordis.Value { return "base" })
	fmt.Println("  waterfall result:", value)

	// A middleware that never calls Next vetoes the rest of the chain.
	vetoRoot := root.Isolate("veto-demo")
	if _, err := vetoRoot.OnWaterfall("danger", func([]cordis.Value, *cordis.WaterfallNext) cordis.Value {
		return "vetoed"
	}); err != nil {
		panic(err)
	}
	fmt.Println("  veto result:", vetoRoot.Waterfall("danger", nil, func([]cordis.Value) cordis.Value {
		fmt.Println("  terminal must not run")
		return "terminal"
	}))

	// --- async lane: parallel dispatch of blocking listeners --------------
	if _, err := root.OnAsync("async", func(ctx context.Context, args []cordis.Value) cordis.EventResult {
		fmt.Println("  async listener 1 ran with", args)
		return cordis.Continue()
	}); err != nil {
		panic(err)
	}
	if _, err := root.OnAsync("async", func(ctx context.Context, args []cordis.Value) cordis.EventResult {
		fmt.Println("  async listener 2 ran with", args)
		return cordis.Continue()
	}); err != nil {
		panic(err)
	}
	root.ParallelAsyncContext(context.Background(), "async", "payload")

	if err := root.DisposeAll(); err != nil {
		panic(err)
	}
}
