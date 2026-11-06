package cordis

import (
	"context"
	"log/slog"
)

// Waterfall dispatch: middleware chains with an explicit Next continuation.
//
// Every middleware receives the dispatch arguments plus the continuation. It
// decides whether to continue the chain (call Next), rewrite the value on the
// way out (the middleware pattern), or veto the rest of the chain by returning
// without calling Next.
//
// Panic policy is fail-closed on both lanes: a panicking middleware aborts the
// chain (the terminal must not run, because that would turn a failed policy
// check into an allow) and the panic is re-raised to the dispatcher after being
// logged.

// WaterfallListenerFunc is a synchronous waterfall middleware.
type WaterfallListenerFunc func(args []Value, next *WaterfallNext) Value

// WaterfallTerminal is the built-in behavior run once the chain is exhausted.
type WaterfallTerminal func(args []Value) Value

// AsyncWaterfallListenerFunc is an asynchronous waterfall middleware.
type AsyncWaterfallListenerFunc func(ctx context.Context, args []Value, next *AsyncWaterfallNext) Value

// AsyncWaterfallTerminal is the built-in behavior of an async waterfall chain.
type AsyncWaterfallTerminal func(ctx context.Context, args []Value) Value

// WaterfallNext is the explicit continuation handed to every synchronous
// middleware.
type WaterfallNext struct {
	remaining []WaterfallListenerFunc
	args      []Value
	terminal  WaterfallTerminal
}

// Args returns the dispatch arguments (identical for every middleware).
func (n *WaterfallNext) Args() []Value { return n.args }

// Remaining returns how many middlewares are left after the current one.
func (n *WaterfallNext) Remaining() int { return len(n.remaining) }

// Next runs the rest of the chain, or the terminal when nothing is left.
//
// Calling it more than once re-runs the remaining chain, which is how
// retry/rewrite middlewares are expressed. A panicking middleware aborts the
// chain (fail-closed: remaining middleware and the terminal never run) and the
// panic is re-raised to the dispatcher.
func (n *WaterfallNext) Next() Value {
	if len(n.remaining) == 0 {
		return n.callTerminal()
	}
	listener, rest := n.remaining[0], n.remaining[1:]
	next := &WaterfallNext{remaining: rest, args: n.args, terminal: n.terminal}
	value, panicked := callValueContained(func() Value { return listener(n.args, next) })
	if panicked != nil {
		slog.Error("waterfall middleware panicked; aborting the chain and re-raising",
			"panic", panicText(panicked))
		panic(panicked)
	}
	return value
}

// Terminal skips the remaining middlewares and runs the built-in behavior.
func (n *WaterfallNext) Terminal() Value { return n.callTerminal() }

func (n *WaterfallNext) callTerminal() Value {
	if n.terminal == nil {
		return nil
	}
	return n.terminal(n.args)
}

// AsyncWaterfallNext is the explicit continuation handed to async middleware.
//
// It carries the caller's context.Context so cancellation propagates down the
// chain; middlewares may derive a child context with NextWith.
type AsyncWaterfallNext struct {
	ctx       context.Context
	remaining []AsyncWaterfallListenerFunc
	args      []Value
	terminal  AsyncWaterfallTerminal
}

// Args returns the dispatch arguments.
func (n *AsyncWaterfallNext) Args() []Value { return n.args }

// Remaining returns how many middlewares are left after the current one.
func (n *AsyncWaterfallNext) Remaining() int { return len(n.remaining) }

// Next runs and awaits the rest of the chain, or the terminal when nothing is
// left. Calling it more than once re-runs the remaining chain.
func (n *AsyncWaterfallNext) Next() Value { return n.NextWith(n.ctx) }

// NextWith is Next with an explicit derived context.
func (n *AsyncWaterfallNext) NextWith(ctx context.Context) Value {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(n.remaining) == 0 {
		return n.callTerminal(ctx)
	}
	listener, rest := n.remaining[0], n.remaining[1:]
	next := &AsyncWaterfallNext{ctx: ctx, remaining: rest, args: n.args, terminal: n.terminal}
	value, panicked := callValueContained(func() Value { return listener(ctx, n.args, next) })
	if panicked != nil {
		slog.Error("async waterfall middleware panicked; aborting the chain and re-raising",
			"panic", panicText(panicked))
		panic(panicked)
	}
	return value
}

// Terminal skips the remaining middlewares and runs the built-in behavior.
func (n *AsyncWaterfallNext) Terminal() Value { return n.TerminalWith(n.ctx) }

// TerminalWith is Terminal with an explicit derived context.
func (n *AsyncWaterfallNext) TerminalWith(ctx context.Context) Value {
	if ctx == nil {
		ctx = context.Background()
	}
	return n.callTerminal(ctx)
}

func (n *AsyncWaterfallNext) callTerminal(ctx context.Context) Value {
	if n.terminal == nil {
		return nil
	}
	return n.terminal(ctx, n.args)
}

// callValueContained runs fn and reports its panic value separately from its
// (possibly nil) result.
func callValueContained(fn func() Value) (value Value, panicValue any) {
	defer func() {
		if r := recover(); r != nil {
			panicValue = r
		}
	}()
	return fn(), nil
}

// waterfall helpers mirroring the Cordis constructors.

// WaterfallListener wraps fn as a WaterfallListenerFunc.
func WaterfallListener(fn WaterfallListenerFunc) WaterfallListenerFunc { return fn }

// WaterfallTerminalOf wraps fn as a WaterfallTerminal.
func WaterfallTerminalOf(fn func(args []Value) Value) WaterfallTerminal { return fn }

// NullWaterfallTerminal returns the default terminal: nil (JSON null).
func NullWaterfallTerminal() WaterfallTerminal { return func([]Value) Value { return nil } }

// NullAsyncWaterfallTerminal returns the default async terminal: nil.
func NullAsyncWaterfallTerminal() AsyncWaterfallTerminal {
	return func(context.Context, []Value) Value { return nil }
}
