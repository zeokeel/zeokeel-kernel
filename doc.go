// Package cordis is an idiomatic Go implementation of the Cordis
// "spatiotemporal composability" kernel described in *A Programming Paradigm
// for Spatiotemporal Composability* (arXiv:2608.25512).
//
// The kernel owns exactly five things:
//
//   - contexts: scoped dependency containers with parent/child relations;
//   - services: typed values registered into an isolation scope, with optional
//     availability checks;
//   - fibers: one lifecycle unit per loaded plugin, owning effects and services;
//   - effects: registrations whose cleanup is owned by a fiber (listeners,
//     derived services, background tasks, and child fibers);
//   - events: synchronous and asynchronous publish/subscribe plus waterfall
//     middleware chains crossing plugin boundaries.
//
// # The two composability dimensions
//
// Time composability: every context mutation is paired with an inverse, the
// runtime keeps the inverses in registration order, and unloading a component
// runs them in reverse order exactly once, restoring the state the system had
// before the component was introduced.
//
// Space composability: a component declares its external dependencies (via
// Plugin.Inject), and the runtime classifies every context change against that
// declaration, activating and deactivating components automatically.
//
// Both dimensions are carried by a single Context tree, so unloading a
// component both undoes its effects and triggers re-evaluation of everything
// that declared a dependency on it.
//
// # Go-specific design notes
//
// The implementation keeps Cordis's observable contract and adapts the
// mechanisms to Go:
//
//   - Services are typed with generics (ServiceKey[T]) and Go 1.27 generic
//     methods: the type argument is inferred from the key, so typed access reads
//     as ctx.Provide(key, value), ctx.Get(key) and ctx.Require(key). Package-level
//     wrappers (cordis.Get, cordis.Provide, ...) remain for convenience. Interface
//     methods still cannot declare type parameters, so interfaces such as Plugin
//     stay non-generic; typed plugin identity uses Context.PluginTyped.
//   - Async is modeled with context.Context and goroutines instead of futures.
//     Async lifecycle entry points accept a context.Context; cancelling that
//     context is the Go analogue of cancelling an async operation.
//   - Panic isolation uses recover() at every user-code boundary. The kernel
//     never lets a plugin panic cross its boundary (the waterfall lane is the
//     one deliberate fail-closed exception).
//   - Ownership is GC-based. There is no deterministic destructor: callers must
//     call Fiber.Dispose/DisposeAsync or Context.DisposeAll. Fiber.Abandon is
//     the explicit Go analogue of releasing the last reference.
//
// The kernel is deliberately small and dependency-free (standard library
// only). Module loading, hot-update file watching, permissions, sandboxing,
// timers and UI composition belong to the embedder (the host application), not
// to this package.
package cordis
