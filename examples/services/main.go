// Command services demonstrates typed services, dependency injection and an
// availability gate.
//
// It mirrors upstream examples/services.rs: a plugin declares that it injects a
// service, so it stays Pending until the service becomes available; when the
// provider disappears the dependent unloads automatically.
package main

import (
	"fmt"
	"os"

	cordis "github.com/zeokeel/cordis-go"
)

// Clock is a service implemented here; *SystemClock satisfies it.
type Clock interface {
	Now() string
}

type wallClock struct{}

func (wallClock) Now() string { return "12:00" }

// ClockKey is the typed handle used to provide and read the service.
var ClockKey = cordis.NewServiceKey[Clock]("clock")

// Reporter injects the clock service.
type Reporter struct{}

func (Reporter) Name() string { return "reporter" }

func (Reporter) Inject() []string { return []string{ClockKey.Name()} }

func (Reporter) Activate(ctx cordis.Context, _ cordis.Value) error {
	clock, err := cordis.Require(ctx, ClockKey)
	if err != nil {
		return err
	}
	fmt.Println("reporter activated, clock says:", clock.Now())
	return nil
}

// Gated is a consumer of a service protected by a ServiceGate.
type GateConsumer struct{}

func (GateConsumer) Name() string { return "gate-consumer" }

func (GateConsumer) Inject() []string { return []string{GateKey.Name()} }

func (GateConsumer) Activate(cordis.Context, cordis.Value) error {
	fmt.Println("gate-consumer activated (service is available)")
	return nil
}

var GateKey = cordis.NewServiceKey[*int]("feature-flag")

func main() {
	root := cordis.Root()

	// A dependent loaded before its dependency stays Pending; providing the
	// service triggers a dependent refresh and activates it.
	reporter, err := root.Plugin(Reporter{}, nil)
	if err != nil {
		report(err)
	}
	fmt.Println("reporter state before provide:", reporter.State())

	// The value is passed as the interface type so the generic parameter is
	// inferred consistently from the ServiceKey.
	var impl Clock = wallClock{}
	if _, err := cordis.Provide(root, ClockKey, impl); err != nil {
		report(err)
	}
	fmt.Println("reporter state after provide:", reporter.State())

	// Read the service back with the typed lane.
	clock, ok := cordis.Get(root, ClockKey)
	fmt.Println("typed get:", clock.Now(), ok)

	// A ServiceGate re-evaluates dependents automatically when flipped.
	gate := cordis.NewServiceGate(false)
	flag := 1
	if _, err := cordis.ProvideGated(root, GateKey, &flag, gate); err != nil {
		report(err)
	}
	gated, err := root.Plugin(GateConsumer{}, nil)
	if err != nil {
		report(err)
	}
	fmt.Println("gate-consumer state while closed:", gated.State())

	gate.Set(true) // dependent refresh: gate-consumer activates
	fmt.Println("gate-consumer state after opening the gate:", gated.State())
	gate.Set(false) // dependent unloads back to Pending
	fmt.Println("gate-consumer state after closing the gate:", gated.State())

	// isolate() gives a subtree its own scope for one service name: the same
	// service name can be provided independently below the isolated child.
	isolated := root.Isolate(ClockKey.Name())
	var isolatedImpl Clock = wallClock{}
	if _, err := cordis.Provide(isolated, ClockKey, isolatedImpl); err != nil {
		report(err)
	}
	fmt.Println("isolated context has its own provisioned clock:", func() bool {
		_, ok := cordis.Get(isolated, ClockKey)
		return ok
	}())

	if err := root.DisposeAll(); err != nil {
		report(err)
	}
}

func report(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
