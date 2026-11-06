// Command hello is the minimal Zeokeel Kernel plugin example.
//
// It registers one effect whose cleanup runs exactly once, in reverse
// registration order, when the fiber is disposed.
package main

import (
	"fmt"
	"os"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

// Greeter is a minimal synchronous plugin.
type Greeter struct{}

// Name is the plugin display name (also its default registry key).
func (Greeter) Name() string { return "greeter" }

// Activate runs as a transaction: every registration made through ctx is owned
// by the fiber and rolled back if Activate returns an error or panics.
func (Greeter) Activate(ctx cordis.Context, config cordis.Value) error {
	who := "world"
	if object, ok := config.(map[string]cordis.Value); ok {
		if name, ok := object["name"].(string); ok {
			who = name
		}
	}
	fmt.Printf("hello, %s\n", who)
	// The cleanup is owned by the fiber: it runs once when the fiber unloads.
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() {
			fmt.Printf("bye, %s\n", who)
		})}
	})
	return err
}

func main() {
	root := cordis.Root()

	config := map[string]cordis.Value{"name": "Cordis"}
	fiber, err := root.Plugin(Greeter{}, config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load greeter:", err)
		os.Exit(1)
	}
	fmt.Println("fiber state:", fiber.State())

	// Disposing the fiber runs the registered cleanups in reverse order.
	if err := fiber.Dispose(); err != nil {
		fmt.Fprintln(os.Stderr, "dispose greeter:", err)
		os.Exit(1)
	}
	fmt.Println("fiber state after dispose:", fiber.State())

	// The host shutdown path: dispose every fiber and the root fiber.
	if err := root.DisposeAll(); err != nil {
		fmt.Fprintln(os.Stderr, "shutdown:", err)
		os.Exit(1)
	}
	fmt.Println("registry size after shutdown:", root.Registry().Size())
}
