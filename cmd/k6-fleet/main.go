// k6-fleet runs one k6 test across several machines and treats it as one test:
// thresholds, checks and the summary are computed over all samples.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Jwrede/k6-fleet/internal/agent"
	"github.com/Jwrede/k6-fleet/internal/controller"
	"github.com/Jwrede/k6-fleet/internal/transport"
)

var version = "0.2.0"

const usage = `k6-fleet runs one k6 test across several machines.

  k6-fleet run [flags] script.js [-- k6 archive flags]
  k6-fleet agent [--k6 PATH]       (started by the controller)
  k6-fleet version

Flags after -- go to k6 archive, for example: -- --vus 300 --duration 5m -e BASE=http://x
`

type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, ",") }
func (r *repeated) Set(s string) error { *r = append(*r, s); return nil }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		os.Exit(run(os.Args[2:]))
	case "agent":
		fs := flag.NewFlagSet("agent", flag.ExitOnError)
		k6 := fs.String("k6", "k6", "k6 binary used for the version report")
		_ = fs.Parse(os.Args[2:])
		if err := agent.Run(os.Stdin, os.Stdout, version, *k6); err != nil {
			fmt.Fprintln(os.Stderr, "agent:", err)
			os.Exit(1)
		}
	case "version", "--version":
		fmt.Println(version)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}

func run(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usage+"\nFlags:\n")
		fs.PrintDefaults()
	}
	var o controller.Options
	var out repeated
	inventory := fs.String("inventory", "", "JSON file with the agents")
	local := fs.Int("local", 0, "run N agents on this machine")
	ssh := fs.String("ssh", "", "comma-separated ssh hosts, one agent each")
	fs.StringVar(&o.K6, "k6", "k6", "k6 binary on the controller")
	fs.StringVar(&o.Summary, "summary-export", "", "write the merged summary as JSON")
	fs.StringVar(&o.HTML, "report", "", "write a self-contained HTML report")
	fs.StringVar(&o.Listen, "listen", "", "serve live merged metrics at this address under /metrics")
	fs.StringVar(&o.Web, "web", "", "serve the web page at this address, a bare :port means localhost")
	fs.BoolVar(&o.Autostart, "autostart", false, "with --web: run once without waiting for the page and exit afterwards")
	fs.StringVar(&o.OnLoss, "on-agent-loss", "abort", "abort or continue when an agent disappears")
	fs.DurationVar(&o.Ready, "ready-timeout", 2*time.Minute, "how long agents may take to initialise")
	fs.DurationVar(&o.MaxRun, "max-duration", 0, "stop the test after this long (default: script duration + 2m)")
	fs.BoolVar(&o.Quiet, "quiet", false, "print only warnings and the summary")
	fs.Var(&out, "out", "extra k6 --out for every agent (repeatable)")

	rest, passthrough := splitArgs(args)
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	o.Script, o.K6Args, o.AgentOut = fs.Arg(0), passthrough, out
	if o.OnLoss != "abort" && o.OnLoss != "continue" {
		fmt.Fprintln(os.Stderr, "--on-agent-loss must be abort or continue")
		return 2
	}
	if o.Autostart && o.Web == "" {
		fmt.Fprintln(os.Stderr, "--autostart needs --web")
		return 2
	}
	o.WebToken = os.Getenv("K6FLEET_WEB_TOKEN")
	agents, err := loadAgents(*inventory, *local, *ssh, o.K6)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	o.Agents = agents
	if o.Self, err = os.Executable(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, err := controller.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "k6-fleet:", err)
	}
	return code
}

// splitArgs separates the flags after "--". The flag package would otherwise
// stop there and report them as extra positional arguments.
func splitArgs(args []string) (own, passthrough []string) {
	for i, a := range args {
		if a == "--" {
			return args[:i], args[i+1:]
		}
	}
	return args, nil
}

func loadAgents(inventory string, local int, ssh, k6 string) ([]transport.Spec, error) {
	var agents []transport.Spec
	if inventory != "" {
		data, err := os.ReadFile(inventory)
		if err != nil {
			return nil, err
		}
		if agents, err = controller.ParseInventory(data); err != nil {
			return nil, err
		}
	}
	for i := 0; i < local; i++ {
		agents = append(agents, transport.Spec{Name: "local-" + strconv.Itoa(i), Transport: "local", K6: k6})
	}
	if ssh != "" {
		for _, h := range strings.Split(ssh, ",") {
			agents = append(agents, transport.Spec{Name: h, Transport: "ssh", Host: h})
		}
	}
	if len(agents) == 0 {
		return nil, errors.New("no agents: use --inventory, --local or --ssh")
	}
	return agents, nil
}
