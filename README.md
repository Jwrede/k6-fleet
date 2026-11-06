# k6-fleet

Runs one [k6](https://github.com/grafana/k6) test across several machines and treats it as one test. Thresholds, `abortOnFail`, checks and the summary are computed on the samples of all machines. It needs ssh or Docker on the load generators, and no Kubernetes.

```
k6-fleet run --ssh lg1,lg2,lg3 --summary-export summary.json --report report.html checkout.js -- -e BASE=https://staging.example.com
```

The exit code follows k6, so a CI job fails on exit 99 when a threshold is crossed on the merged data.

## Why

[grafana/k6 #140](https://github.com/grafana/k6/issues/140), "Clustering and distributed execution", has been open since March 2017 and has 103 thumbs up. k6 itself can split a test with execution segments, but the maintainers describe what is then missing. In January 2021: "you can launch distributed tests with the minimal amount of synchronization work described in that comment, though, unfortunately, without thresholds and a centralized summary", and of the Kubernetes operator: "again with the lack of thresholds and summary". The two proofs of concept for native support, [#2438](https://github.com/grafana/k6/pull/2438) and [#2816](https://github.com/grafana/k6/pull/2816), were closed unmerged, the second in May 2026. The issue carries the stale label since October 2026.

The options today are these:

| Option | Needs | Thresholds on merged data |
|---|---|---|
| [k6-operator](https://github.com/grafana/k6-operator) | Kubernetes | A code search finds threshold handling only in its Grafana Cloud path (`pkg/cloud`) |
| Grafana Cloud k6 | Paid service | Yes |
| [orderly-ape](https://github.com/ReviewSignal/orderly-ape) | Several Kubernetes clusters and a web app | Results go to InfluxDB and Grafana |
| [distributed-load-testing-on-aws](https://github.com/aws-solutions/distributed-load-testing-on-aws) | An AWS deployment | Own reporting |
| Starting k6 by hand with `--execution-segment` | Own scripts for start, setup and aggregation | No |

Without merged data a threshold can give the wrong answer. If one of three generators gets all the slow requests, the other two pass `p(95)<300` on their own samples, and the merged p(95) does not.

## How it works

```
                        k6 archive, k6 inspect
 controller  ---------------------------------------------.
   |  stdio over ssh, docker or a local process            |
   +---- agent ---- k6 --paused --execution-segment 0:1/3  |
   +---- agent ---- k6 --paused --execution-segment 1/3:2/3|
   +---- agent ---- k6 --paused --execution-segment 2/3:1  |
```

1. The controller runs `k6 archive`, so the script, its imports and files from `open()` travel as one file, and reads thresholds and the duration with `k6 inspect`.
2. It starts an agent per machine. The agent is the same static binary, uploaded over ssh or copied into a container, and talks JSON lines on stdin and stdout. No port is opened on a load generator.
3. Each agent starts k6 with `--paused --linger --no-setup --no-teardown --no-thresholds` and a segment from `--execution-segment-sequence`. Weights in the inventory set the size of a segment.
4. `setup()` runs once through k6's REST API on the first agent. Its return value is sent to the others with `PUT /v1/setup`.
5. The controller measures each agent's clock offset and gives all of them a common start time, so a slow link does not delay one of them.
6. Agents read k6's sample stream from a FIFO and send one frame per second: counters, rates and a mergeable quantile sketch per Trend series and per submetric that a threshold uses. Raw samples stay on the load generator.
7. The controller merges the frames, applies `abortOnFail` with `delayAbortEval` every two seconds, runs `teardown()` once, evaluates all thresholds and writes the summary.

Beyond merging, the controller watches the agents. It warns when a generator stays above 90 percent CPU for 5 seconds, because latencies then describe the generator, and when an agent falls behind on samples. Losing an agent stops the run with exit 3, or continues with `--on-agent-loss continue` and a warning that the totals are too low. If `exec.test.abort()` or a crash ends one instance, the others stop too. k6 cannot outlive its agent.

## Install

```
go install github.com/Jwrede/k6-fleet/cmd/k6-fleet@latest
```

Use `CGO_ENABLED=0` for a static binary if you use the docker transport with the `grafana/k6` image. The [releases](https://github.com/Jwrede/k6-fleet/releases) have static binaries for linux amd64 and arm64.

k6 has to be installed on the controller and on every agent, in the same version; a mismatch is refused. Tested with k6 v2.3.0 and v1.8.2.

## Usage

```
k6-fleet run [flags] script.js [-- k6 archive flags]

  --ssh a,b,c           one agent per ssh host
  --local N             N agents on this machine
  --inventory file      JSON agent list, see examples/fleet.json
  --summary-export f    merged summary as JSON
  --report f            self-contained HTML report
  --listen addr         live merged metrics at /metrics, Prometheus format
  --out uri             extra k6 --out on every agent (repeatable)
  --on-agent-loss       abort (default) or continue
  --ready-timeout d     time for agents to initialise (default 2m)
  --max-duration d      stop after this long (default: script duration + 2m)
  --k6 path             k6 binary on the controller
  --quiet               only warnings and the summary
```

Flags after `--` go to `k6 archive` and so reach every agent: `-- --vus 300 --duration 5m -e BASE=https://example.com`.

An inventory sets per-agent options. `weight` gives an agent a larger segment, and `binary` names the file to upload to agents of another platform.

```json
{
  "agents": [
    { "name": "lg1", "transport": "ssh", "host": "ubuntu@10.0.0.11", "weight": 2 },
    { "name": "lg2", "transport": "ssh", "host": "ubuntu@10.0.0.12", "ssh_args": ["-i", "/home/me/.ssh/loadtest"] },
    { "name": "box", "transport": "docker", "image": "grafana/k6:2.3.0", "network": "host" }
  ]
}
```

The docker transport uses `docker create`, `cp` and `start`, so it also works with `DOCKER_HOST` or a docker context.

Exit codes are those of k6: 0, 99 for thresholds, 105 for an interrupt, 107 for a script error, 108 for `exec.test.abort()`. In addition: 1 for an orchestration error and 3 for a lost agent.

## Example

Output from CI, real k6 v2.3.0, three agents, `e2e/scripts/shared.js`. The script runs 1000 shared iterations and adds the iteration number to a Trend, so the exact results are known: average 499.5, p(95) 949.05.

```
  k6-fleet summary: shared.js, 3 agents, k6 v2.3.0, 200ms

  THRESHOLDS (evaluated on merged data)
    ok   checks "rate==1"  actual=1
    ok   deterministic "avg==499.5"  actual=499.5
    ok   deterministic "max==999"  actual=999
    ok   deterministic "min==0"  actual=0
    ok   deterministic "p(95)<960"  actual=948.7
    ok   deterministic "p(95)>940"  actual=948.7
    ok   http_req_duration{name:hit} "max<5000"  actual=12.27ms

  CHECKS
    status is 200                            1000 passed, 0 failed
    ::g body is ok                           1000 passed, 0 failed

  METRICS
    checks                       100.00%  2000 out of 2000
    deterministic                avg=499.5 min=0 med=500.2 max=999 p(90)=902.4 p(95)=948.7 p(99)=987.4
    http_req_duration            avg=655us min=59us med=417us max=12.27ms p(90)=1.33ms p(95)=2.09ms p(99)=3.80ms
    http_req_duration{name:hit}  avg=656us min=59us med=417us max=12.27ms p(90)=1.33ms p(95)=2.09ms p(99)=3.96ms
    http_req_failed              0.00%  0 out of 1002
    http_reqs                    1002  5097.20/s
    iterations                   1000  5087.03/s
    ...

  AGENTS
    name             segment    iterations   requests  cpu avg  cpu max  lag max   exit
    local-0          0:1/3             334        336       6%       6%    218ms      0
    local-1          1/3:2/3           333        333       7%       7%    203ms      0
    local-2          2/3:1             333        333       6%       6%    210ms      0
```

## Testing

`go test ./...` runs the unit tests and the orchestration scenarios against `internal/fakek6`, a stub of the k6 CLI, REST API and JSON output. It needs no k6.

`K6FLEET_E2E=1 go test ./e2e/` runs against real k6 and a real HTTP target that records what actually arrived. CI does this with k6 v2.3.0 and v1.8.2, plus the docker transport with the `grafana/k6` image and the ssh transport against sshd on the runner, and repeats it weekly. The tests check:

- 1000 shared iterations arrive exactly once each across three agents, `setup()` and `teardown()` run once, and the setup data reaches every VU.
- Counts and exact statistics equal a single `k6 run` of the same script, and p(90) and p(95) are within 0.5 percent of it.
- A `constant-arrival-rate` of 60/s for 6 s gives about 360 iterations in total, and ramping VUs add up across agents.
- Exit 99 on a crossed threshold, as with plain k6, `abortOnFail` stopping all agents, and `exec.test.abort()` on one instance ending the run with 108.
- Relative imports, `open()`, and CLI options after `--` reach the agents.
- `kill -9` on an agent ends the run with exit 3 and removes its k6.

## Limits

- All tests ran on one machine per CI job. The ssh and docker transports were exercised on that machine; a fleet of separate hosts over a real network has not been tested.
- Sample throughput costs something. k6 serialises every sample to JSON for the agent, and that dominates on k6 2.x. In one CI job with k6, the target and the agent sharing a 4-vCPU runner, k6-fleet with one agent reached 48 percent of plain k6's requests per second on v2.3.0 and 87 percent on v1.8.2. In the same jobs `k6 --out json=/dev/null` alone reached 57 and 94 percent. Runner speed varied by more than a factor of two between runs, so only ratios within a job are meaningful. Plan for more agents than plain k6 would need.
- `handleSummary()` is not called, and `--summary-export` writes the k6-fleet JSON format, whose `metrics` section has the shape of the `handleSummary` data.
- Percentiles have a relative error of at most 0.5 percent, so a threshold within that distance of its limit can go either way. Counts, rates, min, max and avg are exact.
- A custom Gauge has no meaningful sum, so the newest sample across agents counts, compared by agent clocks. `vus` and `vus_max` are summed.
- k6 metrics pass through the agent's FIFO, so the agent is Linux-only. The controller also runs on macOS only as far as `go build` allows, which is untested.
- Environment variables such as `K6_*` are not forwarded to agents. Use `-e` and options after `--`, which end up in the archive.
- The start is synchronised to the measured clock offset. Its precision depends on the round trip of the control connection.

## License

MIT
