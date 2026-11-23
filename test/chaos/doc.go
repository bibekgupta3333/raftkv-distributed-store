// Package chaos holds the kind + Chaos Mesh tests (D-47, docs/testing.md 2.6).
//
// manifests/ has one Chaos Mesh experiment per fault: PodChaos pod-kill (one
// pod, two pods) and NetworkChaos partition, delay and loss, all aimed at the
// raftkv namespace that `make kind-up` deploys. `make chaos-up` installs Chaos
// Mesh, `make chaos-apply FAULT=<name>` applies one manifest. The driver that
// runs a fault schedule under a recorded workload and checks it with
// Porcupine (WBS 7.4.4) comes later.
package chaos
