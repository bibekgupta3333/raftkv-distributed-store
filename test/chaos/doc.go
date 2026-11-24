// Package chaos holds the kind + Chaos Mesh tests (D-47, docs/testing.md 2.6).
//
// manifests/ has one Chaos Mesh experiment per fault: PodChaos pod-kill (one
// pod, two pods) and NetworkChaos partition, delay and loss, all aimed at the
// raftkv namespace that `make kind-up` deploys. `make chaos-up` installs Chaos
// Mesh, `make chaos-apply FAULT=<name>` applies one manifest.
//
// The chaos driver (WBS 7.4.4, build tag chaos, `make chaos`) runs a seeded
// schedule of those manifests while the workload in client/ (an in-cluster
// pod) records a history, then checks the history with Porcupine. Rolling
// restarts of the StatefulSet (WBS 7.4.5) are steps of the same schedule; the
// leader handover check reads the members' logs (handover.go). See TestChaos
// and docs/testing.md 2.6.
package chaos
