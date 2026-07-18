# KEP-NNNN: Partial Preemption of Elastic Workloads

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1: Preserve the Ray head node](#story-1-preserve-the-ray-head-node)
    - [Story 2: Reclaim part of a large elastic training job](#story-2-reclaim-part-of-a-large-elastic-training-job)
    - [Story 3: Automatic re-expansion](#story-3-automatic-re-expansion)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API Changes](#api-changes)
    - [Job-level opt-in: per-PodSet preemption floor](#job-level-opt-in-per-podset-preemption-floor)
    - [Workload API](#workload-api)
    - [ClusterQueue API](#clusterqueue-api)
  - [Preemptor Changes](#preemptor-changes)
    - [Target selection](#target-selection)
    - [Issuing a shrink](#issuing-a-shrink)
  - [Job Framework Changes](#job-framework-changes)
  - [Quota Accounting](#quota-accounting)
  - [Enforcement and Fallback](#enforcement-and-fallback)
  - [Interaction with Fair Sharing](#interaction-with-fair-sharing)
  - [Observability](#observability)
  - [Test Plan](#test-plan)
    - [Prerequisite testing updates](#prerequisite-testing-updates)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
    - [e2e tests](#e2e-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Reuse partial admission (KEP-420) via evict-and-readmit](#reuse-partial-admission-kep-420-via-evict-and-readmit)
  - [External policy controller](#external-policy-controller)
  - [Split the job into multiple Workloads](#split-the-job-into-multiple-workloads)
  - [Pod-level preemption by Kueue](#pod-level-preemption-by-kueue)
<!-- /toc -->

## Summary

Today, preemption in Kueue is all-or-nothing: when a pending Workload needs
quota held by a lower-priority admitted Workload, the victim is evicted in its
entirety, even if reclaiming only a fraction of its resources would be enough.

This KEP proposes **partial preemption** for elastic workloads (workloads
admitted with the `kueue.x-k8s.io/elastic-job` annotation under the
`ElasticJobsViaWorkloadSlices` feature gate, KEP-77). Instead of evicting the
whole victim, the preemptor may **shrink** selected PodSets of the victim down
to owner-declared per-PodSet floors, releasing just enough quota to admit the
pending Workload while the retained pods keep running uninterrupted.

The shrink is executed through the existing workload-slice scale-down
machinery: Kueue patches the job's replica counts through the integration
layer, the job's own controller removes the excess pods, and the Workload
slice's PodSet counts are updated, releasing quota.

## Motivation

Elastic workloads such as Ray clusters often mix PodSets of very different
value: a head node holds cluster state (driver process, object store metadata,
job history) and is cheap to run, while worker groups hold expensive
accelerators and are, by design, disposable and re-creatable.

When such a workload is preempted today, everything is killed. For a Ray
cluster this means the head node dies, and with it all in-memory state of a
possibly long-running experiment — even though the preemptor only needed the
GPUs held by the workers. Frameworks like Ray are explicitly built to tolerate
losing workers and rescaling; Kueue currently cannot take advantage of that.

KEP-77 (dynamically sized jobs) already gives Kueue everything needed to
account for a *user-initiated* resize of a running workload without restarting
it. This KEP extends the same mechanics to *Kueue-initiated* resizes performed
by the preemptor, and explicitly revisits the KEP-77 non-goal of "partial
preemption of the workload slices".

### Goals

- Allow the preemptor to reclaim quota from an admitted elastic Workload by
  shrinking selected PodSets instead of evicting the whole Workload.
- Let workload owners declare, per PodSet, the minimum count that must survive
  preemption (e.g. `head` must keep 1 replica; `workers` may go to 0).
- Reuse the KEP-77 workload-slice scale-down path so retained pods are never
  restarted and quota accounting stays consistent.
- Respect all existing preemption semantics: `withinClusterQueue`,
  `reclaimWithinCohort`, `borrowWithinCohort`, priority ordering, and fair
  sharing. A Workload may only be shrunk in situations where it could today be
  evicted.
- Guarantee forward progress: if a shrink is not honored in time, fall back to
  full eviction.

### Non-Goals

- Selecting *which pods* inside a shrunk PodSet are terminated. This remains
  the job controller's decision (e.g. KubeRay's scale-down logic and
  `scaleStrategy.workersToDelete`).
- Vertical shrinking (reducing per-pod resource requests).
- Partial preemption of non-elastic workloads (those not opted into
  workload slices).
- Changing which Workloads are *eligible* as preemption victims. Partial
  preemption only changes *how much* of an eligible victim is reclaimed.
- Automatically growing a shrunk workload back. Re-expansion happens through
  the normal KEP-77 scale-up flow, driven by the workload owner or an
  autoscaler (see Story 3).

## Proposal

1. Workload owners annotate their elastic job with per-PodSet preemption
   floors. Integrations propagate these into the Workload's PodSets as a new
   `preemptionMinCount` field.
2. A new opt-in field on `ClusterQueue.spec.preemption` enables partial
   preemption for victims admitted by that queue.
3. During preemption, after computing the set of full-eviction targets exactly
   as today, the preemptor runs a *shrink refinement* pass: for each target
   that is an elastic Workload with shrinkable PodSets, it checks whether
   shrinking (down to the floors) instead of evicting still releases enough of
   the resources the pending Workload needs. If so, the target becomes a
   shrink target.
4. Issuing a shrink target patches the victim job's replica counts through a
   new optional job-framework interface. The job's controller terminates the
   excess pods; the existing workload-slice scale-down path updates the
   Workload slice's counts and releases quota; the pending Workload is
   admitted in a subsequent scheduling cycle, exactly like after an eviction.
5. If the job does not converge to the requested size within a timeout, the
   preemptor falls back to evicting the victim entirely.

### User Stories

#### Story 1: Preserve the Ray head node

A researcher runs a long experiment on a RayCluster with a `head` PodSet
(1 replica, CPU-only) and a `workers` PodSet (4 replicas, 1 GPU each), admitted
to a research ClusterQueue. The cluster admin has enabled partial preemption on
the queue, and the RayCluster is annotated:

```yaml
metadata:
  annotations:
    kueue.x-k8s.io/elastic-job: "true"
    kueue.x-k8s.io/preemption-min-counts: '{"head": 1, "workers": 0}'
```

A high-priority production training job is submitted and needs 4 GPUs. Today,
the whole RayCluster would be evicted. With this KEP, the preemptor shrinks
`workers` from 4 to 0 and leaves `head` untouched. The head node keeps running,
the experiment state survives, and the production job gets the GPUs.

#### Story 2: Reclaim part of a large elastic training job

An elastic training job runs with `workers: 16`, floors declared as
`{"workers": 4}`. A pending workload needs quota equivalent to 10 workers.
Instead of evicting the whole job, the preemptor shrinks it to `workers: 6`
(the smallest count that releases enough quota, not below the floor of 4).
The training framework rebalances onto the remaining 6 workers.

#### Story 3: Automatic re-expansion

After the production job from Story 1 finishes, the Ray autoscaler (or the
user) scales `workers` back up. This is a plain KEP-77 scale-up: a new
workload slice is created, gets admitted now that quota is free, and the new
worker pods are ungated. No new API is needed.

### Notes/Constraints/Caveats

- **The job object remains the source of truth for counts.** In KEP-77, the
  job → Workload direction drives PodSet counts. Kueue therefore must not
  shrink the Workload slice directly (the job reconciler would sync it back
  up). Instead, the preemptor asks the *integration* to patch the job object,
  and the slice follows through the existing `EnsureWorkloadSlices` scale-down
  path.
- **A shrink is asynchronous**, like an eviction. The pending Workload is not
  admitted in the same scheduling cycle; it waits until the victim's usage
  actually drops. This matches today's evict-then-admit flow.
- **PodSets without a declared floor are not shrinkable.** Absent opt-in, a
  PodSet behaves as today: it can only be reclaimed by evicting the whole
  Workload.
- Whether the *shrunk remainder* is still useful is the owner's responsibility,
  expressed through the floors — the same trust model as partial admission
  (KEP-420).

### Risks and Mitigations

- **The job controller ignores or delays the shrink** (buggy or adversarial
  integration, stuck pods). Mitigation: a per-shrink timeout after which the
  preemptor falls back to full eviction of the victim, preserving today's
  forward-progress guarantee.
- **Thrashing**: a shrunk workload immediately scales back up, gets admitted,
  then is shrunk again. Mitigation: re-expansion is a new slice that competes
  through normal admission (it simply stays pending while quota is scarce), so
  the thrash loop requires quota to actually free up in between; additionally
  the scale-up slice of a recently shrunk workload is subject to the standard
  requeueing backoff.
- **Under-estimation of released quota**: shrinking releases specific
  flavor/resource amounts that may not match what the pending Workload needs
  (e.g. it frees GPUs but the pending Workload also needs CPU held by a
  non-shrinkable PodSet). Mitigation: the shrink refinement pass operates on
  the same snapshot simulation as regular preemption and only downgrades an
  eviction to a shrink when the simulation still admits the pending Workload.
- **Increased preemptor complexity**. Mitigation: partial preemption is a
  refinement pass over the output of the existing algorithms
  (`classicalPreemptions`, `fairPreemptions`), not a change to candidate
  selection or ordering; with the feature gate off, the code path is inert.

## Design Details

### API Changes

#### Job-level opt-in: per-PodSet preemption floor

A new annotation, readable by all integrations that support workload slices:

```yaml
kueue.x-k8s.io/preemption-min-counts: '{"<podset-name>": <min-count>, ...}'
```

- Only valid together with `kueue.x-k8s.io/elastic-job: "true"`.
- PodSets not listed are not shrinkable.
- `0` is a valid floor (the PodSet may be shrunk away entirely, while the
  Workload as a whole stays admitted).

Job CRDs that grow first-class support later can map their own fields to the
same Workload field; the annotation is the lowest-common-denominator interface,
mirroring how `kueue.x-k8s.io/job-min-parallelism` works for partial admission.

#### Workload API

```go
type PodSet struct {
    ...
    // preemptionMinCount is the minimum number of replicas of this PodSet
    // that must be retained if Kueue partially preempts the workload by
    // shrinking it. If nil, this PodSet cannot be shrunk by preemption.
    //
    // Only allowed on workloads enabled for workload slices.
    // preemptionMinCount must be <= count.
    //
    // +optional
    PreemptionMinCount *int32 `json:"preemptionMinCount,omitempty"`
}
```

This is deliberately symmetric with KEP-420's `minCount` (admission floor);
`preemptionMinCount` is the *runtime* floor.

To record an in-flight shrink request on the victim, a new condition and
annotation on the Workload:

- Condition `ShrinkRequested` (`status: True`, reason `PartialPreemption`,
  message naming the preemptor Workload), set by the preemptor.
- Annotation `kueue.x-k8s.io/shrink-target-counts: '{"workers": 0}'` holding
  the requested counts (conditions cannot carry structured data). Cleared when
  the slice's counts reach the target or the fallback eviction fires.

#### ClusterQueue API

```go
type ClusterQueuePreemption struct {
    ...
    // partial configures whether preemption may shrink elastic workloads
    // instead of evicting them.
    // +optional
    Partial *PartialPreemption `json:"partial,omitempty"`
}

type PartialPreemption struct {
    // policy determines when the preemptor may shrink an eligible elastic
    // victim instead of evicting it:
    //
    // - Never (default): always evict whole workloads.
    // - Preferred: shrink instead of evict whenever shrinking releases
    //   enough resources for the preemptor workload.
    //
    // +kubebuilder:validation:Enum=Never;Preferred
    Policy PartialPreemptionPolicy `json:"policy,omitempty"`

    // timeout after which a requested shrink that has not been fulfilled
    // falls back to full eviction of the victim.
    // Defaults to 60s.
    // +optional
    Timeout *metav1.Duration `json:"timeout,omitempty"`
}
```

The policy applies to the ClusterQueue that *admitted the victim* — the queue
owner decides how their workloads may be reclaimed, consistent with how
`withinClusterQueue` is interpreted.

### Preemptor Changes

#### Target selection

`Preemptor.GetTargets` computes targets as today. A new refinement pass then
runs over the resulting `[]*Target`:

1. For each target whose Workload is slice-enabled, has at least one PodSet
   with `preemptionMinCount`, and whose ClusterQueue has `partial.policy:
   Preferred`, compute the *minimal shrink*: the smallest per-PodSet reduction
   (never below the floors, removing whole replicas of the highest-yield
   shrinkable PodSets first) whose released resources, substituted for the
   target's full usage in the snapshot, still lets the pending Workload's
   flavor assignment fit.
2. If such a shrink exists, the target is marked as a shrink target with the
   desired counts (`Target.ShrinkTo workload.PodSetsCounts`); otherwise it
   stays a full-eviction target.

Because the pass only ever *reduces* the amount reclaimed from already-chosen
victims and re-validates fit in the simulation, it cannot admit fewer pending
workloads than today's algorithm — it can only leave victims less damaged.

#### Issuing a shrink

`Preemptor.IssuePreemptions` handles shrink targets by, instead of calling
eviction:

1. Setting the `kueue.x-k8s.io/shrink-target-counts` annotation and the
   `ShrinkRequested` condition on the victim Workload slice.
2. Emitting a `ShrunkDueToPreemption` event on the victim Workload and job,
   symmetric to today's `Preempted` event, including the preemptor's name and
   the requested counts.

The preemptor does not touch the job object directly; that is the job
framework's responsibility (below), keeping the scheduler free of
integration-specific logic.

### Job Framework Changes

A new optional interface in `pkg/controller/jobframework`:

```go
// JobWithPodSetShrink is implemented by integrations whose job object can be
// scaled down in place, per PodSet, without being suspended.
type JobWithPodSetShrink interface {
    // ShrinkPodSets patches the job so that each listed PodSet converges to
    // the given count. The job's own controller is responsible for
    // terminating the excess pods.
    ShrinkPodSets(ctx context.Context, c client.Client, counts workload.PodSetsCounts) error
}
```

The generic reconciler, upon seeing an admitted Workload slice with a
`shrink-target-counts` annotation on a job implementing this interface:

1. Calls `ShrinkPodSets` (e.g. batch Job: lower `spec.parallelism`;
   RayCluster: lower the worker group's `replicas`, optionally filling
   `scaleStrategy.workersToDelete`).
2. On the next reconcile, the job reports the reduced PodSet counts and the
   existing `EnsureWorkloadSlices` scale-down branch updates the slice's
   counts in place — precisely the KEP-77 user-initiated scale-down flow.
3. Once the slice's counts match the target, the annotation and condition are
   cleared.

Integrations that support workload slices but do not implement
`JobWithPodSetShrink` are simply never selected as shrink targets.

### Quota Accounting

Quota is released when the Workload slice's PodSet counts are updated by the
scale-down path — identical to KEP-77 semantics for user-initiated scale-down.
The cache and snapshot pick up the reduced usage, and the pending Workload is
admitted in a later scheduling cycle.

As with KEP-77, there is a window where excess pods are still terminating
after the counts have dropped. This is the same over-commit window that exists
for user-initiated scale-down today and is out of scope here.

### Enforcement and Fallback

The preemptor records the request time in the `ShrinkRequested` condition's
`lastTransitionTime`. A check in the workload reconciler (analogous to the
`waitForPodsReady` timeout handling) evicts the victim entirely, with reason
`PartialPreemptionTimeout`, if the slice has not reached the target counts
within `partial.timeout`. This preserves the invariant that a preemption
decision always eventually frees the computed resources.

While a shrink is pending on a victim, the preemptor treats the victim's usage
as its *target* (shrunk) usage in subsequent scheduling cycles, so it does not
double-preempt other workloads for the same demand — mirroring how evicted-but-
not-yet-gone workloads are handled today.

### Interaction with Fair Sharing

A shrunk Workload's usage drops, so its ClusterQueue's `DominantResourceShare`
drops accordingly — no changes needed to the fair-sharing value computation.
In `fairPreemptions`, the shrink refinement runs after strategy-based target
selection, the same way as in the classical path, and re-validates each
downgrade against the strategy (the share comparison is recomputed with the
shrunk usage rather than zero usage).

### Observability

- Events: `ShrunkDueToPreemption` on the victim (job and Workload), and the
  existing admission events on the preemptor.
- Metrics:
  - `partial_preemptions_total{cluster_queue, reason}` — shrinks issued.
  - `partial_preemption_fallback_evictions_total{cluster_queue}` — timeouts
    that degraded into full evictions.
- The `preempted_workloads_total` metric gains a `mode=Shrunk|Evicted` label.

### Test Plan

[x] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes necessary
to implement this enhancement.

#### Prerequisite testing updates

The workload-slice scale-down path (`pkg/workloadslicing`) must have solid
coverage for in-place count reduction of admitted slices, since this KEP makes
it a load-bearing part of preemption.

#### Unit tests

- `pkg/scheduler/preemption`: shrink refinement pass — minimal shrink
  computation, floors respected, non-shrinkable PodSets untouched, fallback to
  full eviction when shrinking is insufficient, interaction with fair-sharing
  strategies.
- `pkg/workloadslicing`: shrink-target annotation lifecycle.
- `pkg/controller/jobframework`: reconciler shrink dispatch, timeout fallback.
- Integrations (`batch/Job`, `RayCluster`): `ShrinkPodSets` patch logic.

#### Integration tests

- Pending high-priority workload triggers a shrink of a lower-priority elastic
  Job; the victim's slice counts drop; the pending workload is admitted; the
  victim's remaining pods are never recreated.
- Per-PodSet floors: a two-PodSet victim is shrunk only in the shrinkable
  PodSet.
- Timeout fallback: a victim whose job never converges is fully evicted after
  `partial.timeout`.
- Shrunk victim scales back up after the preemptor finishes (Story 3).
- Feature gate off / `partial.policy: Never`: behavior identical to today.

#### e2e tests

Extend the KubeRay e2e suite: a RayCluster (head + GPU workers, with
`preemption-min-counts`) is partially preempted by a higher-priority workload;
assert the head pod's UID is unchanged throughout, the workers are gone, and
the preemptor workload runs.

### Graduation Criteria

Alpha (this KEP):

- Feature gate `ElasticWorkloadPartialPreemption` (alpha, default off);
  requires `ElasticJobsViaWorkloadSlices`.
- Implemented for `batch/Job` and `RayCluster`.
- Classical preemption path; fair-sharing refinement may be alpha-scoped out
  if review deems it too risky, in which case fair-sharing queues fall back to
  full evictions.

Beta:

- Fair-sharing support complete.
- Metrics stabilized; feedback from at least one production Ray user.
- Re-evaluate defaulting `partial.timeout`.

## Implementation History

- 2026-07-18: Initial draft.

## Drawbacks

- Adds a second preemption mode, increasing the state space of the scheduler
  and the mental model for operators.
- Reclaiming via shrink is slower than eviction (job controller round-trip
  plus graceful pod termination), delaying admission of the preemptor.
- Extends the trust placed in job controllers: Kueue depends on them to
  actually remove pods. The timeout fallback bounds, but does not eliminate,
  the cost of a misbehaving controller.
- Partial state loss still occurs for the removed pods; only frameworks that
  genuinely tolerate worker loss benefit.

## Alternatives

### Reuse partial admission (KEP-420) via evict-and-readmit

Evict the victim entirely and rely on `minCount` partial admission to re-admit
it smaller. This requires no preemptor changes, but the full eviction restarts
*all* pods — the head node dies and the state this KEP exists to preserve is
lost. KEP-420 also targets admission time only and its counts are not
recomputed against preemption pressure.

### External policy controller

A controller outside Kueue watches for pending high-priority workloads and
proactively scales down victim jobs (triggering the KEP-77 scale-down path).
This works today without any Kueue changes and is the recommended interim
workaround. However, it must re-implement preemption policy (priorities,
cohort quota math, fair sharing) outside the scheduler, races Kueue's own
preemptor (which may still evict the whole victim first), and every
organization has to build its own.

### Split the job into multiple Workloads

Represent head and workers as separately admitted Workloads so the workers can
be evicted independently. This breaks gang admission across the job, is not
expressible for single-owner CRDs like RayCluster (one object → one Workload
chain), and pushes scheduling semantics into every integration.

### Pod-level preemption by Kueue

Have Kueue delete individual pods of the victim directly. This bypasses the
job controller's ownership of its pods: the controller would fight Kueue by
recreating them, and framework-specific victim selection (e.g. Ray preferring
idle workers) would be lost. Rejected on ownership grounds; count-based shrink
through the job object keeps the job controller in charge.
