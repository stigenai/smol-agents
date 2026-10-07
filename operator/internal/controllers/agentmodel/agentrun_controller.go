package agentmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	"github.com/smol-platform/smol-agents/operator/internal/builders"
	"github.com/smol-platform/smol-agents/operator/internal/controllers/features"
	"github.com/smol-platform/smol-agents/pkg/agentfs"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
	"github.com/smol-platform/smol-agents/pkg/agentruntime"
	"github.com/smol-platform/smol-agents/pkg/eventsink"
)

// memoryFSRetriever resolves a MemoryRetriever by name in the same namespace as
// the run and returns its filesystem mount input for AttachMemoryFS. It returns
// (zero, false, nil) when no retriever ref is set. It returns (zero, false, err)
// on a hard API error, and (zero, false, nil) for NotFound (treated as Pending).
func memoryFSRetriever(
	ctx context.Context,
	c client.Reader,
	run *amv1.AgentRun,
) (builders.MemoryMountInput, bool, error) {
	ref := run.Spec.MemoryRetrieverRef
	if ref == "" {
		return builders.MemoryMountInput{}, false, nil
	}

	retriever := &amv1.MemoryRetriever{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: ref}
	if err := c.Get(ctx, key, retriever); err != nil {
		if apierrors.IsNotFound(err) {
			// Recoverable: the retriever may not exist yet; caller marks Pending.
			return builders.MemoryMountInput{}, false, nil
		}
		return builders.MemoryMountInput{}, false, fmt.Errorf("get MemoryRetriever %q: %w", key, err)
	}

	// Only filesystem retrievers with mounting enabled produce a volume.
	if retriever.Spec.Mount == nil || !retriever.Spec.Mount.Enabled {
		return builders.MemoryMountInput{}, false, nil
	}

	// Resolve the first filesystem MemoryStore to get its AgentFSSpec.
	var agentFS *pure.AgentFSSpec
	for _, storeName := range retriever.Spec.Stores {
		store := &amv1.MemoryStore{}
		sk := types.NamespacedName{Namespace: run.Namespace, Name: storeName}
		if err := c.Get(ctx, sk, store); err != nil {
			continue // tolerate missing stores; skip non-filesystem ones
		}
		if store.Spec.Kind == pure.MemoryStoreFilesystem && store.Spec.AgentFS != nil {
			agentFS = store.Spec.AgentFS
			break
		}
	}
	if agentFS == nil {
		// Retriever references no filesystem store — nothing to mount.
		return builders.MemoryMountInput{}, false, nil
	}

	input := builders.MemoryMountInput{
		AgentFS: agentFS,
		Mount:   retriever.Spec.Mount,
	}
	return input, input.MountEnabled(), nil
}

// AgentRunReconciler turns an AgentRun CR into a Pod and tracks its
// lifecycle. State machine mirrors pure.Phase: Pending → Running →
// Completed | Failed | Cancelled.
type AgentRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// DefaultRunRuntimeClass is the sandbox RuntimeClass applied to run pods when
	// the Agent does not override it. Empty falls back to kata-fc. Set from
	// --default-run-runtime-class.
	DefaultRunRuntimeClass string
	// AllowHostRuntime permits runc (shared host kernel) on run pods; otherwise
	// a runc selection is a fail-closed R-SBX-1 policy violation. Set from
	// --allow-host-runtime (dev/CI clusters without a sandbox runtime).
	AllowHostRuntime bool
	// CNIEnforcesNetworkPolicy declares that the cluster CNI actually enforces
	// NetworkPolicy. Default false: the egress floor is created but reported as
	// "unenforced" on status (rv1.2) since CNIs like kindnet silently no-op it.
	// Set true (--cni-enforces-networkpolicy) on Cilium/Calico/eBPF clusters.
	CNIEnforcesNetworkPolicy bool
	// RequireEgressEnforcement fails closed (D3, knative-agents-7p3) instead of
	// only reporting: when true, a run with a BOUND AgentNetwork on a CNI that
	// cannot enforce NetworkPolicy (CNIEnforcesNetworkPolicy false) is held
	// Pending/EgressUnenforced rather than scheduled uncaged. The bare default
	// egress floor with no bound AgentNetwork is never gated — see the
	// Reconcile gate's comment. Default false (strictly opt-in) so existing
	// kind/CI clusters keep working unchanged. Set from
	// --require-egress-enforcement. Admission-time only: it does not
	// retroactively stall an already-running pod — see Reconcile's post-phase-
	// switch comment (knative-agents-1c5) for why.
	RequireEgressEnforcement bool
	// MaxConcurrentReconciles bounds parallel run reconciles (default 1).
	MaxConcurrentReconciles int
	// RunDeadlineMultiplier scales the run's wall-clock budget into the pod's
	// ActiveDeadlineSeconds hard backstop (0 → 1.5). Set from
	// --run-deadline-multiplier.
	RunDeadlineMultiplier float64
	// DefaultApprovalTimeout expires an un-decided pre-run approval when the
	// Agent's ApprovalPolicy sets no timeout (0 → 1h). Set from
	// --default-approval-timeout.
	DefaultApprovalTimeout time.Duration
	// DefaultNamespaceRunConcurrency caps Running AgentRuns per namespace when no
	// AgentRunQuota sets one (0 = unlimited). Set from
	// --default-namespace-run-concurrency.
	DefaultNamespaceRunConcurrency int32
	// EnableAdmissionQueue turns on per-namespace priority ordering (M1.13): when
	// a namespace has free capacity but higher-priority runs are queued, a
	// lower-priority/newer run waits its turn. Off (default) = M1.12 behavior
	// (admit any run while under cap). The ordering is computed statelessly from
	// live cluster state each reconcile, so it survives leader failover with no
	// in-memory queue to rebuild.
	EnableAdmissionQueue bool
	// MaxPriority clamps spec.priority (0 → 1000). Set from --max-run-priority.
	MaxPriority int32
	// ResultSinkClient POSTs result CloudEvents to an Agent's spec.resultSink (wbb).
	// nil → a default 5s-timeout client (so a slow sink never stalls reconcile).
	ResultSinkClient *http.Client
	// PlatformAgentPolicy names the platform-wide baseline AgentPolicy
	// (knative-agents-7dm, D1), threaded into compileNamespaceRedaction so a
	// policy-less namespace's status redaction also honors the baseline's
	// patterns. See AgentReconciler.PlatformAgentPolicy for the full semantics.
	// Zero value disables the fallback. Set from --platform-agent-policy.
	PlatformAgentPolicy types.NamespacedName
}

// SetupWithManager wires the controller; Owns(Pod) so we react to Pod
// status changes immediately. Owns the egress NetworkPolicy so it is GC'd
// with the run.
func (r *AgentRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mc := r.MaxConcurrentReconciles
	if mc < 1 {
		mc = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&amv1.AgentRun{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
		Owns(&networkingv1.NetworkPolicy{}).
		// Re-reconcile every non-terminal bound run when its AgentNetwork changes
		// (M1.16) so it picks up the new egress cage — whether still pre-Pod
		// (RunPrepPending / SandboxNotReady / … retries) or already Running.
		// knative-agents-1c5: the ensure helpers are update-in-place (not
		// create-only) and Reconcile calls them on BOTH paths, so a tightened
		// AgentNetwork re-cages a live pod, not just a still-queued run. A
		// currently-broken AgentNetwork does not fail a live run — see the
		// post-switch call site.
		Watches(&amv1.AgentNetwork{}, handler.EnqueueRequestsFromMapFunc(r.runsForNetwork)).
		WithOptions(controller.Options{MaxConcurrentReconciles: mc}).
		Complete(r)
}

// runsForNetwork maps an AgentNetwork change to reconcile requests for the
// non-terminal AgentRuns bound to it (their Agent matches the network's
// selector). M1.16.
func (r *AgentRunReconciler) runsForNetwork(ctx context.Context, obj client.Object) []reconcile.Request {
	an, ok := obj.(*amv1.AgentNetwork)
	if !ok {
		return nil
	}
	agents := agentsBoundToNetwork(ctx, r.Client, an)
	if len(agents) == 0 {
		return nil
	}
	var runs amv1.AgentRunList
	if err := r.List(ctx, &runs, client.InNamespace(an.Namespace)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range runs.Items {
		run := &runs.Items[i]
		if !agents[run.Spec.AgentRef] || pure.Phase(run.Status.State).Terminal() {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}})
	}
	return reqs
}

// Reconcile is the per-Run entrypoint.
func (r *AgentRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("agentrun", req.NamespacedName)

	run := &amv1.AgentRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Resolve the parent Agent.
	agent := &amv1.Agent{}
	err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.AgentRef}, agent)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.markPending(run, "AgentMissing", "spec.agentRef not found")
			return r.updateRunStatus(ctx, run, ctrl.Result{})
		}
		return ctrl.Result{}, err
	}

	// Cancellation: if spec.cancel is set and we're not yet terminal,
	// stamp Cancelled and (best-effort) delete the Pod.
	if run.Spec.Cancel && !run.Status.State.Terminal() {
		_ = r.deletePod(ctx, run)
		r.markTerminal(run, pure.PhaseCancelled, "cancel:requested")
		return r.updateRunStatus(ctx, run, ctrl.Result{})
	}

	// A session.required Agent (D4/M4.4) is served by its resident AgentSession
	// worker — turns are delivered to a warm pod, never a one-shot run. A turn
	// that shares the session (spec.sessionRef set, e.g. AgentFS / Hermes memory
	// scoping) is fine; a bare standalone AgentRun against a resident-only agent
	// is a misuse, so fail it fast (before any pod or cost) rather than spawn an
	// ephemeral RestartPolicy=Never pod that bypasses the resident worker.
	if !run.Status.State.Terminal() && run.Spec.SessionRef == "" &&
		agent.Spec.Session != nil && agent.Spec.Session.Required {
		r.markTerminal(run, pure.PhaseFailed, "agent:requires-session")
		return r.updateRunStatus(ctx, run, ctrl.Result{})
	}

	// Ensure the Pod exists.
	pod := &corev1.Pod{}
	err = r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, pod)
	if apierrors.IsNotFound(err) {
		// Pre-run approval gate (M5): hold the run in RequiresAction until a
		// human approves via spec.decision, before any pod or cost exists. The
		// cancel check above still wins. handled ⇒ status is set, return now.
		if handled, gres, gerr := r.preRunApproval(ctx, run, agent); handled {
			return gres, gerr
		}

		// Per-tenant concurrency gate (D10): hold the run Pending if the Agent or
		// namespace is at its Running-runs cap. Soft / eventually-consistent
		// (MaxConcurrentReconciles bounds boundary overshoot); fails open on a
		// list error so an apiserver hiccup never strands a run.
		if handled, cres, cerr := r.admitRunConcurrency(ctx, run, agent); handled {
			return cres, cerr
		}

		// Resolve run-pod isolation fail-closed before any prep work: a policy
		// violation (runc without operator opt-in) fails the run; a not-yet-
		// registered hardened RuntimeClass holds it Pending rather than
		// scheduling an unisolated pod (R-SBX-1 on the run datapath).
		sbClass, sbPending, sbFailed := r.resolveRunSandbox(ctx, agent)
		if sbFailed != "" {
			r.markTerminal(run, pure.PhaseFailed, "sandbox:"+sbFailed)
			return r.updateRunStatus(ctx, run, ctrl.Result{})
		}
		if sbPending != "" {
			r.markPending(run, "SandboxNotReady", sbPending)
			return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 15 * time.Second})
		}

		// D3 (M3.15): danger permission/sandbox flags are admission-refused unless
		// the resolved class is a kata microVM. Fail-closed: a shared-kernel class
		// (runc/gvisor) must never run --dangerously-skip-permissions / approval
		// "never" / danger-full-access. No-op for the safe default posture.
		if reason := dangerFlagViolation(agent, sbClass); reason != "" {
			r.markTerminal(run, pure.PhaseFailed, "sandbox:"+reason)
			return r.updateRunStatus(ctx, run, ctrl.Result{})
		}

		// Fail-closed egress-enforcement gate (knative-agents-7p3, D3), opt-in via
		// --require-egress-enforcement. Without it, a run bound to an
		// AgentNetwork on a non-enforcing CNI (CNIEnforcesNetworkPolicy false)
		// still gets scheduled — the egress NetworkPolicy is created but the CNI
		// silently no-ops it, so the run executes uncaged with the gap visible
		// only as status.EgressEnforcement="unenforced". With the flag on, hold
		// the run Pending instead. The trigger is "a bound AgentNetwork exists"
		// (netPlan.Networks, the same field resolveBoundNetworks/
		// egressEnforcementLabel already populate for status) — NOT the presence
		// of allow rules: a wireguardMesh (or nil-proxy) bound network
		// contributes no AllowRules to the plan (see plan.BuildNetworkPlan) but
		// is still a real binding the author intended to restrict egress with,
		// so gating only on AllowRules would silently miss it. The bare default
		// floor with NO bound AgentNetwork is deliberately NOT gated — it is
		// defense-in-depth applied to every run, and blocking every run on every
		// non-enforcing cluster would make the flag unusable. A NetworkPlan
		// compose error is left for ensureRunEgressPolicy below to classify
		// (NetworkConflict/NetworkDatapathUnwired) rather than duplicated here.
		if r.RequireEgressEnforcement && !r.CNIEnforcesNetworkPolicy {
			if netPlan, nerr := resolveBoundNetworks(ctx, r.Client, agent); nerr == nil && len(netPlan.Networks) > 0 {
				r.markPending(run, "EgressUnenforced",
					fmt.Sprintf("bound AgentNetwork(s) %v need egress enforcement the CNI cannot provide (--require-egress-enforcement)", netPlan.Networks))
				return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 30 * time.Second})
			}
		}

		// Resolve node placement for the sandbox class. A KVM class with no
		// matching AgentNodePool holds the run Pending (fail-closed) rather than
		// scheduling a kata pod that can never run — unless PlacementFallback is
		// "Schedule" (a dev/unlabelled-cluster escape hatch). R-PROV-2 / D3.
		placement, _, plErr := features.ResolvePlacementForClass(ctx, r.Client, sbClass)
		if plErr != nil {
			return ctrl.Result{}, fmt.Errorf("resolve placement: %w", plErr)
		}
		if placement == nil && builders.RequiresKVM(sbClass) && run.Spec.PlacementFallback != "Schedule" {
			r.markPending(run, "NoKVMCapacity",
				fmt.Sprintf("no AgentNodePool provides isolation %q", sbClass))
			return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 30 * time.Second})
		}

		// Resolve the loop-mode ModelProvider + gather every secret the broker
		// must serve (harness env secretRef + the provider API key). A missing
		// source Secret / ModelProvider keeps the run Pending and retries.
		provider, brokerValues, prepErr := r.prepareRun(ctx, run, agent)
		if prepErr != nil {
			r.markPending(run, "RunPrepPending", prepErr.Error())
			return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 10 * time.Second})
		}

		// Render the spec the run pod executes (`agent run` reads it) before the
		// pod, so the mounted ConfigMap exists when the pod schedules.
		if err := r.ensureRunSpec(ctx, run, agent, provider); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure run spec: %w", err)
		}

		// Generate the per-run broker config (resolved values + a uid-keyed
		// policy) before the pod mounts it.
		if len(brokerValues) > 0 {
			if err := r.ensureBrokerConfig(ctx, run, brokerValues); err != nil {
				return ctrl.Result{}, fmt.Errorf("ensure broker config: %w", err)
			}
		}

		desired := builders.BuildAgentRunPod(run, agent)
		// Pin the resolved sandbox RuntimeClass so the run executes under a real
		// isolation boundary (default kata-fc), not the cluster-default runtime.
		builders.ApplyRunSandbox(desired, sbClass)

		// Re-enable the apiserver token only for an A2A-capable Agent (its A2A Role
		// exists — the authoritative signal it declares a kind=agent tool). Every
		// other run pod keeps AutomountServiceAccountToken=false (D1).
		hasA2A, a2aErr := agentHasA2AGrant(ctx, r.Client, agent)
		if a2aErr != nil {
			return ctrl.Result{}, fmt.Errorf("check A2A grant: %w", a2aErr)
		}
		if hasA2A {
			builders.AllowA2AToken(desired)
		}

		// M2.26: wire workspace-file egress onto the AgentFS serve sidecar (the
		// only container with S3 creds; the harness/agent container gets none). The
		// sidecar collects + uploads on shutdown and reports its manifest via its
		// termination message — no new k8s client/RBAC. No-op without agentfs +
		// spec.artifacts.
		builders.ApplyArtifactCollection(desired, &agent.Spec, run.Namespace, run.Name)

		// Bind the pod to its kata node pool (no-op for runc / no pool) and set a
		// hard ActiveDeadlineSeconds backstop from the effective wall-clock budget
		// (BudgetOverride wins over the Agent's budget).
		builders.ApplyRunPodPlacement(desired, placement)
		// An explicit per-harness ActiveDeadlineSeconds (pi-mono, M4.18) wins over
		// the budget-derived backstop; else the deadline is MaxWallClock * grace
		// (BudgetOverride wins over the Agent's budget). DeadlineExceeded maps to a
		// terminal run (terminationReason).
		if h := agent.Spec.Harness; h != nil && h.PiMono != nil && h.PiMono.ActiveDeadlineSeconds > 0 {
			secs := int64(h.PiMono.ActiveDeadlineSeconds)
			desired.Spec.ActiveDeadlineSeconds = &secs
		} else {
			effWall := agent.Spec.Budget.MaxWallClockSeconds
			if run.Spec.BudgetOverride != nil && run.Spec.BudgetOverride.MaxWallClockSeconds > 0 {
				effWall = run.Spec.BudgetOverride.MaxWallClockSeconds
			}
			builders.ApplyRunDeadline(desired, effWall, r.RunDeadlineMultiplier)
		}

		// Attach filesystem MemoryRetriever mount if referenced (R-MEM-FS-2).
		mountInput, mountEnabled, mountErr := memoryFSRetriever(ctx, r.Client, run)
		if mountErr != nil {
			return ctrl.Result{}, fmt.Errorf("resolve memory retriever: %w", mountErr)
		}
		if mountEnabled {
			builders.AttachMemoryFS(desired, mountInput)
			logger.Info("attached memory AgentFS volume", "retriever", run.Spec.MemoryRetrieverRef)
		} else if run.Spec.MemoryRetrieverRef != "" && !mountEnabled {
			// Retriever referenced but not yet resolvable — stay Pending.
			r.markPending(run, "MemoryRetrieverPending",
				fmt.Sprintf("MemoryRetriever %q not ready for mounting", run.Spec.MemoryRetrieverRef))
			return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 10 * time.Second})
		}

		// Attach the in-pod secret broker when there are secrets to serve
		// (harness env secretRef and/or the loop provider key).
		if len(brokerValues) > 0 {
			builders.AttachSecretBroker(desired, run.Name)
		}

		// Cage egress: a default-deny NetworkPolicy (blocks instance-metadata /
		// arbitrary outbound) must exist before the pod schedules.
		if err := r.ensureRunEgressPolicy(ctx, run, agent, desired); err != nil {
			// A NetworkPlan compose conflict is a spec-level error in the bound
			// AgentNetworks — hold the run Pending (fail-closed, visible) rather
			// than caging it wrong; a fixed network re-reconciles it (Watches).
			if errors.Is(err, ErrNetworkConflict) {
				r.markPending(run, "NetworkConflict", err.Error())
				return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 30 * time.Second})
			}
			// A bound network needing the unwired Tier-2 (proxy/eBPF) datapath is a
			// terminal spec error — fail closed (c5r.20) rather than running with the
			// requested enforcement silently dropped.
			if errors.Is(err, ErrNetworkDatapathUnwired) {
				r.markTerminal(run, pure.PhaseFailed, "network:"+err.Error())
				return r.updateRunStatus(ctx, run, ctrl.Result{})
			}
			return ctrl.Result{}, fmt.Errorf("ensure egress policy: %w", err)
		}

		// Cage ingress: same-namespace-only floor closes the cross-namespace
		// tenant-boundary hole (knative-agents-8s1, D1) before the pod schedules.
		if err := r.ensureRunIngressPolicy(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure ingress policy: %w", err)
		}

		if err := ctrl.SetControllerReference(run, desired, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set controller ref: %w", err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, fmt.Errorf("create pod: %w", err)
		}
		r.markRunning(run)
		// M2.26: mark artifact egress Pending when collection was wired, so status
		// reflects that an upload is expected; foldArtifacts overwrites it with the
		// sidecar's terminal verdict (Complete/Partial/Failed). Never affects State.
		if artifactsRequested(desired) {
			run.Status.Artifacts = &pure.ArtifactsStatus{State: pure.ArtifactStatePending}
		}
		logger.Info("created run pod", "agent", agent.Name)
		return r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 5 * time.Second})
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Map Pod phase → AgentRun phase.
	switch pod.Status.Phase {
	case corev1.PodPending:
		r.markPending(run, "PodPending", "Pod is Pending")
	case corev1.PodRunning:
		r.markRunning(run)
	case corev1.PodSucceeded:
		r.markTerminal(run, pure.PhaseCompleted, "")
		r.foldRunResult(ctx, run, agent, pod)
		r.foldArtifacts(run, pod)
		// wbb: emit a result CloudEvent to the Agent's sink, once (the output is now
		// folded). Best-effort + annotation-guarded so a re-reconcile of this
		// terminal run does not re-POST.
		r.emitResultOnce(ctx, run, agent)
	case corev1.PodFailed:
		r.markTerminal(run, pure.PhaseFailed, terminationReason(pod))
		r.foldRunResult(ctx, run, agent, pod)
		r.foldArtifacts(run, pod)
	}

	// Re-cage the live pod in place (knative-agents-1c5). The AgentNetwork Watch
	// re-enqueues every non-terminal bound run, so recompute + reapply the cage
	// here too — otherwise a tightened AgentNetwork never reaches a pod that
	// already exists. Gated AFTER the phase switch so a run whose terminal
	// transition is folded on THIS reconcile is already excluded.
	if !run.Status.State.Terminal() {
		if err := r.ensureRunEgressPolicy(ctx, run, agent, pod); err != nil {
			// A broken bound AgentNetwork is fatal pre-Pod (nothing spent yet)
			// but NOT here: the pod is already running and billing, so flipping
			// it Pending/Failed stops nothing and just lets an unrelated
			// AgentNetwork typo collaterally kill a live agent. Not fail-open —
			// both sentinels return before the Spec is touched, so the pod stays
			// caged by the (at-least-as-tight) policy it was admitted under.
			if errors.Is(err, ErrNetworkConflict) || errors.Is(err, ErrNetworkDatapathUnwired) {
				reason := "NetworkConflict"
				if errors.Is(err, ErrNetworkDatapathUnwired) {
					reason = "NetworkDatapathUnwired"
				}
				logger.Info("bound AgentNetwork unappliable to a live run; keeping its existing cage",
					"reason", reason, "error", err)
				run.Status.Reason = reason
			} else {
				return ctrl.Result{}, fmt.Errorf("re-cage egress policy: %w", err)
			}
		} else {
			if run.Status.Reason == "NetworkConflict" || run.Status.Reason == "NetworkDatapathUnwired" {
				// Resolves cleanly again — clear the marker, which markRunning only
				// does on a Pending→Running edge, not on a steady-state reconcile.
				run.Status.Reason = ""
			}
			// RequireEgressEnforcement (knative-agents-7p3) is an ADMISSION-time
			// control only — see the pre-Pod gate above. It never retroactively
			// stalls or kills an already-running pod: the pod is already running
			// and billing, so flipping it Pending/Failed here would stop nothing,
			// exactly the ErrNetworkConflict carve-out's reasoning above. Instead
			// this mirrors that carve-out's shape: when a live run's bound-network
			// egress is (still, or newly) unenforceable, surface it on Reason,
			// observability-only, never on State — set/cleared each reconcile as
			// the condition comes and goes, without disturbing any more specific
			// Reason (e.g. "PodPending") already occupying the field.
			egressUnenforced := r.RequireEgressEnforcement && !r.CNIEnforcesNetworkPolicy && len(run.Status.Networks) > 0
			switch {
			case egressUnenforced && run.Status.Reason == "":
				run.Status.Reason = "EgressUnenforced"
			case !egressUnenforced && run.Status.Reason == "EgressUnenforced":
				run.Status.Reason = ""
			}
		}
		// The ingress floor has no AgentNetwork dependency, so it re-applies
		// regardless of the egress sentinel handling above.
		if err := r.ensureRunIngressPolicy(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("re-cage ingress policy: %w", err)
		}
	}

	// poll Pod state every 5s until terminal
	res := ctrl.Result{}
	if !run.Status.State.Terminal() {
		res = ctrl.Result{RequeueAfter: 5 * time.Second}
	}
	return r.updateRunStatus(ctx, run, res)
}

// updateRunStatus persists run.Status and maps the outcome onto the (Result,
// error) the reconcile returns. An optimistic-lock conflict is benign and
// expected: the cached AgentRun can lag a just-written status (e.g. the Pod
// watch reconcile racing the periodic 5s poll), so a conflict means the object
// already advanced — requeue and let the next reconcile read fresh, rather than
// surfacing a noisy "Reconciler error". onSuccess is returned verbatim when the
// write lands.
func (r *AgentRunReconciler) updateRunStatus(ctx context.Context, run *amv1.AgentRun, onSuccess ctrl.Result) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, run); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return onSuccess, nil
}

func (r *AgentRunReconciler) markPending(run *amv1.AgentRun, reason, msg string) {
	run.Status.State = pure.PhasePending
	run.Status.Reason = reason
	run.Status.TerminationReason = msg
	run.Status.StartedAt = nil
	setReadyCondition(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, msg)
	setProgressingCondition(&run.Status.Conditions, run.Generation, true, reason, msg)
}

func (r *AgentRunReconciler) markRequiresAction(run *amv1.AgentRun, pa *pure.PendingAction) {
	run.Status.State = pure.PhaseRequiresAction
	run.Status.PendingAction = pa
	run.Status.TerminationReason = "" // non-terminal; clear any stale Pending hint
	setReadyCondition(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, "RequiresAction", "awaiting an approval decision")
	setProgressingCondition(&run.Status.Conditions, run.Generation, true, "RequiresAction", "awaiting an approval decision")
}

// approvalTimeout resolves the effective pre-run approval TTL: the Agent's
// override, else the operator default, else 1h.
func (r *AgentRunReconciler) approvalTimeout(agent *amv1.Agent) time.Duration {
	if agent.Spec.Approval != nil && agent.Spec.Approval.ApprovalTimeoutSeconds > 0 {
		return time.Duration(agent.Spec.Approval.ApprovalTimeoutSeconds) * time.Second
	}
	if r.DefaultApprovalTimeout > 0 {
		return r.DefaultApprovalTimeout
	}
	return time.Hour
}

// preRunApproval implements the M5 pre-run approval gate. It returns handled=true
// (with the result/err to return) when the run is waiting for, denied by, or has
// expired an approval — NO pod is created in those cases. handled=false means the
// run is ungated or approved and reconcile should proceed to create the pod.
func (r *AgentRunReconciler) preRunApproval(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent) (handled bool, res ctrl.Result, err error) {
	gated := agent.Spec.Approval != nil && agent.Spec.Approval.RequireApprovalBeforeRun
	if run.Spec.RequireApprovalBeforeRun != nil {
		gated = *run.Spec.RequireApprovalBeforeRun // per-run override
	}
	if !gated {
		return false, ctrl.Result{}, nil
	}

	// A decision only counts when its token matches the pending one (or none has
	// been minted yet) — a stale/mismatched token is ignored, keeping the run
	// parked rather than acting on an approval for a different state.
	tokenMatches := run.Status.PendingAction == nil ||
		(run.Spec.Decision != nil && run.Spec.Decision.Token == run.Status.PendingAction.Token)

	if d := run.Spec.Decision; d != nil && tokenMatches {
		if d.Approve {
			run.Status.PendingAction = nil // clear the gate; pod-create proceeds
			return false, ctrl.Result{}, nil
		}
		r.markTerminal(run, pure.PhaseCancelled, "decision:denied:"+d.Reason)
		out, e := r.updateRunStatus(ctx, run, ctrl.Result{})
		return true, out, e
	}

	// No matching decision yet — mint the pending token on first entry.
	if run.Status.PendingAction == nil {
		token := string(run.UID)
		if token == "" {
			token = run.Name
		}
		r.markRequiresAction(run, &pure.PendingAction{
			Kind:        "pre-run",
			Token:       token,
			RequestedAt: metav1.Now(),
			Reason:      "awaiting human approval before run",
		})
		out, e := r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: r.approvalTimeout(agent)})
		return true, out, e
	}

	// Already pending — expire on TTL.
	if !run.Status.PendingAction.RequestedAt.IsZero() &&
		time.Since(run.Status.PendingAction.RequestedAt.Time) >= r.approvalTimeout(agent) {
		r.markTerminal(run, pure.PhaseExpired, "approval:timeout")
		out, e := r.updateRunStatus(ctx, run, ctrl.Result{})
		return true, out, e
	}

	// Still waiting (incl. a stale/mismatched decision token) — requeue near TTL.
	out, e := r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 30 * time.Second})
	return true, out, e
}

// admitRunConcurrency holds a run Pending when its Agent or namespace is at the
// Running-runs cap. handled=true ⇒ status set + return now. Fails open (admits)
// on a count error.
func (r *AgentRunReconciler) admitRunConcurrency(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent) (handled bool, res ctrl.Result, err error) {
	perAgentCap := agent.Spec.MaxConcurrentRuns
	nsCap := r.resolveNamespaceCap(ctx, run.Namespace)
	if perAgentCap <= 0 && nsCap <= 0 {
		return false, ctrl.Result{}, nil
	}
	perAgent, total, queued, cErr := r.namespaceRunState(ctx, run.Namespace, run.Name)
	if cErr != nil {
		return false, ctrl.Result{}, nil // fail open
	}
	if perAgentCap > 0 && perAgent[run.Spec.AgentRef] >= perAgentCap {
		r.markPending(run, "ConcurrencyLimited",
			fmt.Sprintf("agent %q at concurrency cap %d", run.Spec.AgentRef, perAgentCap))
		out, e := r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 10 * time.Second})
		return true, out, e
	}
	if nsCap > 0 {
		if total >= nsCap {
			r.markPending(run, "ConcurrencyLimited",
				fmt.Sprintf("namespace at concurrency cap %d", nsCap))
			out, e := r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 10 * time.Second})
			return true, out, e
		}
		// Priority ordering (M1.13): even with free capacity, a run waits when
		// higher-priority (or equal-priority, older) runs are queued ahead of it
		// for the remaining slots. Stateless — recomputed from the queued set, so
		// it needs no in-memory queue + survives leader failover.
		if r.EnableAdmissionQueue {
			if ahead := r.rankAhead(queued, run); int32(ahead) >= nsCap-total {
				r.markPending(run, "ConcurrencyLimited",
					fmt.Sprintf("queued: %d higher-priority run(s) ahead for %d free slot(s)", ahead, nsCap-total))
				out, e := r.updateRunStatus(ctx, run, ctrl.Result{RequeueAfter: 10 * time.Second})
				return true, out, e
			}
		}
	}
	return false, ctrl.Result{}, nil
}

// namespaceRunState lists the namespace's AgentRuns once and returns the
// per-agent + total Running counts (only Running runs hold a slot, per M1.12)
// plus the concurrency-queued runs (Pending/ConcurrencyLimited, excluding self)
// that compete for free slots in the M1.13 priority order.
func (r *AgentRunReconciler) namespaceRunState(ctx context.Context, ns, selfName string) (perAgent map[string]int32, total int32, queued []amv1.AgentRun, err error) {
	var list amv1.AgentRunList
	if err := r.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil, 0, nil, err
	}
	perAgent = map[string]int32{}
	for i := range list.Items {
		run := &list.Items[i]
		if run.Name == selfName {
			continue
		}
		switch {
		case run.Status.State == pure.PhaseRunning:
			perAgent[run.Spec.AgentRef]++
			total++
		case run.Status.State == pure.PhasePending && run.Status.Reason == "ConcurrencyLimited":
			queued = append(queued, *run)
		}
	}
	return perAgent, total, queued, nil
}

// rankAhead counts the queued runs that should be admitted before run. Ordering
// is higher effective priority first, then — within a priority tier —
// PER-AGENT ROUND-ROBIN (rv2.3): a run is ranked by its position in its OWN
// Agent's FIFO queue, so Agent A's 2nd run waits behind Agent B's 1st rather than
// a noisy Agent's backlog starving everyone else under a namespace cap. Same
// round-robin slot ties break by creationTimestamp asc, then name asc (a total
// order). A single-Agent queue degenerates to plain priority-then-FIFO.
func (r *AgentRunReconciler) rankAhead(queued []amv1.AgentRun, run *amv1.AgentRun) int {
	selfP := r.clampPriority(run.Spec.Priority)
	selfIdx := r.agentLocalRank(queued, run)
	ahead := 0
	for i := range queued {
		q := &queued[i]
		qP := r.clampPriority(q.Spec.Priority)
		switch {
		case qP > selfP:
			ahead++
		case qP < selfP:
			// behind
		default: // same priority tier → per-Agent round-robin
			qIdx := r.agentLocalRank(queued, q)
			switch {
			case qIdx < selfIdx:
				ahead++
			case qIdx > selfIdx:
				// behind
			case q.CreationTimestamp.Before(&run.CreationTimestamp):
				ahead++
			case q.CreationTimestamp.Equal(&run.CreationTimestamp) && q.Name < run.Name:
				ahead++
			}
		}
	}
	return ahead
}

// agentLocalRank is run's position in its OWN Agent's FIFO queue within its
// priority tier: how many queued runs of the SAME agentRef at the SAME effective
// priority are older (creation asc, then name asc). Round-robin fairness ranks
// across Agents by this index (every Agent's k-th run competes together).
func (r *AgentRunReconciler) agentLocalRank(queued []amv1.AgentRun, run *amv1.AgentRun) int {
	selfP := r.clampPriority(run.Spec.Priority)
	idx := 0
	for i := range queued {
		q := &queued[i]
		if q.Spec.AgentRef != run.Spec.AgentRef || r.clampPriority(q.Spec.Priority) != selfP || q.Name == run.Name {
			continue
		}
		if q.CreationTimestamp.Before(&run.CreationTimestamp) ||
			(q.CreationTimestamp.Equal(&run.CreationTimestamp) && q.Name < run.Name) {
			idx++
		}
	}
	return idx
}

// clampPriority bounds spec.priority to [0, MaxPriority] (MaxPriority 0 → 1000).
func (r *AgentRunReconciler) clampPriority(p int32) int32 {
	max := r.MaxPriority
	if max <= 0 {
		max = 1000
	}
	if p < 0 {
		return 0
	}
	if p > max {
		return max
	}
	return p
}

// resolveNamespaceCap returns the strictest (smallest non-zero) MaxConcurrentRuns
// across the namespace's AgentRunQuotas, else the operator default.
func (r *AgentRunReconciler) resolveNamespaceCap(ctx context.Context, ns string) int32 {
	var list amv1.AgentRunQuotaList
	if err := r.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return r.DefaultNamespaceRunConcurrency
	}
	cap := int32(0)
	for i := range list.Items {
		if m := list.Items[i].Spec.MaxConcurrentRuns; m > 0 && (cap == 0 || m < cap) {
			cap = m
		}
	}
	if cap == 0 {
		return r.DefaultNamespaceRunConcurrency
	}
	return cap
}

func (r *AgentRunReconciler) markRunning(run *amv1.AgentRun) {
	if run.Status.State == pure.PhaseRunning {
		return
	}
	run.Status.State = pure.PhaseRunning
	run.Status.Reason = "" // clear any prior Pending reason (e.g. ConcurrencyLimited)
	now := metav1.Now()
	run.Status.StartedAt = &now
	setReadyCondition(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, "Running", "")
	setProgressingCondition(&run.Status.Conditions, run.Generation, true, "Running", "")
}

func (r *AgentRunReconciler) markTerminal(run *amv1.AgentRun, phase pure.Phase, reason string) {
	if run.Status.State.Terminal() && run.Status.State == phase {
		return
	}
	run.Status.State = phase
	now := metav1.Now()
	run.Status.EndedAt = &now
	// Terminal state owns TerminationReason — overwrite any stale Pending/
	// Running hint (e.g. "Pod is Pending" left by an earlier markPending).
	// foldRunResult may refine this further with the runtime's own reason.
	run.Status.TerminationReason = reason

	readyStatus := metav1.ConditionFalse
	condReason := string(phase)
	if phase == pure.PhaseCompleted {
		readyStatus = metav1.ConditionTrue
	}
	setReadyCondition(&run.Status.Conditions, run.Generation, readyStatus, condReason, reason)
	setProgressingCondition(&run.Status.Conditions, run.Generation, false, condReason, reason)
}

// emitResultOnce POSTs a com.smol-agents.run.completed CloudEvent to the Agent's
// spec.resultSink when this run first completes (wbb). The once-guard + bounded
// emit live in the shared emitResultEventOnce; this only shapes the run's event.
func (r *AgentRunReconciler) emitResultOnce(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent) {
	emitResultEventOnce(ctx, r.Client, r.ResultSinkClient, run, agent.Spec.ResultSink, eventsink.Event{
		ID:     string(run.UID),
		Type:   "com.smol-agents.run.completed",
		Source: fmt.Sprintf("/namespaces/%s/agentruns/%s", run.Namespace, run.Name),
		Data:   run.Status.Output,
	})
}

func (r *AgentRunReconciler) deletePod(ctx context.Context, run *amv1.AgentRun) error {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	return r.Delete(ctx, pod)
}

func terminationReason(pod *corev1.Pod) string {
	// Pod-level reason first: the kubelet sets pod.Status.Reason (e.g.
	// "DeadlineExceeded" from activeDeadlineSeconds, "Evicted") when it kills the
	// pod before any container records its own terminated reason — so a run that
	// blew its wall-clock deadline (M1.10) surfaces as pod:DeadlineExceeded
	// rather than a generic pod:Failed (M4.18).
	if pod.Status.Reason != "" {
		return "pod:" + pod.Status.Reason
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return "pod:" + cs.State.Terminated.Reason
		}
	}
	return "pod:Failed"
}

// prepareRun resolves the loop-mode ModelProvider (rendered into the run spec)
// and gathers every secret the broker must serve: each harness env secretRef
// value plus the provider API key. Values are keyed by lease name (the
// SecretRef.SecretName the runtime leases by). Harness mode returns a nil
// provider; agents with no secrets return an empty map.
func (r *AgentRunReconciler) prepareRun(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent) (*builders.RunProvider, map[string][]byte, error) {
	return gatherRunSecrets(ctx, r.Client, agent, run.Namespace, run.Spec.Inputs)
}

// resolveRunSandbox picks the run pod's RuntimeClass, fail-closed. At most one
// of (class, pending, failed) is non-empty: failed is a hard R-SBX-1 violation
// (runc without operator opt-in); pending means the chosen hardened RuntimeClass
// is not registered yet, so we refuse to schedule an unisolated pod and wait. An
// empty Agent override falls back to --default-run-runtime-class, then kata-fc.
func (r *AgentRunReconciler) resolveRunSandbox(ctx context.Context, agent *amv1.Agent) (class, pending, failed string) {
	return resolveSandbox(ctx, r.Client, agent.Spec.Sandbox.RuntimeClass, r.DefaultRunRuntimeClass, r.AllowHostRuntime)
}

// ensureRunEgressPolicy creates or UPDATES the run pod's default-deny egress
// NetworkPolicy (idempotent), owned by the run so it is GC'd with it.
// knative-agents-1c5: update-in-place (not create-only) so a recomputed cage
// — e.g. a tightened AgentNetwork allow-list, or a newly-added M1.18
// apiserver rule — actually lands instead of being silently discarded
// against a stale stored Spec. Security direction: this can only make an
// in-flight run's egress cage MORE restrictive on a given reconcile than
// what it had before, never less, since the desired Spec is always freshly
// recomputed from the bound AgentNetwork (fail-closed governance, D3). See
// SetupWithManager's Watch comment for the scope of when this actually fires
// during a run's lifecycle: called both before the run's Pod exists (in the
// create path below) AND on every reconcile of an already-Pod'd, non-terminal
// run (after the "map Pod phase" switch) — so a bound AgentNetwork is now
// re-applied for the run's whole live span, not just once at admission.
func (r *AgentRunReconciler) ensureRunEgressPolicy(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent, pod *corev1.Pod) error {
	netPlan, err := resolveBoundNetworks(ctx, r.Client, agent)
	if err != nil {
		// May wrap ErrNetworkConflict. The pre-Pod caller maps that to
		// Pending; the post-Pod (live-run) caller does NOT — see its comment
		// — it keeps the run Running and its existing NetworkPolicy as-is.
		return err
	}
	// Surface the egress posture for observability (M1.19): the bound network
	// names + whether a tightened allow-list applies on top of the floor.
	run.Status.Networks = netPlan.Networks
	run.Status.EgressEnforcement = egressEnforcementLabel(netPlan, r.CNIEnforcesNetworkPolicy)
	// Wire-or-gate the Tier-2 seam (c5r.20): a bound network needing the unwired
	// proxy/eBPF datapath fails closed. The pre-Pod caller maps
	// ErrNetworkDatapathUnwired to Failed; the post-Pod (live-run) caller does
	// not (same carve-out as ErrNetworkConflict above).
	if err := checkTier2Wired(netPlan); err != nil {
		return err
	}
	// Tier-2 datapath seam (no-op in Phase 1; Tier-1 is the NetworkPolicy below).
	builders.AttachAgentNetwork(pod, netPlan)
	np := builders.BuildAgentRunEgressPolicyWithPlan(run, netPlan)
	// M1.18: allow the kube-apiserver endpoints (e.g. <node-ip>:6443 on a
	// public-IP cluster), which the default floor would otherwise block.
	if rule := apiserverEgressRule(ctx, r.Client); rule != nil {
		np.Spec.Egress = append(np.Spec.Egress, *rule)
	}
	return r.ensureNetworkPolicy(ctx, run, np)
}

// ensureRunIngressPolicy creates or UPDATES the run pod's same-namespace-only
// ingress NetworkPolicy (idempotent; a separate function from
// ensureRunEgressPolicy purely because the two seams — egress plan
// resolution vs. a fixed same-namespace floor — differ, not because of any
// create-vs-update split; both now share ensureNetworkPolicy), owned by the
// run so it is GC'd with it (knative-agents-8s1: without this, a run pod is
// reachable from any pod in any namespace, a tenant-boundary hole under D1).
func (r *AgentRunReconciler) ensureRunIngressPolicy(ctx context.Context, run *amv1.AgentRun) error {
	np := builders.BuildAgentRunIngressPolicy(run)
	return r.ensureNetworkPolicy(ctx, run, np)
}

// ensureNetworkPolicy creates or UPDATES a run's owned NetworkPolicy (egress
// or ingress) in place, mirroring AgentSessionReconciler.ensureNetworkPolicy.
// knative-agents-1c5: update-in-place replaces the prior create-only
// behavior, under which a recomputed desired Spec (from a mutated
// AgentNetwork) was thrown away once the object already existed.
func (r *AgentRunReconciler) ensureNetworkPolicy(ctx context.Context, run *amv1.AgentRun, np *networkingv1.NetworkPolicy) error {
	if err := ctrl.SetControllerReference(run, np, r.Scheme); err != nil {
		return err
	}
	existing := &networkingv1.NetworkPolicy{}
	err := r.Get(ctx, client.ObjectKeyFromObject(np), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, np)
	}
	if err != nil {
		return err
	}
	existing.Spec = np.Spec
	return r.Update(ctx, existing)
}

// readSecret fetches one key from a k8s Secret (the sole key when key is empty).
func (r *AgentRunReconciler) readSecret(ctx context.Context, ns, name, key string) ([]byte, error) {
	return readSecretKey(ctx, r.Client, ns, name, key)
}

// ensureBrokerConfig creates (once) the run-owned Secret holding the
// secret-proxy config (static backend + policy) the broker sidecar reads.
func (r *AgentRunReconciler) ensureBrokerConfig(ctx context.Context, run *amv1.AgentRun, values map[string][]byte) error {
	sec, err := builders.BuildBrokerConfigSecret(run, values)
	if err != nil {
		return err
	}
	if err := ctrl.SetControllerReference(run, sec, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.Secret{}
	getErr := r.Get(ctx, types.NamespacedName{Namespace: sec.Namespace, Name: sec.Name}, existing)
	if apierrors.IsNotFound(getErr) {
		return r.Create(ctx, sec)
	}
	return getErr
}

// ensureRunSpec creates (once) the ConfigMap the run pod mounts, carrying the
// Agent + AgentRunSpec the `agent run` entrypoint executes. Owned by the run so
// it is garbage-collected with it. The spec is immutable per run, so an existing
// ConfigMap is left as-is.
// hermesSessionAgent injects a stable Hermes provider session id when the run
// belongs to an AgentSession and the harness is Hermes — the cross-turn memory
// rail for HTTP harnesses (D6, M3.12): memory lives gateway-side keyed by a
// session id derived from the AgentSession UID (immutable, no cross-tenant
// bleed). It deep-copies so the stored Agent is never mutated; non-Hermes or
// non-session runs are returned unchanged.
func (r *AgentRunReconciler) hermesSessionAgent(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent) *amv1.Agent {
	if run.Spec.SessionRef == "" || agent.Spec.Harness == nil || agent.Spec.Harness.Kind != pure.HarnessHermes {
		return agent
	}
	var session amv1.AgentSession
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.SessionRef}, &session); err != nil {
		return agent // unresolvable session → no injection (run still works, just unscoped)
	}
	out := agent.DeepCopy()
	out.Spec.Harness.SessionPolicy = pure.SessionPersistent
	out.Spec.Harness.Env = append(out.Spec.Harness.Env, pure.HarnessEnvVar{
		Name:  "HERMES_SESSION_ID",
		Value: "sess-" + string(session.UID),
	})
	// Optional cross-turn memory partition (M3.12): scope (or share) gateway-side
	// memory beyond the per-session id.
	if session.Spec.MemoryScope != "" {
		out.Spec.Harness.Env = append(out.Spec.Harness.Env, pure.HarnessEnvVar{
			Name:  "HERMES_SESSION_KEY",
			Value: session.Spec.MemoryScope,
		})
	}
	return out
}

func (r *AgentRunReconciler) ensureRunSpec(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent, provider *builders.RunProvider) error {
	tools, err := r.resolveRunTools(ctx, agent)
	if err != nil {
		return err
	}
	cm, err := builders.BuildRunSpecConfigMapWithTools(run, r.hermesSessionAgent(ctx, run, agent), provider, tools)
	if err != nil {
		return err
	}
	if err := ctrl.SetControllerReference(run, cm, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.ConfigMap{}
	getErr := r.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, existing)
	if apierrors.IsNotFound(getErr) {
		return r.Create(ctx, cm)
	}
	return getErr
}

// resolveRunTools fetches the Agent's referenced Tool CRs into the pure catalog
// the executor loads from tools.json (M2.12). Without this the run pod ships no
// tool definitions and the executor rejects every tool call ("tool not found in
// catalog"). Loop mode only — harness agents drive their own tools. A missing
// Tool is skipped (the Agent reconciler already gates readiness on resolution),
// so a transient race can't strand the run.
func (r *AgentRunReconciler) resolveRunTools(ctx context.Context, agent *amv1.Agent) ([]pure.Tool, error) {
	if agent.Spec.Mode == pure.ModeHarness {
		return nil, nil
	}
	tools := make([]pure.Tool, 0, len(agent.Spec.Tools))
	for _, ref := range agent.Spec.Tools {
		ns := ref.Namespace
		if ns == "" {
			ns = agent.Namespace
		}
		t := &amv1.Tool{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: ref.Name}, t); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		tools = append(tools, pure.Tool{Name: t.Name, Spec: t.Spec})
	}
	return tools, nil
}

// foldRunResult parses the run container's termination message (the RunResult
// the `agent run` entrypoint emits) into AgentRun.Status: output, usage, and —
// when the runtime reports a more specific phase than the Pod (e.g. Expired on
// a budget cap, which still exits 0) — the phase itself. The runtime's own
// reason (e.g. "budget:tokens") is the most specific signal we have and wins
// over any pod-level reason markTerminal set; a runtime error wins outright.
func (r *AgentRunReconciler) foldRunResult(ctx context.Context, run *amv1.AgentRun, agent *amv1.Agent, pod *corev1.Pod) {
	rr, ok := runResultFromPod(pod)
	if !ok {
		return
	}
	// Apply any namespace RedactionPolicy to the cluster-facing record. This is
	// a disclosure control on Status only — the harness already observed the
	// raw data, so it is never containment (agentpolicy R1). rr.TerminationReason
	// is a controlled runtime signal (e.g. "budget:tokens") and stays
	// unredacted; rr.Error is the harness's own error text and is redacted when
	// the harness kind is CLI (subprocess env holds the provider credential —
	// not agent-blind — so an auth failure can echo it verbatim).
	pats := compileNamespaceRedaction(ctx, r.Client, run.Namespace, r.PlatformAgentPolicy)
	run.Status.Output = pure.RedactJSON(rr.Output, pats)
	run.Status.Steps = pure.RedactSteps(rr.Steps, pats)
	run.Status.Usage = rr.Usage
	run.Status.Trace = rr.Trace // compact trace metadata (M2.2)
	if rr.SessionID != "" {
		run.Status.HarnessSessionID = rr.SessionID // M3.19: surface for run-to-run resume
	}
	var kind pure.HarnessKind
	if agent != nil && agent.Spec.Harness != nil {
		kind = pure.CanonicalHarnessKind(agent.Spec.Harness.Kind)
	}
	switch {
	case rr.Error != "" && kind.IsCLI():
		run.Status.TerminationReason = pure.RedactString(rr.Error, pats)
	case rr.Error != "":
		run.Status.TerminationReason = rr.Error
	case rr.TerminationReason != "":
		run.Status.TerminationReason = rr.TerminationReason
	}
	if rr.Phase != "" && rr.Phase != run.Status.State {
		run.Status.State = rr.Phase
		readyStatus := metav1.ConditionFalse
		condReason := string(rr.Phase)
		if rr.Phase == pure.PhaseCompleted {
			readyStatus = metav1.ConditionTrue
		}
		setReadyCondition(&run.Status.Conditions, run.Generation, readyStatus, condReason, run.Status.TerminationReason)
		setProgressingCondition(&run.Status.Conditions, run.Generation, !rr.Phase.Terminal(), condReason, run.Status.TerminationReason)
	}
}

// runResultFromPod reads the RunResult JSON the run container wrote to its
// termination message. Matches the execution container (agent|harness), not the
// AgentFS / memory / secret-proxy sidecars.
func runResultFromPod(pod *corev1.Pod) (agentruntime.RunResult, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "agent" && cs.Name != "harness" {
			continue
		}
		if cs.State.Terminated == nil || cs.State.Terminated.Message == "" {
			continue
		}
		var rr agentruntime.RunResult
		if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &rr); err == nil {
			return rr, true
		}
	}
	return agentruntime.RunResult{}, false
}

// foldArtifacts surfaces the agentfs-sidecar's artifact-collection manifest
// (M2.26) into Status.Artifacts. The sidecar (a native sidecar = init container)
// records the manifest in its termination message, which is present by the time
// the pod is terminal — so the controller folds it from pod status without any
// new sidecar k8s client or RBAC. Observability-only: this never sets State.
// When egress was requested but the sidecar reported nothing, the result is
// recorded as Failed (the files may still be in S3 — this only reflects what
// the sidecar told us).
func (r *AgentRunReconciler) foldArtifacts(run *amv1.AgentRun, pod *corev1.Pod) {
	if !artifactsRequested(pod) {
		return
	}
	m, ok := artifactManifestFromPod(pod)
	if !ok {
		run.Status.Artifacts = &pure.ArtifactsStatus{State: pure.ArtifactStateFailed}
		return
	}
	out := &pure.ArtifactsStatus{State: m.State}
	for _, ref := range m.Refs {
		out.Refs = append(out.Refs, pure.ArtifactStatusRef{
			Name: ref.Name, Path: ref.Path, S3Key: ref.S3Key,
			SizeBytes: ref.SizeBytes, SHA256: ref.SHA256, Skipped: ref.Skipped,
		})
	}
	run.Status.Artifacts = out
}

// artifactsRequested reports whether the AgentFS serve sidecar was told to
// collect artifacts (AGENTFS_ARTIFACTS env), so an absent manifest can be
// distinguished from "egress was never configured".
func artifactsRequested(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.Name != builders.StorageFSSidecarName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == "AGENTFS_ARTIFACTS" && e.Value != "" {
				return true
			}
		}
	}
	return false
}

// artifactManifestFromPod reads the agentfs-sidecar's termination message (the
// collection manifest). The sidecar is a native sidecar, so its status is in
// InitContainerStatuses.
func artifactManifestFromPod(pod *corev1.Pod) (agentfs.ArtifactManifest, bool) {
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name != builders.StorageFSSidecarName {
			continue
		}
		if cs.State.Terminated == nil || cs.State.Terminated.Message == "" {
			return agentfs.ArtifactManifest{}, false
		}
		var m agentfs.ArtifactManifest
		if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &m); err == nil {
			return m, true
		}
	}
	return agentfs.ArtifactManifest{}, false
}
