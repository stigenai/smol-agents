package agentmodel

import (
	"context"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
)

const (
	// TeamMemberLabel marks an AgentRun/AgentSession as a team member's worker,
	// set by the coordinator when it spawns the member (from P1). P0 reads it to
	// map an owned run/session back to its TeamMemberSpec by name. Canonical in
	// the builders package (the run-pod builder reads it too).
	TeamMemberLabel = amv1.TeamMemberLabel
	// TeamLabel names the owning AgentTeam (set alongside the OwnerReference).
	TeamLabel = amv1.TeamLabel
)

// AgentTeamReconciler maintains the governance envelope for an AgentTeam (P0):
// it self-validates the team (fail-closed), rolls member run/session usage up
// FIELD-WISE into status (observability — the team Budget is the ceiling but cost
// never gates), and the team is the OwnerReference GC root for its members so
// deleting it GCs the whole subtree. Coordination — the shared task list, peer
// mailbox, convergence controllers — is layered on in later phases; P0 stands up
// the governed envelope and the usage roll-up over the owned subtree.
type AgentTeamReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	MaxConcurrentReconciles int
}

// AgentTeamOwnerRef is the controller OwnerReference a coordinator sets on each
// member run/session so the team GCs its subtree on deletion (the team-scale
// generalization of the A2A per-run ownerRef GC). UID is the team's own uid as a
// literal — NOT a downward-API metadata.uid, which would resolve to the wrong
// object (the A2A child-GC bug).
func AgentTeamOwnerRef(team *amv1.AgentTeam) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         amv1.GroupVersion.String(),
		Kind:               "AgentTeam",
		Name:               team.Name,
		UID:                team.UID,
		Controller:         ptrBool(true),
		BlockOwnerDeletion: ptrBool(true),
	}
}

func ptrBool(b bool) *bool { return &b }

func (r *AgentTeamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var team amv1.AgentTeam
	if err := r.Get(ctx, req.NamespacedName, &team); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Fail-closed self-validation — the reconcile backstop for a team that
	// bypassed (or pre-dates) the admission webhook.
	if err := pure.ValidateAgentTeam(pure.AgentTeam{Name: team.Name, Spec: team.Spec}); err != nil {
		return r.apply(ctx, &team, pure.AgentTeamStatus{
			Phase: pure.PhaseFailed, Reason: "InvalidSpec", Message: err.Error(),
		})
	}

	usages, members, anyRunning, err := r.gatherMembers(ctx, &team)
	if err != nil {
		return ctrl.Result{}, err
	}

	phase := pure.PhasePending
	if anyRunning {
		phase = pure.PhaseRunning
	}
	// P0 never declares the team Completed — that is a coordinator decision in a
	// later phase. It tracks liveness (Pending/Running) + the usage roll-up.
	return r.apply(ctx, &team, pure.AgentTeamStatus{
		Phase:           phase,
		Reason:          "Reconciled",
		Members:         members,
		CumulativeUsage: pure.RollUpTeamUsage(usages),
	})
}

// gatherMembers finds the team's members = AgentRuns + AgentSessions
// OwnerReferenced by this team (the GC subtree), returns their usages (for the
// field-wise team roll-up), a per-declared-member status keyed by the
// team-member label, and whether any member is Running.
func (r *AgentTeamReconciler) gatherMembers(ctx context.Context, team *amv1.AgentTeam) ([]pure.Usage, []pure.TeamMemberStatus, bool, error) {
	byName := make(map[string]*pure.TeamMemberStatus, len(team.Spec.Members))
	order := make([]string, 0, len(team.Spec.Members))
	for _, m := range team.Spec.Members {
		byName[m.Name] = &pure.TeamMemberStatus{Name: m.Name}
		order = append(order, m.Name)
	}

	var usages []pure.Usage
	anyRunning := false

	var runs amv1.AgentRunList
	if err := r.List(ctx, &runs, client.InNamespace(team.Namespace)); err != nil {
		return nil, nil, false, err
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if !ownedByTeam(run, team) {
			continue
		}
		usages = append(usages, run.Status.Usage)
		if run.Status.State == pure.PhaseRunning {
			anyRunning = true
		}
		if ms, ok := byName[run.Labels[TeamMemberLabel]]; ok {
			ms.Phase = run.Status.State
			ms.Usage = addUsage(ms.Usage, run.Status.Usage)
		}
	}

	var sessions amv1.AgentSessionList
	if err := r.List(ctx, &sessions, client.InNamespace(team.Namespace)); err != nil {
		return nil, nil, false, err
	}
	for i := range sessions.Items {
		sess := &sessions.Items[i]
		if !ownedByTeam(sess, team) {
			continue
		}
		usages = append(usages, sess.Status.Usage)
		if sess.Status.Phase == pure.PhaseRunning {
			anyRunning = true
		}
		if ms, ok := byName[sess.Labels[TeamMemberLabel]]; ok {
			ms.Phase = sess.Status.Phase
			ms.Usage = addUsage(ms.Usage, sess.Status.Usage)
		}
	}

	members := make([]pure.TeamMemberStatus, 0, len(order))
	for _, n := range order {
		members = append(members, *byName[n])
	}
	return usages, members, anyRunning, nil
}

// apply writes the computed status, setting LastActivity when the usage roll-up
// changed, and skips the write when nothing material changed (avoids status
// churn / requeue loops).
func (r *AgentTeamReconciler) apply(ctx context.Context, team *amv1.AgentTeam, desired pure.AgentTeamStatus) (ctrl.Result, error) {
	desired.ObservedGeneration = team.Generation
	desired.LastActivity = team.Status.LastActivity
	if desired.CumulativeUsage != team.Status.CumulativeUsage {
		now := metav1.Now()
		desired.LastActivity = &now
	}

	desired.Conditions = team.Status.Conditions
	ready := metav1.ConditionFalse
	if desired.Phase == pure.PhaseRunning {
		ready = metav1.ConditionTrue
	}
	setReadyCondition(&desired.Conditions, team.Generation, ready, desired.Reason, desired.Message)
	setProgressingCondition(&desired.Conditions, team.Generation, desired.Phase == pure.PhasePending, desired.Reason, desired.Message)

	if statusEqual(team.Status, desired) {
		return ctrl.Result{}, nil
	}
	team.Status = desired
	if err := r.Status().Update(ctx, team); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

// statusEqual compares the material status fields (LastActivity is derived from
// CumulativeUsage, so it is excluded to keep the comparison stable).
func statusEqual(a, b pure.AgentTeamStatus) bool {
	a.LastActivity, b.LastActivity = nil, nil
	return reflect.DeepEqual(a, b)
}

func ownedByTeam(obj metav1.Object, team *amv1.AgentTeam) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == team.UID && ref.Kind == "AgentTeam" {
			return true
		}
	}
	return false
}

// addUsage folds b into a FIELD-WISE (never Usage.Add); WallClock is not summed
// (members run concurrently), matching pure.RollUpTeamUsage.
func addUsage(a, b pure.Usage) pure.Usage {
	a.Steps += b.Steps
	a.Tokens += b.Tokens
	a.ToolCalls += b.ToolCalls
	a.CostUSDMilli += b.CostUSDMilli
	return a
}

func (r *AgentTeamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mc := r.MaxConcurrentReconciles
	if mc < 1 {
		mc = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&amv1.AgentTeam{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Re-reconcile a team when one of its owned members' usage/phase changes,
		// so the roll-up tracks the subtree.
		Owns(&amv1.AgentRun{}).
		Owns(&amv1.AgentSession{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mc}).
		Complete(r)
}
