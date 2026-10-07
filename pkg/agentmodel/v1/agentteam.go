package v1

import (
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentTeam is the durable record + governance envelope for a team of agents
// collaborating on a goal (multi-agent orchestration — successor to the shipped
// A2A orchestrator→subagent edge). It names a lead (coordinator) agent, a set of
// members, the coordination pattern, and a team-wide budget ceiling.
//
// P0 is the GOVERNED ENVELOPE only: the operator validates the team, rolls
// member usage up field-wise into status, and owns the members via an
// OwnerReference subtree so deleting the team GCs the whole tree (the team-scale
// generalization of A2A's per-run subtree GC). Coordination — the shared task
// list, peer mailbox, and convergence controllers — lands in later phases.
//
// Namespaced; the lead and every member reference Agents in the SAME namespace
// (D1 — no cross-tenant team without an explicit, policy-gated grant).
type AgentTeam struct {
	Name   string          `json:"name"`
	Spec   AgentTeamSpec   `json:"spec"`
	Status AgentTeamStatus `json:"status,omitempty"`
}

// TeamPattern is the coordination shape the team uses (the five industry
// patterns). Only the envelope is wired in P0; the pattern selects which
// coordination controller drives the team in later phases.
type TeamPattern string

const (
	// TeamPatternOrchestrator — a lead delegates bounded subtasks to members and
	// synthesizes (the shipped A2A orchestrator→subagent edge, widened).
	TeamPatternOrchestrator TeamPattern = "orchestrator"
	// TeamPatternGeneratorVerifier — a generator/verifier loop with explicit
	// convergence criteria (P3).
	TeamPatternGeneratorVerifier TeamPattern = "generator-verifier"
	// TeamPatternTeam — members claim from a shared task list (P1) and message
	// peers (P2); the coordinator collects.
	TeamPatternTeam TeamPattern = "team"
	// TeamPatternBus — members publish/subscribe team subjects; emergent workflow (P5).
	TeamPatternBus TeamPattern = "bus"
	// TeamPatternSharedState — members read/write a shared AgentFS blackboard (P4).
	TeamPatternSharedState TeamPattern = "shared-state"
)

type AgentTeamSpec struct {
	// Lead is the coordinator Agent (loop mode) in THIS namespace — a bare name;
	// a cross-namespace reference is rejected (D1).
	Lead string `json:"lead"`

	// Members are the team's worker agents (at least one required).
	Members []TeamMemberSpec `json:"members"`

	// Pattern is the coordination shape (default orchestrator).
	// +kubebuilder:validation:Enum=orchestrator;generator-verifier;team;bus;shared-state
	// +optional
	Pattern TeamPattern `json:"pattern,omitempty"`

	// Budget is the TEAM-WIDE resource ceiling. Member usage rolls up field-wise
	// into status; a team cannot out-spend this ceiling (surfaced in status from
	// P0, enforced by the coordinator from P3). Cost stays milli-USD
	// observability-only and never gates. nil = no team ceiling (members keep
	// their own per-agent budgets).
	// +optional
	Budget *Budget `json:"budget,omitempty"`

	// MaxMembers caps concurrent team width — the width analog of the A2A MaxDepth
	// recursion guard. 0 → default (the number of declared members).
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxMembers int32 `json:"maxMembers,omitempty"`

	// Convergence bounds an iterative pattern so the team terminates instead of
	// looping forever. MANDATORY for generator-verifier / bus / shared-state
	// (their stop condition is not otherwise defined); ignored for orchestrator /
	// team. The team Budget + pod deadline are the hard backstops beneath it.
	// +optional
	Convergence *ConvergenceSpec `json:"convergence,omitempty"`

	// SharedWorkspace mounts one shared AgentFS volume across members (the
	// blackboard / shared-state pattern), distinct from each member's private
	// workspace. nil = members keep only their private workspaces.
	// +optional
	SharedWorkspace *SharedWorkspaceSpec `json:"sharedWorkspace,omitempty"`

	// Hooks attach fail-closed gates to team-lifecycle events; the coordinator
	// consults them via team.EvaluateHooks (pkg/turnmodel/team). A TaskCreated
	// veto refuses a turn before any member work runs; a TaskCompleted veto
	// rejects an otherwise-accepted result. An absent hook = allow — hooks only
	// ever tighten. +optional
	Hooks []TeamHookSpec `json:"hooks,omitempty"`
}

// WorkspaceConflictMode selects how concurrent writes to the shared workspace
// are reconciled (P4).
type WorkspaceConflictMode string

const (
	// ConflictSharedRW is the simple default: one read/write volume; members
	// coordinate writes themselves (or partition by path).
	ConflictSharedRW WorkspaceConflictMode = "shared-rw"
	// ConflictBranchMerge is the strong mode: each member works a branch and the
	// coordinator 3-way-merges branches at task completion — turning the
	// "two teammates, one file" overwrite risk into an enforced merge.
	ConflictBranchMerge WorkspaceConflictMode = "branch-merge"
)

// SharedWorkspaceSpec is one shared AgentFS volume for a team (P4).
type SharedWorkspaceSpec struct {
	// SizeGiB is the shared volume size.
	// +kubebuilder:validation:Minimum=1
	SizeGiB int32 `json:"sizeGiB"`
	// MountPath where every member sees the shared root (default /var/agentfs-team).
	// +optional
	MountPath string `json:"mountPath,omitempty"`
	// ConflictMode is shared-rw (default) or branch-merge.
	// +kubebuilder:validation:Enum=shared-rw;branch-merge
	// +optional
	ConflictMode WorkspaceConflictMode `json:"conflictMode,omitempty"`
}

// EffectiveConflictMode defaults to shared-rw.
func (s SharedWorkspaceSpec) EffectiveConflictMode() WorkspaceConflictMode {
	if s.ConflictMode == "" {
		return ConflictSharedRW
	}
	return s.ConflictMode
}

// ConvergenceSpec bounds an iterative coordination loop (P3). Termination is the
// blog's #1 multi-agent failure mode ("cycle indefinitely"), so for the patterns
// without an intrinsic stop it is required, not advisory.
type ConvergenceSpec struct {
	// MaxIterations caps generator→verifier rounds (must be ≥ 1).
	// +kubebuilder:validation:Minimum=1
	MaxIterations int32 `json:"maxIterations"`
	// Criteria is the verifier's acceptance standard. Required + non-empty: a
	// verifier is only as good as its criteria, so this is a first-class field,
	// not free-form prompt text.
	Criteria string `json:"criteria"`
	// TimeBudgetSeconds bounds total wall-clock for the loop (0 → the team
	// Budget's wall-clock governs).
	// +kubebuilder:validation:Minimum=0
	// +optional
	TimeBudgetSeconds int32 `json:"timeBudgetSeconds,omitempty"`
}

// requiresConvergence reports whether a pattern must declare a ConvergenceSpec
// (it has no intrinsic stop condition). Orchestrator + team converge on their
// own (the task list drains / the lead synthesizes).
func requiresConvergence(p TeamPattern) bool {
	switch p {
	case TeamPatternGeneratorVerifier, TeamPatternBus, TeamPatternSharedState:
		return true
	}
	return false
}

// TeamHookEvent is a gated team-lifecycle event — the platform analog of Claude
// Code's TeammateIdle / TaskCreated / TaskCompleted hooks. The coordinator
// consults the team's hooks at these points (the pure evaluation is
// team.EvaluateHooks). The string values match that package's constants 1:1.
// +kubebuilder:validation:Enum=TeammateIdle;TaskCreated;TaskCompleted
type TeamHookEvent string

const (
	TeamHookTeammateIdle  TeamHookEvent = "TeammateIdle"
	TeamHookTaskCreated   TeamHookEvent = "TaskCreated"
	TeamHookTaskCompleted TeamHookEvent = "TaskCompleted"
)

// HookAction is a hook's verdict on its event: allow proceeds, veto refuses
// fail-closed, requeue defers an idle member. A hook only ever tightens.
// +kubebuilder:validation:Enum=allow;veto;requeue
type HookAction string

const (
	HookActionAllow   HookAction = "allow"
	HookActionVeto    HookAction = "veto"
	HookActionRequeue HookAction = "requeue"
)

// TeamHookSpec attaches one fail-closed gate to a team-lifecycle event: when
// Event fires, Action (veto/requeue) tightens behavior — an absent hook = allow.
// Converted to the pure team.TeamHook by team.TeamHooksFromSpec (the CRD type and
// the domain type are split to keep this package free of any dependency on
// pkg/turnmodel/team — the import only runs the other way).
type TeamHookSpec struct {
	// Event is the team-lifecycle event this hook gates.
	Event TeamHookEvent `json:"event"`
	// Action is the verdict when the event fires (allow|veto|requeue).
	Action HookAction `json:"action"`
	// Reason is an optional human-readable explanation surfaced when the hook
	// fires. +optional
	Reason string `json:"reason,omitempty"`
}

// TeamMemberSpec names one member of a team.
type TeamMemberSpec struct {
	// Name is the member's stable handle within the team (used for addressing in
	// the P2 mailbox). Unique within the team.
	Name string `json:"name"`
	// AgentRef is the Agent backing this member — a bare name in the team's
	// namespace (no cross-namespace reference, D1).
	AgentRef string `json:"agentRef"`
	// Role is an optional free-text role label (e.g. "researcher", "critic").
	// +optional
	Role string `json:"role,omitempty"`
	// MaxConcurrent caps how many runs this member may have in flight (0 → 1).
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrent int32 `json:"maxConcurrent,omitempty"`
}

type AgentTeamStatus struct {
	// Phase is the team lifecycle as observed by the operator: Pending (forming /
	// validating), Running (members active), Completed, or Failed (invalid spec).
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// Reason is a short machine-readable cause for the current Phase (e.g.
	// InvalidSpec, Reconciled). +optional
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable elaboration of Reason. +optional
	Message string `json:"message,omitempty"`

	// Members mirrors each member's observed phase + usage contribution. +optional
	Members []TeamMemberStatus `json:"members,omitempty"`

	// CumulativeUsage is the team-wide resource accounting, rolled up FIELD-WISE
	// from member runs/sessions (never via Usage.Add). Observability only — the
	// team Budget is the ceiling but cost never gates. +optional
	CumulativeUsage Usage `json:"cumulativeUsage,omitempty"`

	// ObservedGeneration is the spec generation last reconciled. +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// LastActivity is when a member last reported progress (a usage change). +optional
	LastActivity *metav1.Time `json:"lastActivity,omitempty"`

	// Conditions follows the standard Kubernetes condition pattern (Ready /
	// Progressing), enabling `kubectl wait --for=condition=Ready` and
	// Argo/Flux health assessment. Phase/Reason/Message remain the
	// human-readable summary; Conditions is additive. +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// TeamMemberStatus is the operator-observed state of one member.
type TeamMemberStatus struct {
	Name  string `json:"name"`
	Phase Phase  `json:"phase,omitempty"`
	// Usage is this member's contribution to the team roll-up (field-wise). +optional
	Usage Usage `json:"usage,omitempty"`
}

// EffectiveMaxMembers is the team width cap (default = the number of declared
// members when maxMembers is unset).
func (s AgentTeamSpec) EffectiveMaxMembers() int32 {
	if s.MaxMembers > 0 {
		return s.MaxMembers
	}
	return int32(len(s.Members))
}

// EffectivePattern defaults to orchestrator.
func (s AgentTeamSpec) EffectivePattern() TeamPattern {
	if s.Pattern == "" {
		return TeamPatternOrchestrator
	}
	return s.Pattern
}

// RollUpTeamUsage sums member usages FIELD-WISE into a team total — the
// team-scale generalization of the A2A child-usage fold. It never calls
// Usage.Add (which increments Steps per call, double-counting). WallClockUsed is
// deliberately NOT summed: members run concurrently, so the team's elapsed time
// is its own (set by the controller), not the sum of member wall-clocks.
// Observability only; cost never gates.
func RollUpTeamUsage(members []Usage) Usage {
	var total Usage
	for _, u := range members {
		total.Steps += u.Steps
		total.Tokens += u.Tokens
		total.ToolCalls += u.ToolCalls
		total.CostUSDMilli += u.CostUSDMilli
	}
	return total
}

// ValidateAgentTeam checks an AgentTeam's self-consistency (admission-time, no
// cluster lookups): a lead, ≥1 member with unique names + same-namespace bare
// agentRefs, a known pattern, a valid team budget (if set), and maxMembers ≥ 0.
func ValidateAgentTeam(t AgentTeam) error {
	var errs []error
	if t.Spec.Lead == "" {
		errs = append(errs, errors.New("spec.lead is required"))
	} else if containsSlash(t.Spec.Lead) {
		errs = append(errs, errors.New("spec.lead must be a bare name in this namespace (no cross-namespace reference)"))
	}
	if len(t.Spec.Members) == 0 {
		errs = append(errs, errors.New("spec.members must list at least one member"))
	}
	seen := make(map[string]bool, len(t.Spec.Members))
	for i, m := range t.Spec.Members {
		switch {
		case m.Name == "":
			errs = append(errs, fmt.Errorf("spec.members[%d].name is required", i))
		case seen[m.Name]:
			errs = append(errs, fmt.Errorf("spec.members[%d].name %q is duplicated", i, m.Name))
		default:
			seen[m.Name] = true
		}
		if m.AgentRef == "" {
			errs = append(errs, fmt.Errorf("spec.members[%d].agentRef is required", i))
		} else if containsSlash(m.AgentRef) {
			errs = append(errs, fmt.Errorf("spec.members[%d].agentRef must be a bare name in this namespace (no cross-namespace reference)", i))
		}
		if m.MaxConcurrent < 0 {
			errs = append(errs, fmt.Errorf("spec.members[%d].maxConcurrent must be ≥ 0", i))
		}
	}
	switch t.Spec.Pattern {
	case "", TeamPatternOrchestrator, TeamPatternGeneratorVerifier, TeamPatternTeam, TeamPatternBus, TeamPatternSharedState:
	default:
		errs = append(errs, fmt.Errorf("spec.pattern %q is not a known coordination pattern", t.Spec.Pattern))
	}
	if t.Spec.MaxMembers < 0 {
		errs = append(errs, errors.New("spec.maxMembers must be ≥ 0"))
	}
	if t.Spec.Budget != nil {
		if err := t.Spec.Budget.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	// Convergence is mandatory for the patterns without an intrinsic stop, and
	// whenever present must be well-formed (so a team never loops forever).
	switch {
	case t.Spec.Convergence != nil:
		if t.Spec.Convergence.MaxIterations < 1 {
			errs = append(errs, errors.New("spec.convergence.maxIterations must be ≥ 1"))
		}
		if t.Spec.Convergence.Criteria == "" {
			errs = append(errs, errors.New("spec.convergence.criteria is required (the verifier's acceptance standard)"))
		}
		if t.Spec.Convergence.TimeBudgetSeconds < 0 {
			errs = append(errs, errors.New("spec.convergence.timeBudgetSeconds must be ≥ 0"))
		}
	case requiresConvergence(t.Spec.EffectivePattern()):
		errs = append(errs, fmt.Errorf("spec.convergence is required for pattern %q (it has no intrinsic stop condition)", t.Spec.EffectivePattern()))
	}
	if w := t.Spec.SharedWorkspace; w != nil {
		if w.SizeGiB < 1 {
			errs = append(errs, errors.New("spec.sharedWorkspace.sizeGiB must be ≥ 1"))
		}
		switch w.ConflictMode {
		case "", ConflictSharedRW, ConflictBranchMerge:
		default:
			errs = append(errs, fmt.Errorf("spec.sharedWorkspace.conflictMode %q must be shared-rw or branch-merge", w.ConflictMode))
		}
	}
	// Hooks gate team-lifecycle events; each must name a known event + action so a
	// typo fails admission rather than silently never firing (fail-closed).
	for i, h := range t.Spec.Hooks {
		switch h.Event {
		case TeamHookTeammateIdle, TeamHookTaskCreated, TeamHookTaskCompleted:
		default:
			errs = append(errs, fmt.Errorf("spec.hooks[%d].event %q is not a known team-lifecycle event", i, h.Event))
		}
		switch h.Action {
		case HookActionAllow, HookActionVeto, HookActionRequeue:
		default:
			errs = append(errs, fmt.Errorf("spec.hooks[%d].action %q must be allow, veto, or requeue", i, h.Action))
		}
	}
	return errors.Join(errs...)
}
