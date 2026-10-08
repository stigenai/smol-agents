package v1

import (
	"encoding/json"
	"errors"
	"fmt"
)

// AgentWorkflow is a declarative, result-routed DAG of agent steps (LangGraph
// StateGraph): nodes name a same-namespace Agent, static edges wire them, and
// conditional edges route on the prior node's output via an operator-evaluated,
// side-effect-free predicate (never an LLM, never a child agent picking its own
// next hop — fail-closed, D3). It complements AgentTeam (LLM-lead runtime
// routing) with a static, reproducible control flow.
//
// Namespaced; every nodeRef is a bare same-namespace Agent (D1). The operator
// materializes each ready node as a child AgentRun (A2A spawn shape: ownerRef
// subtree GC, depth budget) and advances on terminal output.
//
// For when to use a static-DAG workflow vs the other execution surfaces, see
// docs/design/workload-selection.md.
type AgentWorkflow struct {
	Name   string              `json:"name"`
	Spec   AgentWorkflowSpec   `json:"spec"`
	Status AgentWorkflowStatus `json:"status,omitempty"`
}

const (
	// WorkflowStart / WorkflowEnd are the reserved entry/exit node names.
	WorkflowStart = "START"
	WorkflowEnd   = "END"
)

type AgentWorkflowSpec struct {
	// Paused marks this workflow a dormant TEMPLATE: the operator does not
	// materialize its nodes, so it can be referenced as an event target
	// (EventBinding) without running standalone. Each inbound event clones it into
	// an un-paused per-event instance (BuildWorkflowInstance) that runs. Mirrors
	// CronJob.spec.suspend. +optional
	Paused bool `json:"paused,omitempty"`
	// Nodes are the workflow steps (at least one). Names are unique; START/END
	// are reserved and not declared as nodes.
	Nodes []WorkflowNode `json:"nodes"`
	// Edges wire nodes. An edge with a When predicate is conditional (taken only
	// when the predicate holds against the from-node's output); edges without one
	// are unconditional.
	Edges []WorkflowEdge `json:"edges"`

	// ResultSink, when set, is a URI the operator POSTs a CloudEvent to when this
	// workflow completes (wbb) — the final node's output, so a workflow's result
	// can drive downstream Knative Triggers. A per-event instance inherits it from
	// its template. At-least-once, stable ce-id = workflow UID. Empty = no
	// emission. +optional
	ResultSink string `json:"resultSink,omitempty"`
}

// WorkflowNode is one DAG step.
type WorkflowNode struct {
	// Name is the node's unique handle (not START/END).
	Name string `json:"name"`
	// AgentRef is the Agent this node runs — a bare name in the workflow's
	// namespace (no cross-namespace reference, D1).
	AgentRef string `json:"agentRef"`
	// Input is the node's run input (a static object; templating over the shared
	// blackboard is a later enhancement). +optional
	Input json.RawMessage `json:"input,omitempty"`
}

// WorkflowEdge wires from → to, optionally conditional on When.
type WorkflowEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	// When, when set, is a routing predicate evaluated operator-side against the
	// from-node's status.output (e.g. "score >= 80"). Empty = unconditional.
	// +optional
	When string `json:"when,omitempty"`
}

type AgentWorkflowStatus struct {
	// Phase is the workflow lifecycle (Pending/Running/Completed/Failed).
	Phase Phase `json:"phase,omitempty"`
	// Reason / Message explain the current phase. +optional
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	// NodeStates mirrors each node's child run. +optional
	NodeStates []NodeState `json:"nodeStates,omitempty"`
	// CumulativeUsage rolls node usage up field-wise (never Usage.Add). +optional
	CumulativeUsage    Usage `json:"cumulativeUsage,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// NodeState is the operator-observed state of one node.
type NodeState struct {
	Name    string `json:"name"`
	Phase   Phase  `json:"phase,omitempty"`
	RunName string `json:"runName,omitempty"`
}

// ValidateAgentWorkflow checks self-consistency (admission-time, no cluster
// lookups): ≥1 node with unique non-reserved names + bare same-namespace
// agentRefs; every edge endpoint exists (a node or START/END); a START edge
// exists; every conditional predicate compiles; and the graph is acyclic (a DAG).
func ValidateAgentWorkflow(w AgentWorkflow) error {
	var errs []error
	if len(w.Spec.Nodes) == 0 {
		errs = append(errs, errors.New("spec.nodes must list at least one node"))
	}
	names := map[string]bool{WorkflowStart: true, WorkflowEnd: true}
	for i, n := range w.Spec.Nodes {
		switch {
		case n.Name == "":
			errs = append(errs, fmt.Errorf("spec.nodes[%d].name is required", i))
		case n.Name == WorkflowStart || n.Name == WorkflowEnd:
			errs = append(errs, fmt.Errorf("spec.nodes[%d].name %q is reserved", i, n.Name))
		case names[n.Name]:
			errs = append(errs, fmt.Errorf("spec.nodes[%d].name %q is duplicated", i, n.Name))
		default:
			names[n.Name] = true
		}
		if n.AgentRef == "" {
			errs = append(errs, fmt.Errorf("spec.nodes[%d].agentRef is required", i))
		} else if containsSlash(n.AgentRef) {
			errs = append(errs, fmt.Errorf("spec.nodes[%d].agentRef must be a bare name in this namespace (no cross-namespace reference)", i))
		}
	}
	hasStartEdge := false
	for i, e := range w.Spec.Edges {
		if e.From == "" || e.To == "" {
			errs = append(errs, fmt.Errorf("spec.edges[%d] needs both from and to", i))
			continue
		}
		if !names[e.From] {
			errs = append(errs, fmt.Errorf("spec.edges[%d].from %q is not a node", i, e.From))
		}
		if !names[e.To] {
			errs = append(errs, fmt.Errorf("spec.edges[%d].to %q is not a node", i, e.To))
		}
		if e.From == WorkflowStart {
			hasStartEdge = true
		}
		if e.When != "" {
			if _, err := CompilePredicate(e.When); err != nil {
				errs = append(errs, fmt.Errorf("spec.edges[%d].when: %w", i, err))
			}
		}
	}
	if len(w.Spec.Edges) > 0 && !hasStartEdge {
		errs = append(errs, errors.New("no edge originates from START (the workflow has no entry point)"))
	}
	if err := checkAcyclic(w.Spec.Edges); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// checkAcyclic reports an error if the edge set contains a cycle (DFS with a
// recursion stack). START/END are ordinary vertices here.
func checkAcyclic(edges []WorkflowEdge) error {
	adj := map[string][]string{}
	for _, e := range edges {
		adj[e.From] = append(adj[e.From], e.To)
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var visit func(string) bool
	visit = func(n string) bool {
		color[n] = gray
		for _, m := range adj[n] {
			if color[m] == gray {
				return true // back-edge → cycle
			}
			if color[m] == white && visit(m) {
				return true
			}
		}
		color[n] = black
		return false
	}
	for n := range adj {
		if color[n] == white && visit(n) {
			return fmt.Errorf("spec.edges form a cycle (a workflow must be a DAG)")
		}
	}
	return nil
}
