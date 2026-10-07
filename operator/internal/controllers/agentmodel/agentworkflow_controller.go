package agentmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
	"github.com/smol-platform/smol-agents/pkg/eventsink"
)

// WorkflowNodeLabel ties a child AgentRun to the workflow node it runs.
const WorkflowNodeLabel = "runtime.agents.smol-agents.ai/workflow-node"

// AgentWorkflowReconciler walks an AgentWorkflow DAG (iru.4.5): it materializes
// each ready node as a child AgentRun (ownerRef to the workflow for subtree GC),
// routes on each node's terminal output via the operator-evaluated predicate DSL
// (never an LLM — D3), rolls node usage up field-wise, and completes when a
// satisfied edge reaches END (or fails closed when a node fails).
type AgentWorkflowReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	MaxConcurrentReconciles int
	// ResultSinkClient POSTs result CloudEvents to a workflow's spec.resultSink
	// (wbb). nil → a default 5s-timeout client.
	ResultSinkClient *http.Client
}

func workflowOwnerRef(wf *amv1.AgentWorkflow) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         amv1.GroupVersion.String(),
		Kind:               "AgentWorkflow",
		Name:               wf.Name,
		UID:                wf.UID,
		Controller:         ptrBool(true),
		BlockOwnerDeletion: ptrBool(true),
	}
}

func (r *AgentWorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var wf amv1.AgentWorkflow
	if err := r.Get(ctx, req.NamespacedName, &wf); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := pure.ValidateAgentWorkflow(pure.AgentWorkflow{Name: wf.Name, Spec: wf.Spec}); err != nil {
		return r.applyWF(ctx, &wf, pure.AgentWorkflowStatus{Phase: pure.PhaseFailed, Reason: "InvalidSpec", Message: err.Error()}, ctrl.Result{})
	}

	// A paused workflow is a dormant event template (v9h): never materialize its
	// nodes — each inbound event clones it into an un-paused per-event instance
	// (BuildWorkflowInstance) that runs instead.
	if wf.Spec.Paused {
		return r.applyWF(ctx, &wf, pure.AgentWorkflowStatus{Phase: pure.PhasePending, Reason: "Template", Message: "paused workflow is an event template; per-event instances run instead"}, ctrl.Result{})
	}

	// Gather the workflow's child runs, keyed by node.
	var runs amv1.AgentRunList
	if err := r.List(ctx, &runs, client.InNamespace(wf.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	childByNode := map[string]*amv1.AgentRun{}
	for i := range runs.Items {
		run := &runs.Items[i]
		if ownedByWorkflow(run, &wf) {
			if node := run.Labels[WorkflowNodeLabel]; node != "" {
				childByNode[node] = run
			}
		}
	}

	// satisfied reports whether edge e is "taken": from START (entry), or the
	// from-node completed and the predicate (if any) holds against its output.
	satisfied := func(e pure.WorkflowEdge) bool {
		if e.From == pure.WorkflowStart {
			return true
		}
		c := childByNode[e.From]
		if c == nil || c.Status.State != pure.PhaseCompleted {
			return false
		}
		if e.When == "" {
			return true
		}
		p, err := pure.CompilePredicate(e.When)
		if err != nil {
			return false // validated at admission; fail-closed if it somehow doesn't compile
		}
		ok, _ := p.Eval(c.Status.Output)
		return ok
	}
	activated := func(node string) bool {
		for _, e := range wf.Spec.Edges {
			if e.To == node && satisfied(e) {
				return true
			}
		}
		return false
	}

	// Materialize each activated node that has no child yet.
	for _, node := range wf.Spec.Nodes {
		if childByNode[node.Name] != nil || !activated(node.Name) {
			continue
		}
		child := &amv1.AgentRun{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName:    wf.Name + "-" + node.Name + "-",
				Namespace:       wf.Namespace,
				Labels:          map[string]string{WorkflowNodeLabel: node.Name},
				OwnerReferences: []metav1.OwnerReference{workflowOwnerRef(&wf)},
			},
			Spec: pure.AgentRunSpec{AgentRef: node.AgentRef, Input: node.Input},
		}
		if err := r.Create(ctx, child); err != nil {
			return ctrl.Result{}, err
		}
		childByNode[node.Name] = child
	}

	// Build status: per-node state, field-wise usage, phase.
	failed := false
	var usages []pure.Usage
	nodeStates := make([]pure.NodeState, 0, len(wf.Spec.Nodes))
	for _, node := range wf.Spec.Nodes {
		st := pure.NodeState{Name: node.Name}
		if c := childByNode[node.Name]; c != nil {
			st.Phase, st.RunName = c.Status.State, c.Name
			usages = append(usages, c.Status.Usage)
			if c.Status.State.Terminal() && c.Status.State != pure.PhaseCompleted {
				failed = true
			}
		}
		nodeStates = append(nodeStates, st)
	}
	endReached := false
	var finalOutput json.RawMessage
	for _, e := range wf.Spec.Edges {
		if e.To == pure.WorkflowEnd && satisfied(e) {
			endReached = true
			if c := childByNode[e.From]; c != nil {
				finalOutput = c.Status.Output // the workflow's result = the END-reaching node's output
			}
			break
		}
	}

	phase, reason := pure.PhaseRunning, "Running"
	res := ctrl.Result{RequeueAfter: 10 * time.Second}
	switch {
	case failed:
		phase, reason, res = pure.PhaseFailed, "NodeFailed", ctrl.Result{}
	case endReached:
		phase, reason, res = pure.PhaseCompleted, "ReachedEnd", ctrl.Result{}
	case len(childByNode) == 0:
		phase, reason = pure.PhasePending, "Forming"
	}
	if phase == pure.PhaseCompleted {
		// wbb: emit a workflow.completed CloudEvent to the workflow's sink, once
		// (annotation-guarded — a Completed workflow re-reconciles on child events).
		r.emitWorkflowResultOnce(ctx, &wf, finalOutput)
	}
	return r.applyWF(ctx, &wf, pure.AgentWorkflowStatus{
		Phase: phase, Reason: reason, NodeStates: nodeStates,
		CumulativeUsage: pure.RollUpTeamUsage(usages),
	}, res)
}

// emitWorkflowResultOnce POSTs a com.smol-agents.workflow.completed CloudEvent to
// the workflow's spec.resultSink when it first completes (wbb), carrying the final
// node's output. Once-guard + bounded emit live in the shared emitResultEventOnce.
func (r *AgentWorkflowReconciler) emitWorkflowResultOnce(ctx context.Context, wf *amv1.AgentWorkflow, output json.RawMessage) {
	emitResultEventOnce(ctx, r.Client, r.ResultSinkClient, wf, wf.Spec.ResultSink, eventsink.Event{
		ID:     string(wf.UID),
		Type:   "com.smol-agents.workflow.completed",
		Source: fmt.Sprintf("/namespaces/%s/agentworkflows/%s", wf.Namespace, wf.Name),
		Data:   output,
	})
}

func (r *AgentWorkflowReconciler) applyWF(ctx context.Context, wf *amv1.AgentWorkflow, desired pure.AgentWorkflowStatus, res ctrl.Result) (ctrl.Result, error) {
	desired.ObservedGeneration = wf.Generation

	desired.Conditions = wf.Status.Conditions
	ready := metav1.ConditionFalse
	condReason := desired.Reason
	if condReason == "" {
		condReason = string(desired.Phase)
	}
	if desired.Phase == pure.PhaseCompleted {
		ready = metav1.ConditionTrue
	}
	setReadyCondition(&desired.Conditions, wf.Generation, ready, condReason, desired.Message)
	setProgressingCondition(&desired.Conditions, wf.Generation, desired.Phase == pure.PhasePending || desired.Phase == pure.PhaseRunning, condReason, desired.Message)

	if reflect.DeepEqual(wf.Status, desired) {
		return res, nil
	}
	wf.Status = desired
	if err := r.Status().Update(ctx, wf); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return res, nil
}

func ownedByWorkflow(obj metav1.Object, wf *amv1.AgentWorkflow) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == wf.UID && ref.Kind == "AgentWorkflow" {
			return true
		}
	}
	return false
}

func (r *AgentWorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mc := r.MaxConcurrentReconciles
	if mc < 1 {
		mc = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&amv1.AgentWorkflow{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&amv1.AgentRun{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: mc}).
		Complete(r)
}
