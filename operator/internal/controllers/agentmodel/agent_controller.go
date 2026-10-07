// Package agentmodel hosts the controllers for the runtime.agents.smol-agents.ai
// CR family: Agent, Tool, ModelProvider, AgentRun, AgentSession, AgentPolicy.
package agentmodel

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	"github.com/smol-platform/smol-agents/operator/internal/builders"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
)

// AgentReconciler validates an Agent CR's references (ModelProvider,
// Tools), enforces the budget, and reports Status.Phase. It does NOT
// produce any owned objects — Agent is a passive declaration; the
// running Pod is produced from an AgentRun by the Run reconciler.
type AgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// AllowedStdioMCP is the operator's cluster allow-list of approved stdio MCP
	// server URLs (D7/D11, M2.15). A kind=mcp tool with a stdio (non-http) URL is
	// admission-refused unless its URL is in this set — arbitrary tenant stdio is
	// denied fail-closed. http(s) MCP is unaffected. Empty = no stdio permitted.
	AllowedStdioMCP map[string]bool
	// PlatformAgentPolicy names an existing AgentPolicy (any namespace) that
	// serves as the platform-wide governance floor (knative-agents-7dm, D1): a
	// namespace with no AgentPolicy of its own — or only policies that
	// constrain nothing — is checked against this baseline instead of falling
	// open. Zero value (empty Name) disables the fallback: today's behavior,
	// unchanged. Set from --platform-agent-policy.
	PlatformAgentPolicy types.NamespacedName
}

// SetupWithManager wires the controller. We Own ServiceAccount so the
// per-Agent SA (created by ensureServiceAccount) is GC'd with the Agent and
// re-created if it goes missing.
func (r *AgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&amv1.Agent{}).
		Owns(&corev1.ServiceAccount{}).
		Watches(&amv1.AgentPolicy{}, handler.EnqueueRequestsFromMapFunc(r.agentsInNamespace)).
		Complete(r)
}

// agentsInNamespace maps an AgentPolicy event to every Agent in the same
// namespace, so tightening (or relaxing) a policy re-evaluates its dependents
// within one reconcile rather than waiting for the next spec bump.
func (r *AgentReconciler) agentsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	var list amv1.AgentList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: list.Items[i].Namespace, Name: list.Items[i].Name,
		}})
	}
	return reqs
}

// Reconcile is the per-Agent entrypoint.
func (r *AgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("agent", req.NamespacedName)

	agent := &amv1.Agent{}
	if err := r.Get(ctx, req.NamespacedName, agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := pure.ValidateAgent(toPure(agent)); err != nil {
		r.setStatus(agent, "Failed", "InvalidSpec", err.Error())
		return ctrl.Result{}, r.Status().Update(ctx, agent)
	}

	// Ensure the per-Agent ServiceAccount that AgentRun pods execute as, so
	// pod-create doesn't fail with "service account not found".
	if err := r.ensureServiceAccount(ctx, agent); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure ServiceAccount: %w", err)
	}

	// Resolve ModelProvider — only when the Agent actually has one. Harness
	// agents (mode=harness) delegate generation to a sidecar/HTTP gateway and
	// have no Model field to look up; treating "no provider" as Pending would
	// strand them in a misleading ProviderMissing state forever.
	var providerName string
	if agent.Spec.Model.ProviderRef != "" {
		provider := &amv1.ModelProvider{}
		err := r.Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Spec.Model.ProviderRef}, provider)
		if err != nil {
			if apierrors.IsNotFound(err) {
				r.setStatus(agent, "Pending", "ProviderMissing",
					fmt.Sprintf("ModelProvider %q not found", agent.Spec.Model.ProviderRef))
				return ctrl.Result{}, r.Status().Update(ctx, agent)
			}
			return ctrl.Result{}, err
		}
		providerName = provider.Name
	} else if agent.Spec.Mode != pure.ModeHarness {
		// Loop-mode agents need a Model.ProviderRef; harness agents legitimately
		// don't.
		r.setStatus(agent, "Pending", "ProviderMissing",
			"spec.model.providerRef is required for loop-mode agents")
		return ctrl.Result{}, r.Status().Update(ctx, agent)
	}

	// Resolve every referenced Tool.
	resolved := make([]string, 0, len(agent.Spec.Tools))
	hasAgentTool := false
	for _, ref := range agent.Spec.Tools {
		ns := ref.Namespace
		if ns == "" {
			ns = agent.Namespace
		}
		tool := &amv1.Tool{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: ref.Name}, tool); err != nil {
			if apierrors.IsNotFound(err) {
				r.setStatus(agent, "Pending", "ToolMissing",
					fmt.Sprintf("Tool %q (ns=%s) not found", ref.Name, ns))
				return ctrl.Result{}, r.Status().Update(ctx, agent)
			}
			return ctrl.Result{}, err
		}
		// Fail closed (D3) on a loop-mode tool whose kind has no production
		// invoker yet (agent/function). Harness-mode tool refs are inert — don't
		// false-positive them.
		if agent.Spec.Mode != pure.ModeHarness && !pure.SupportedLoopToolKinds()[tool.Spec.Kind] {
			r.setStatus(agent, "Failed", "ToolKindUnsupported",
				fmt.Sprintf("tool %q kind %q has no loop-mode invoker", tool.Name, tool.Spec.Kind))
			return ctrl.Result{}, r.Status().Update(ctx, agent)
		}
		// Fail closed (D7/D11, M2.15): a kind=mcp tool with a stdio (non-http) URL
		// must resolve to an operator-approved server; arbitrary tenant stdio is
		// denied. http(s) MCP is unaffected.
		if agent.Spec.Mode != pure.ModeHarness && isStdioMCPTool(tool.Spec) && !r.AllowedStdioMCP[tool.Spec.MCP.URL] {
			r.setStatus(agent, "Failed", "StdioMCPNotAllowed",
				fmt.Sprintf("tool %q stdio MCP %q is not on the operator allow-list", tool.Name, tool.Spec.MCP.URL))
			return ctrl.Result{}, r.Status().Update(ctx, agent)
		}
		if tool.Spec.Kind == pure.ToolAgent {
			hasAgentTool = true
		}
		resolved = append(resolved, tool.Name)
	}

	// A2A (M3.6/D1): an Agent that declares a kind=agent tool gets a namespaced
	// Role+RoleBinding granting its run pods authority to create + observe CHILD
	// AgentRuns in their OWN namespace only. A non-A2A Agent's pods keep zero
	// apiserver authority. Created before Ready so the grant exists by the time a
	// run pod schedules.
	if hasAgentTool {
		if err := r.ensureA2ARBAC(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure A2A RBAC: %w", err)
		}
	}

	agent.Status.ResolvedTools = resolved
	agent.Status.ResolvedProvider = providerName

	// Enforce namespace AgentPolicy allow-lists at reconcile time (the
	// belt-and-suspenders backstop to the admission webhook): a disallowed
	// resolved provider or tool flips the Agent to Failed/PolicyViolation. A
	// transient list error now fails CLOSED (knative-agents-2mi): we hold the
	// Agent Pending/PolicyUnavailable and requeue rather than let it reach
	// Ready without having been checked against the namespace policy — an
	// apiserver hiccup must not silently bypass governance. A tightened policy
	// re-enqueues dependents via Watches.
	eff, err := effectivePolicyFor(ctx, r.Client, agent.Namespace, r.PlatformAgentPolicy)
	if err != nil {
		r.setStatus(agent, "Pending", "PolicyUnavailable",
			fmt.Sprintf("could not read namespace AgentPolicy: %v", err))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, agent)
	}
	if !eff.Empty {
		if providerName != "" && !eff.AllowsProvider(agent.Spec.Model.ProviderRef) {
			r.setStatus(agent, "Failed", "PolicyViolation",
				fmt.Sprintf("provider %q is not in the AgentPolicy allow-list", agent.Spec.Model.ProviderRef))
			return ctrl.Result{}, r.Status().Update(ctx, agent)
		}
		for _, tool := range resolved {
			if !eff.AllowsTool(tool) {
				r.setStatus(agent, "Failed", "PolicyViolation",
					fmt.Sprintf("tool %q is not in the AgentPolicy allow-list", tool))
				return ctrl.Result{}, r.Status().Update(ctx, agent)
			}
		}
	}

	r.setStatus(agent, "Ready", "Reconciled", "")
	logger.Info("agent ready", "tools", len(resolved), "provider", providerName)
	return ctrl.Result{}, r.Status().Update(ctx, agent)
}

// ensureServiceAccount creates (once) the SA AgentRun pods execute as. Owned
// by the Agent so it's GC'd when the Agent is deleted; a pre-existing SA is
// left untouched.
func (r *AgentReconciler) ensureServiceAccount(ctx context.Context, agent *amv1.Agent) error {
	sa := builders.AgentServiceAccount(agent)
	if err := ctrl.SetControllerReference(agent, sa, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.ServiceAccount{}
	getErr := r.Get(ctx, types.NamespacedName{Namespace: sa.Namespace, Name: sa.Name}, existing)
	if apierrors.IsNotFound(getErr) {
		return r.Create(ctx, sa)
	}
	return getErr
}

// ensureA2ARBAC creates (once) the namespaced Role + RoleBinding that let an
// A2A-capable Agent's run pods create/observe child AgentRuns in their own
// namespace (M3.6/D1). Both are owned by the Agent (GC'd with it) and bind the
// Agent's run ServiceAccount. Idempotent; pre-existing objects are left as-is.
func (r *AgentReconciler) ensureA2ARBAC(ctx context.Context, agent *amv1.Agent) error {
	role := builders.AgentA2ARole(agent)
	if err := ctrl.SetControllerReference(agent, role, r.Scheme); err != nil {
		return err
	}
	existingRole := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: role.Namespace, Name: role.Name}, existingRole); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, role); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	rb := builders.AgentA2ARoleBinding(agent)
	if err := ctrl.SetControllerReference(agent, rb, r.Scheme); err != nil {
		return err
	}
	existingRB := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rb.Namespace, Name: rb.Name}, existingRB); apierrors.IsNotFound(err) {
		return r.Create(ctx, rb)
	} else if err != nil {
		return err
	}
	return nil
}

// toPure unwraps the K8s wrapper into the pure Agent shape so the
// existing pkg/agentmodel/v1.ValidateAgent function can run.
func toPure(a *amv1.Agent) pure.Agent {
	return pure.Agent{Spec: a.Spec, Status: a.Status}
}

func (r *AgentReconciler) setStatus(a *amv1.Agent, phase, reason, msg string) {
	a.Status.Phase = phase
	a.Status.Reason = reason
	a.Status.Message = msg
	a.Status.ObservedGeneration = a.Generation

	ready := metav1.ConditionFalse
	if phase == "Ready" {
		ready = metav1.ConditionTrue
	}
	setReadyCondition(&a.Status.Conditions, a.Generation, ready, reason, msg)
	setProgressingCondition(&a.Status.Conditions, a.Generation, phase == "Pending", reason, msg)
}

// isStdioMCPTool reports whether a tool is a kind=mcp tool targeting a stdio
// (non-http) MCP server — the subject of the M2.15 operator allow-list gate.
// http(s) MCP URLs (handled by MCPInvoker) are not stdio.
func isStdioMCPTool(spec pure.ToolSpec) bool {
	if spec.Kind != pure.ToolMCP || spec.MCP == nil || spec.MCP.URL == "" {
		return false
	}
	u := spec.MCP.URL
	return !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://")
}
