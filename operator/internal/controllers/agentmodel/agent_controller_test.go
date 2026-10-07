package agentmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	"github.com/smol-platform/smol-agents/operator/internal/builders"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
)

// newAgentReconcilerForTest builds an in-memory reconciler with the given
// starting objects. The Agent has a status subresource so r.Status().Update
// must be declared on the fake client.
func newAgentReconcilerForTest(t *testing.T, initial ...client.Object) *AgentReconciler {
	t.Helper()
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	if err := amv1.AddToScheme(sch); err != nil {
		t.Fatalf("amv1 scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1 scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(initial...).
		WithStatusSubresource(&amv1.Agent{}).
		Build()
	return &AgentReconciler{Client: c, Scheme: sch}
}

func harnessAgent(name, ns string) *amv1.Agent {
	a := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	a.Spec.Mode = pure.ModeHarness
	a.Spec.Instructions = "be terse"
	a.Spec.Budget = pure.Budget{MaxSteps: 1, MaxTokens: 1024, MaxWallClockSeconds: 60, MaxToolCalls: 0}
	a.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessHermes, HTTP: &pure.HarnessHTTPSpec{URL: "http://gw"}}
	return a
}

// loopAgent is a minimal valid loop-mode Agent (has a ModelRef) for policy-gate
// tests.
func loopAgent(name, ns, provider string) *amv1.Agent {
	a := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	a.Spec.Model = pure.ModelRef{ProviderRef: provider, Name: "m"}
	a.Spec.Instructions = "hi"
	a.Spec.Budget = pure.Budget{MaxSteps: 1, MaxTokens: 100, MaxWallClockSeconds: 10, MaxToolCalls: 0}
	return a
}

func reconcileAgent(t *testing.T, r *AgentReconciler, ns, name string) *amv1.Agent {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &amv1.Agent{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	return got
}

// M1.5: a namespace AgentPolicy that excludes the Agent's provider flips it to
// Failed/PolicyViolation at reconcile; a conforming policy leaves it Ready.
func TestAgentReconciler_PolicyGate_DeniesDisallowedProvider(t *testing.T) {
	agent := loopAgent("alice", "tenant-a", "anthropic")
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}
	policy := &amv1.AgentPolicy{ObjectMeta: metav1.ObjectMeta{Name: "only-openai", Namespace: "tenant-a"},
		Spec: pure.AgentPolicySpec{AllowedProviders: []string{"openai"}}}

	r := newAgentReconcilerForTest(t, agent, provider, policy)
	got := reconcileAgent(t, r, "tenant-a", "alice")
	if got.Status.Phase != "Failed" || got.Status.Reason != "PolicyViolation" {
		t.Fatalf("want Failed/PolicyViolation, got %q/%q (%s)", got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
}

// knative-agents-2mi: a transient AgentPolicy List error at reconcile must
// fail CLOSED — the Agent is held Pending/PolicyUnavailable and requeued,
// never allowed to reach Ready without having been checked against the
// namespace policy (a prior bug fell through to Ready on the err!=nil path).
func TestAgentReconciler_PolicyGate_ListErrorHoldsPendingNotReady(t *testing.T) {
	agent := loopAgent("carol", "tenant-a", "anthropic")
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}

	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	if err := amv1.AddToScheme(sch); err != nil {
		t.Fatalf("amv1 scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1 scheme: %v", err)
	}
	base := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(agent, provider).
		WithStatusSubresource(&amv1.Agent{}).
		Build()
	// Only AgentPolicyList reads fail — everything else (ServiceAccount lookups
	// etc.) goes through untouched, so this isolates the policy-gate path.
	ic := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*amv1.AgentPolicyList); ok {
				return fmt.Errorf("simulated apiserver hiccup")
			}
			return c.List(ctx, list, opts...)
		},
	})
	r := &AgentReconciler{Client: ic, Scheme: sch}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "tenant-a", Name: "carol"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("want RequeueAfter=30s on a transient policy-list error, got %v", res.RequeueAfter)
	}
	got := &amv1.Agent{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "tenant-a", Name: "carol"}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != "Pending" || got.Status.Reason != "PolicyUnavailable" {
		t.Fatalf("want Pending/PolicyUnavailable on AgentPolicy list error (fail closed), got %q/%q (%s)",
			got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
}

// knative-agents-7dm: with a platform baseline configured, the reconcile
// backstop marks Failed/PolicyViolation against the baseline even though the
// Agent's own namespace has no AgentPolicy at all — closing the same hole as
// the admission-side TestAgentPolicyGate_BaselineDeniesOutsideAllowList.
func TestAgentReconciler_PolicyGate_BaselineDeniesInPolicyLessNamespace(t *testing.T) {
	agent := loopAgent("dave", "tenant-a", "anthropic")
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}
	baseline := &amv1.AgentPolicy{ObjectMeta: metav1.ObjectMeta{Name: "floor", Namespace: "platform"},
		Spec: pure.AgentPolicySpec{AllowedProviders: []string{"openai"}}}

	// tenant-a has NO AgentPolicy of its own.
	r := newAgentReconcilerForTest(t, agent, provider, baseline)
	r.PlatformAgentPolicy = types.NamespacedName{Namespace: "platform", Name: "floor"}
	got := reconcileAgent(t, r, "tenant-a", "dave")
	if got.Status.Phase != "Failed" || got.Status.Reason != "PolicyViolation" {
		t.Fatalf("want Failed/PolicyViolation against the baseline, got %q/%q (%s)",
			got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
}

func TestAgentReconciler_PolicyGate_AllowsConformingProvider(t *testing.T) {
	agent := loopAgent("bob", "tenant-a", "anthropic")
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}
	policy := &amv1.AgentPolicy{ObjectMeta: metav1.ObjectMeta{Name: "allow-anthropic", Namespace: "tenant-a"},
		Spec: pure.AgentPolicySpec{AllowedProviders: []string{"anthropic"}}}

	r := newAgentReconcilerForTest(t, agent, provider, policy)
	got := reconcileAgent(t, r, "tenant-a", "bob")
	if got.Status.Phase != "Ready" {
		t.Fatalf("want Ready, got %q/%q (%s)", got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
}

// M2.15: a loop agent with a kind=mcp tool on a stdio (non-http) URL is failed
// closed unless the URL is on the operator allow-list; http(s) MCP is unaffected.
func TestAgentReconciler_StdioMCPAllowList(t *testing.T) {
	provider := func() *amv1.ModelProvider {
		return &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}
	}
	stdioTool := func() *amv1.Tool {
		return &amv1.Tool{ObjectMeta: metav1.ObjectMeta{Name: "stdio-mcp", Namespace: "tenant-a"},
			Spec: pure.ToolSpec{Kind: pure.ToolMCP, MCP: &pure.MCPSpec{URL: "mcp://local-server"}}}
	}
	loopMCP := func(name string) *amv1.Agent {
		a := loopAgent(name, "tenant-a", "anthropic")
		a.Spec.Tools = []pure.ToolRef{{Name: "stdio-mcp"}}
		return a
	}

	// Not allow-listed → Failed/StdioMCPNotAllowed.
	r := newAgentReconcilerForTest(t, loopMCP("loopy"), provider(), stdioTool())
	if got := reconcileAgent(t, r, "tenant-a", "loopy"); got.Status.Phase != "Failed" || got.Status.Reason != "StdioMCPNotAllowed" {
		t.Fatalf("un-allow-listed stdio MCP → want Failed/StdioMCPNotAllowed, got %q/%q", got.Status.Phase, got.Status.Reason)
	}

	// Allow-listed → passes the stdio gate.
	r2 := newAgentReconcilerForTest(t, loopMCP("loopy2"), provider(), stdioTool())
	r2.AllowedStdioMCP = map[string]bool{"mcp://local-server": true}
	if got := reconcileAgent(t, r2, "tenant-a", "loopy2"); got.Status.Reason == "StdioMCPNotAllowed" {
		t.Errorf("allow-listed stdio MCP must pass the gate, got %q/%q", got.Status.Phase, got.Status.Reason)
	}

	// http(s) MCP is unaffected by the stdio gate, even with an empty allow-list.
	httpTool := &amv1.Tool{ObjectMeta: metav1.ObjectMeta{Name: "http-mcp", Namespace: "tenant-a"},
		Spec: pure.ToolSpec{Kind: pure.ToolMCP, MCP: &pure.MCPSpec{URL: "https://mcp.example/mcp"}}}
	loopH := loopAgent("httpy", "tenant-a", "anthropic")
	loopH.Spec.Tools = []pure.ToolRef{{Name: "http-mcp"}}
	rH := newAgentReconcilerForTest(t, loopH, provider(), httpTool)
	if got := reconcileAgent(t, rH, "tenant-a", "httpy"); got.Status.Reason == "StdioMCPNotAllowed" {
		t.Errorf("http MCP must not hit the stdio gate, got %q/%q", got.Status.Phase, got.Status.Reason)
	}
}

func TestIsStdioMCPTool(t *testing.T) {
	mcp := func(url string) pure.ToolSpec { return pure.ToolSpec{Kind: pure.ToolMCP, MCP: &pure.MCPSpec{URL: url}} }
	if !isStdioMCPTool(mcp("mcp://x")) {
		t.Error("mcp:// must be stdio")
	}
	for _, u := range []string{"https://x/mcp", "http://x/mcp", ""} {
		if isStdioMCPTool(mcp(u)) {
			t.Errorf("%q must not be stdio", u)
		}
	}
	if isStdioMCPTool(pure.ToolSpec{Kind: pure.ToolHTTP, HTTP: &pure.HTTPSpec{URL: "mcp://x"}}) {
		t.Error("non-mcp kind must not be stdio-mcp")
	}
}

// M2.16: a loop-mode agent referencing a tool whose kind has no production
// invoker (agent/function) is failed closed; a harness-mode agent with the same
// inert tool ref is not false-positived.
func TestAgentReconciler_ToolKindUnsupported(t *testing.T) {
	// kind=function has no production invoker (test-only) and stays reserved even
	// after A2A (kind=agent) was wired — so it is the canonical fail-closed case.
	fnTool := &amv1.Tool{ObjectMeta: metav1.ObjectMeta{Name: "fn", Namespace: "tenant-a"}, Spec: pure.ToolSpec{Kind: pure.ToolFunction, Function: &pure.FunctionSpec{Name: "noop"}}}
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}

	loop := loopAgent("loopy", "tenant-a", "anthropic")
	loop.Spec.Tools = []pure.ToolRef{{Name: "fn"}}
	r := newAgentReconcilerForTest(t, loop, provider, fnTool)
	got := reconcileAgent(t, r, "tenant-a", "loopy")
	if got.Status.Phase != "Failed" || got.Status.Reason != "ToolKindUnsupported" {
		t.Fatalf("loop agent w/ kind:function tool → want Failed/ToolKindUnsupported, got %q/%q", got.Status.Phase, got.Status.Reason)
	}

	h := harnessAgent("harn", "tenant-a")
	h.Spec.Tools = []pure.ToolRef{{Name: "fn"}}
	r2 := newAgentReconcilerForTest(t, h, fnTool)
	got2 := reconcileAgent(t, r2, "tenant-a", "harn")
	if got2.Status.Reason == "ToolKindUnsupported" {
		t.Fatalf("harness-mode inert tool ref must NOT be failed as ToolKindUnsupported (got %q/%q)", got2.Status.Phase, got2.Status.Reason)
	}
}

func TestAgentReconciler_A2ARBACGrant(t *testing.T) {
	// A loop Agent that declares a kind=agent tool is now supported (A2A wired)
	// and reconciles to Ready, and the operator grants it the namespaced A2A
	// Role + RoleBinding so its run pods may create child AgentRuns.
	agentTool := &amv1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "delegate", Namespace: "tenant-a"},
		Spec:       pure.ToolSpec{Kind: pure.ToolAgent, Agent: &pure.AgentTargetSpec{Ref: pure.ToolRef{Name: "child"}}},
	}
	provider := &amv1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "tenant-a"}}
	loop := loopAgent("composer", "tenant-a", "anthropic")
	loop.Spec.Tools = []pure.ToolRef{{Name: "delegate"}}

	r := newAgentReconcilerForTest(t, loop, provider, agentTool)
	got := reconcileAgent(t, r, "tenant-a", "composer")
	if got.Status.Phase != "Ready" {
		t.Fatalf("loop agent w/ kind=agent tool → want Ready, got %q/%q", got.Status.Phase, got.Status.Reason)
	}
	roleName := builders.AgentA2ARoleName("composer")
	role := &rbacv1.Role{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "tenant-a", Name: roleName}, role); err != nil {
		t.Fatalf("A2A Role %q not created: %v", roleName, err)
	}
	rb := &rbacv1.RoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "tenant-a", Name: roleName}, rb); err != nil {
		t.Fatalf("A2A RoleBinding %q not created: %v", roleName, err)
	}
	if len(rb.Subjects) == 0 || rb.Subjects[0].Name != builders.AgentSAName("composer") {
		t.Errorf("A2A RoleBinding must bind the agent's run SA %q, got %+v", builders.AgentSAName("composer"), rb.Subjects)
	}
}

func TestToPure_RoundTrip(t *testing.T) {
	a := &amv1.Agent{}
	a.Spec.Model.ProviderRef = "openai"
	a.Spec.Model.Name = "gpt-4"
	a.Spec.Instructions = "be helpful"
	a.Spec.Budget = pure.Budget{MaxSteps: 10, MaxTokens: 1000, MaxWallClockSeconds: 30, MaxToolCalls: 5}

	got := toPure(a)
	if got.Spec.Model.Name != "gpt-4" {
		t.Errorf("model.Name lost: %q", got.Spec.Model.Name)
	}
	if got.Spec.Budget.MaxSteps != 10 {
		t.Errorf("budget.MaxSteps lost: %d", got.Spec.Budget.MaxSteps)
	}

	// Pure validate accepts the same.
	if err := pure.ValidateAgent(got); err != nil {
		t.Errorf("toPure-then-validate rejected valid agent: %v", err)
	}
}

func TestSetStatus_RecordsAllFields(t *testing.T) {
	r := &AgentReconciler{}
	a := &amv1.Agent{}
	a.Generation = 5
	r.setStatus(a, "Ready", "Reconciled", "all good")
	if a.Status.Phase != "Ready" {
		t.Errorf("phase = %q", a.Status.Phase)
	}
	if a.Status.Reason != "Reconciled" {
		t.Errorf("reason = %q", a.Status.Reason)
	}
	if a.Status.ObservedGeneration != 5 {
		t.Errorf("gen = %d", a.Status.ObservedGeneration)
	}
	cond := apimeta.FindStatusCondition(a.Status.Conditions, ConditionReady)
	if cond == nil {
		t.Fatal("Ready condition not set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition status = %v, want True", cond.Status)
	}
	if cond.Reason != "Reconciled" {
		t.Errorf("Ready condition reason = %q", cond.Reason)
	}
	if cond.ObservedGeneration != 5 {
		t.Errorf("Ready condition observedGeneration = %d", cond.ObservedGeneration)
	}
	if prog := apimeta.FindStatusCondition(a.Status.Conditions, ConditionProgressing); prog == nil || prog.Status != metav1.ConditionFalse {
		t.Errorf("Progressing condition = %+v, want False", prog)
	}
}

func TestAgentDeepCopy_PreservesContents(t *testing.T) {
	a := &amv1.Agent{}
	a.Spec.Model.Name = "claude"
	a.Spec.Instructions = "x"
	a.Spec.Budget = pure.Budget{MaxSteps: 1, MaxTokens: 100, MaxWallClockSeconds: 1, MaxToolCalls: 0}
	a.Spec.Tools = []pure.ToolRef{{Name: "search"}}
	cp := a.DeepCopy()
	if cp.Spec.Model.Name != "claude" {
		t.Errorf("model name lost in deepcopy")
	}
	// Verify list independence.
	cp.Spec.Tools[0].Name = "mutated"
	if a.Spec.Tools[0].Name == "mutated" {
		// JSON round-trip would isolate; for shallow copy we accept the
		// shared slice (matches generated DeepCopy when Spec is a value).
		// Verify a fresh deepcopy doesn't propagate the mutation back.
		fresh := a.DeepCopy()
		if fresh.Spec.Tools[0].Name == "mutated" {
			t.Error("deepcopy shared the slice")
		}
	}
}

func TestAgentRun_Marshalable(t *testing.T) {
	r := &amv1.AgentRun{}
	r.Spec.AgentRef = "alice"
	r.Spec.Input = json.RawMessage(`{"q":"hi"}`)
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(string(out), `"agentRef":"alice"`) {
		t.Errorf("marshal lost agentRef: %s", out)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ── Reconcile-level tests (issues #1 / #2 follow-ups) ────────────────────────

// Harness-mode agent has no Model/ProviderRef; the controller used to stamp it
// Pending/ProviderMissing forever. Should now reach Ready.
func TestReconcile_HarnessAgent_NoProvider_ReachesReady(t *testing.T) {
	a := harnessAgent("alice", "tenant-a")
	r := newAgentReconcilerForTest(t, a)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: a.Name, Namespace: a.Namespace}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &amv1.Agent{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: a.Name, Namespace: a.Namespace}, got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status.Phase != "Ready" {
		t.Errorf("phase = %q, want Ready (harness agents have no Model to resolve); reason=%q msg=%q",
			got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
	if got.Status.ResolvedProvider != "" {
		t.Errorf("ResolvedProvider = %q, want empty for harness mode", got.Status.ResolvedProvider)
	}
}

// Loop-mode agent referencing a non-existent ModelProvider should stay Pending
// with ProviderMissing — this path must keep working (we only skip provider
// resolution for harness mode, not for loop).
func TestReconcile_LoopAgent_ProviderRefMissing_StaysPending(t *testing.T) {
	a := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "bob", Namespace: "tenant-a"}}
	a.Spec.Mode = pure.ModeLoop
	a.Spec.Model = pure.ModelRef{ProviderRef: "ghost", Name: "gpt-4"}
	a.Spec.Instructions = "x"
	a.Spec.Budget = pure.Budget{MaxSteps: 1, MaxTokens: 100, MaxWallClockSeconds: 10, MaxToolCalls: 0}

	r := newAgentReconcilerForTest(t, a)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: a.Name, Namespace: a.Namespace}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &amv1.Agent{}
	_ = r.Get(context.Background(), types.NamespacedName{Name: a.Name, Namespace: a.Namespace}, got)
	if got.Status.Phase != "Pending" || got.Status.Reason != "ProviderMissing" {
		t.Errorf("loop with absent providerRef should be Pending/ProviderMissing; got phase=%q reason=%q",
			got.Status.Phase, got.Status.Reason)
	}
}

// Reconciling an Agent must create the ServiceAccount that AgentRun pods run
// as, so pod-create doesn't fail with "service account not found".
func TestReconcile_CreatesServiceAccount_WithOwnerRef(t *testing.T) {
	a := harnessAgent("alice", "tenant-a")
	r := newAgentReconcilerForTest(t, a)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: a.Name, Namespace: a.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	sa := &corev1.ServiceAccount{}
	err := r.Get(context.Background(), types.NamespacedName{Name: builders.AgentSAName(a.Name), Namespace: a.Namespace}, sa)
	if err != nil {
		t.Fatalf("SA not created (want %s/%s): %v", a.Namespace, builders.AgentSAName(a.Name), err)
	}

	// Owned by the Agent so it's garbage-collected with it.
	if len(sa.OwnerReferences) == 0 || sa.OwnerReferences[0].Name != a.Name || sa.OwnerReferences[0].Kind != "Agent" {
		t.Errorf("SA missing controller-ref to its Agent; got %+v", sa.OwnerReferences)
	}
}

// Two reconciles must not double-create the SA.
func TestReconcile_ServiceAccount_Idempotent(t *testing.T) {
	a := harnessAgent("alice", "tenant-a")
	r := newAgentReconcilerForTest(t, a)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: a.Name, Namespace: a.Namespace}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	// One and only one SA in the namespace.
	list := &corev1.ServiceAccountList{}
	if err := r.List(context.Background(), list, client.InNamespace(a.Namespace)); err != nil {
		t.Fatalf("list: %v", err)
	}
	hits := 0
	for _, sa := range list.Items {
		if sa.Name == builders.AgentSAName(a.Name) {
			hits++
		}
	}
	if hits != 1 {
		t.Errorf("expected exactly 1 agent SA, got %d", hits)
	}
}

// If the SA is deleted out-of-band, the next reconcile re-creates it.
func TestReconcile_RecreatesSA_WhenDeleted(t *testing.T) {
	a := harnessAgent("alice", "tenant-a")
	r := newAgentReconcilerForTest(t, a)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: a.Name, Namespace: a.Namespace}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: builders.AgentSAName(a.Name), Namespace: a.Namespace}}
	if err := r.Delete(context.Background(), sa); err != nil {
		t.Fatalf("delete SA: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got := &corev1.ServiceAccount{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: builders.AgentSAName(a.Name), Namespace: a.Namespace}, got); err != nil {
		t.Errorf("SA not recreated after delete: %v", err)
	}
}
