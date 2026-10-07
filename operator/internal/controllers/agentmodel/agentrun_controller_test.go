package agentmodel

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	"github.com/smol-platform/smol-agents/operator/internal/builders"
	"github.com/smol-platform/smol-agents/pkg/agentfs"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
	"github.com/smol-platform/smol-agents/pkg/agentnet/plan"
	"github.com/smol-platform/smol-agents/pkg/agentruntime"
)

// runPodWithTerminationMessage builds a Pod whose `harness` container has
// already terminated with the given RunResult serialised into its termination
// message — the same shape `agent run` writes for the controller to fold.
func runPodWithTerminationMessage(rr agentruntime.RunResult) *corev1.Pod {
	msg, _ := json.Marshal(rr)
	return &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "harness",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
						Message:  string(msg),
					},
				},
			}},
		},
	}
}

func sampleAgent() *amv1.Agent {
	a := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "tenant-a"}}
	a.Spec.Mode = pure.ModeLoop
	a.Spec.Model = pure.ModelRef{ProviderRef: "openai", Name: "gpt-4"}
	a.Spec.Instructions = "be helpful"
	a.Spec.Budget = pure.Budget{MaxSteps: 5, MaxTokens: 1000, MaxWallClockSeconds: 60, MaxToolCalls: 3}
	return a
}

func sampleRun() *amv1.AgentRun {
	r := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Name: "run-001", Namespace: "tenant-a"}}
	r.Spec.AgentRef = "alice"
	r.Spec.Input = []byte(`{"q":"hi"}`)
	return r
}

func TestBuildAgentRunPod_LoopMode(t *testing.T) {
	pod := builders.BuildAgentRunPod(sampleRun(), sampleAgent())
	if pod.Name != "run-001" || pod.Namespace != "tenant-a" {
		t.Errorf("name/ns wrong: %s/%s", pod.Namespace, pod.Name)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %s", pod.Spec.RestartPolicy)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers = %d", len(pod.Spec.Containers))
	}
	if pod.Spec.Containers[0].Name != "agent" {
		t.Errorf("loop container name = %s", pod.Spec.Containers[0].Name)
	}
}

func TestBuildAgentRunPod_HarnessMode_Image(t *testing.T) {
	a := sampleAgent()
	a.Spec.Mode = pure.ModeHarness
	a.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessClaudeCode, Image: "myreg/claude:1"}
	pod := builders.BuildAgentRunPod(sampleRun(), a)
	if pod.Spec.Containers[0].Name != "harness" {
		t.Errorf("harness container name = %s", pod.Spec.Containers[0].Name)
	}
	if pod.Spec.Containers[0].Image != "myreg/claude:1" {
		t.Errorf("image override ignored: %s", pod.Spec.Containers[0].Image)
	}
}

func TestBuildAgentRunPod_AgentFSVolume(t *testing.T) {
	a := sampleAgent()
	a.Spec.Mode = pure.ModeHarness
	a.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessClaudeCode}
	a.Spec.Storage = &pure.StorageSpec{
		Kind:    pure.StorageAgentFS,
		AgentFS: &pure.AgentFSSpec{SizeGiB: 5, MountPath: "/var/agentfs"},
	}
	pod := builders.BuildAgentRunPod(sampleRun(), a)
	found := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "agentfs" {
			found = true
		}
	}
	if !found {
		t.Error("agentfs volume not declared")
	}
	mount := false
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == "agentfs" && m.MountPath == "/var/agentfs" {
			mount = true
		}
	}
	if !mount {
		t.Error("agentfs mount missing on harness container")
	}
}

func TestAgentRunReconciler_MarkRunning(t *testing.T) {
	r := &AgentRunReconciler{}
	run := sampleRun()
	r.markRunning(run)
	if run.Status.State != pure.PhaseRunning {
		t.Errorf("state=%s", run.Status.State)
	}
	if run.Status.StartedAt == nil {
		t.Error("StartedAt not set")
	}
	// Idempotent: a second markRunning should not overwrite StartedAt.
	first := *run.Status.StartedAt
	time.Sleep(2 * time.Millisecond)
	r.markRunning(run)
	if !run.Status.StartedAt.Equal(&first) {
		t.Errorf("StartedAt drifted on idempotent mark")
	}
}

// newRunReconcilerForTest builds an in-memory AgentRun reconciler, optionally
// intercepting client calls (e.g. to inject a status-update conflict).
func newRunReconcilerForTest(t *testing.T, fns interceptor.Funcs, initial ...client.Object) *AgentRunReconciler {
	t.Helper()
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	if err := amv1.AddToScheme(sch); err != nil {
		t.Fatalf("amv1 scheme: %v", err)
	}
	// knative-agents-1c5: the existing-Pod re-cage path exercises
	// NetworkPolicy Get/Create/Update via the real Reconcile(), so the fake
	// client needs the type registered (previously only sandboxReconciler's
	// narrower scheme in run_sandbox_controller_test.go had it).
	if err := networkingv1.AddToScheme(sch); err != nil {
		t.Fatalf("networkingv1 scheme: %v", err)
	}
	// Reconcile's A2A grant check (knative-agents-pwm) Gets an rbacv1.Role, so
	// any test driving the full Reconcile past pod creation needs it registered.
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1 scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(initial...).
		WithStatusSubresource(&amv1.AgentRun{}).
		WithInterceptorFuncs(fns).
		Build()
	return &AgentRunReconciler{Client: c, Scheme: sch}
}

// A status-update conflict is benign cache lag, not a failure: updateRunStatus
// must requeue without surfacing an error (otherwise the controller logs a
// noisy "Reconciler error" on every Pod-watch/poll race).
func TestAgentRunReconciler_updateRunStatus_ConflictRequeues(t *testing.T) {
	run := sampleRun()
	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: "runtime.agents.smol-agents.ai", Resource: "agentruns"},
		run.Name, errors.New("the object has been modified"))
	r := newRunReconcilerForTest(t, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return conflict
		},
	}, run)

	res, err := r.updateRunStatus(context.Background(), run, ctrl.Result{RequeueAfter: 5 * time.Second})
	if err != nil {
		t.Fatalf("conflict must not surface as an error, got %v", err)
	}
	if !res.Requeue {
		t.Errorf("conflict should requeue, got %+v", res)
	}
}

// A successful write returns the caller's intended Result verbatim.
func TestAgentRunReconciler_updateRunStatus_SuccessReturnsResult(t *testing.T) {
	run := sampleRun()
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, run)
	want := ctrl.Result{RequeueAfter: 5 * time.Second}
	res, err := r.updateRunStatus(context.Background(), run, want)
	if err != nil {
		t.Fatalf("updateRunStatus: %v", err)
	}
	if res != want {
		t.Errorf("res = %+v, want %+v", res, want)
	}
}

// A non-conflict error must still surface — conflict tolerance must not swallow
// real failures.
func TestAgentRunReconciler_updateRunStatus_NonConflictErrorSurfaces(t *testing.T) {
	run := sampleRun()
	r := newRunReconcilerForTest(t, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return errors.New("boom")
		},
	}, run)
	if _, err := r.updateRunStatus(context.Background(), run, ctrl.Result{}); err == nil {
		t.Error("a non-conflict error must surface, not be swallowed")
	}
}

func TestAgentRunReconciler_MarkTerminal(t *testing.T) {
	r := &AgentRunReconciler{}
	run := sampleRun()
	r.markTerminal(run, pure.PhaseCompleted, "")
	if run.Status.State != pure.PhaseCompleted {
		t.Errorf("state=%s", run.Status.State)
	}
	if run.Status.EndedAt == nil {
		t.Error("EndedAt not set")
	}
	cond := apimeta.FindStatusCondition(run.Status.Conditions, ConditionReady)
	if cond == nil {
		t.Fatal("Ready condition not set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition status = %v, want True on Completed", cond.Status)
	}
	if prog := apimeta.FindStatusCondition(run.Status.Conditions, ConditionProgressing); prog == nil || prog.Status != metav1.ConditionFalse {
		t.Errorf("Progressing condition = %+v, want False on a terminal phase", prog)
	}
}

// markTerminal must own TerminationReason — without this, a stale
// "Pod is Pending" hint left by markPending was surviving into Completed.
func TestAgentRunReconciler_MarkTerminal_ClearsStaleReason(t *testing.T) {
	r := &AgentRunReconciler{}
	run := sampleRun()
	r.markPending(run, "PodPending", "Pod is Pending") // leaves a stale reason
	if run.Status.TerminationReason != "Pod is Pending" {
		t.Fatalf("setup: terminationReason=%q", run.Status.TerminationReason)
	}
	r.markTerminal(run, pure.PhaseCompleted, "")
	if run.Status.TerminationReason != "" {
		t.Errorf("markTerminal(\"\") should clear stale reason, got %q", run.Status.TerminationReason)
	}
}

// The runtime's own reason ("budget:tokens" for an Expired run that still
// exits 0) is the most specific signal and must win over whatever pod-level
// reason markTerminal left behind.
// M3.12: a Hermes run that belongs to an AgentSession gets a stable
// HERMES_SESSION_ID (sess-<session UID>) + persistent session policy injected
// into a COPY of the agent; the stored Agent is never mutated.
func TestHermesSessionAgent_InjectsSessionID(t *testing.T) {
	session := &amv1.AgentSession{ObjectMeta: metav1.ObjectMeta{Name: "sess-1", Namespace: "tenant-a", UID: "uid-xyz"}}
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, session)
	hermes := harnessAgent("alice", "tenant-a") // HarnessHermes

	run := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"}}
	run.Spec.SessionRef = "sess-1"
	out := r.hermesSessionAgent(context.Background(), run, hermes)
	if out.Spec.Harness.SessionPolicy != pure.SessionPersistent {
		t.Errorf("sessionPolicy not set to persistent")
	}
	var got string
	for _, e := range out.Spec.Harness.Env {
		if e.Name == "HERMES_SESSION_ID" {
			got = e.Value
		}
	}
	if got != "sess-uid-xyz" {
		t.Errorf("HERMES_SESSION_ID = %q, want sess-uid-xyz", got)
	}
	if len(hermes.Spec.Harness.Env) != 0 {
		t.Errorf("original agent env mutated: %+v", hermes.Spec.Harness.Env)
	}

	noSess := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"}}
	if r.hermesSessionAgent(context.Background(), noSess, hermes) != hermes {
		t.Errorf("no sessionRef must return the agent unchanged")
	}
}

// M3.12: a session's MemoryScope is injected as HERMES_SESSION_KEY; absent → none.
func TestHermesSessionAgent_MemoryScope(t *testing.T) {
	session := &amv1.AgentSession{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "tenant-a", UID: "u"}}
	session.Spec.MemoryScope = "team-shared"
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, session)
	run := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"}}
	run.Spec.SessionRef = "s"

	out := r.hermesSessionAgent(context.Background(), run, harnessAgent("alice", "tenant-a"))
	var key string
	for _, e := range out.Spec.Harness.Env {
		if e.Name == "HERMES_SESSION_KEY" {
			key = e.Value
		}
	}
	if key != "team-shared" {
		t.Errorf("HERMES_SESSION_KEY = %q, want team-shared", key)
	}

	// No MemoryScope → no HERMES_SESSION_KEY.
	noScope := &amv1.AgentSession{ObjectMeta: metav1.ObjectMeta{Name: "s2", Namespace: "tenant-a", UID: "u2"}}
	r2 := newRunReconcilerForTest(t, interceptor.Funcs{}, noScope)
	run2 := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"}}
	run2.Spec.SessionRef = "s2"
	for _, e := range r2.hermesSessionAgent(context.Background(), run2, harnessAgent("alice", "tenant-a")).Spec.Harness.Env {
		if e.Name == "HERMES_SESSION_KEY" {
			t.Error("no MemoryScope must inject no HERMES_SESSION_KEY")
		}
	}
}

// M4.18: a pod killed by activeDeadlineSeconds carries the reason at the POD
// level — terminationReason must surface it (pod:DeadlineExceeded), preferring
// the pod-level reason over container status, and falling back to pod:Failed.
func TestTerminationReason_PrefersPodLevel(t *testing.T) {
	deadline := &corev1.Pod{Status: corev1.PodStatus{Reason: "DeadlineExceeded"}}
	if got := terminationReason(deadline); got != "pod:DeadlineExceeded" {
		t.Errorf("deadline pod → %q, want pod:DeadlineExceeded", got)
	}
	container := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
		{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"}}},
	}}}
	if got := terminationReason(container); got != "pod:OOMKilled" {
		t.Errorf("container-terminated pod → %q, want pod:OOMKilled", got)
	}
	if got := terminationReason(&corev1.Pod{}); got != "pod:Failed" {
		t.Errorf("bare failed pod → %q, want pod:Failed", got)
	}
}

func TestAgentRunReconciler_FoldRunResult_RuntimeReasonWins(t *testing.T) {
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	r.markTerminal(run, pure.PhaseCompleted, "") // pod said Succeeded, no reason
	pod := runPodWithTerminationMessage(agentruntime.RunResult{
		Phase:             pure.PhaseExpired,
		TerminationReason: "budget:tokens",
		Usage:             pure.Usage{Steps: 1, Tokens: 16102},
	})
	r.foldRunResult(context.Background(), run, nil, pod)
	if run.Status.State != pure.PhaseExpired {
		t.Errorf("state should be refined to Expired, got %q", run.Status.State)
	}
	if run.Status.TerminationReason != "budget:tokens" {
		t.Errorf("terminationReason = %q, want budget:tokens", run.Status.TerminationReason)
	}
	if run.Status.Usage.Tokens != 16102 {
		t.Errorf("usage not folded: tokens=%d", run.Status.Usage.Tokens)
	}
}

// A runtime error wins over both any prior reason and over the runtime's own
// TerminationReason (an Error is more diagnostic than the bare reason).
func TestAgentRunReconciler_FoldRunResult_ErrorWins(t *testing.T) {
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	r.markTerminal(run, pure.PhaseFailed, "pod:Error")
	pod := runPodWithTerminationMessage(agentruntime.RunResult{
		Phase:             pure.PhaseFailed,
		TerminationReason: "harness:timeout",
		Error:             "harness: http 502: upstream refused",
	})
	r.foldRunResult(context.Background(), run, nil, pod)
	if run.Status.TerminationReason != "harness: http 502: upstream refused" {
		t.Errorf("error should win, got %q", run.Status.TerminationReason)
	}
}

// M2.26: foldArtifacts maps the sidecar's termination-message manifest into
// Status.Artifacts; egress-requested-but-unreported folds to Failed; an
// unconfigured pod leaves Status.Artifacts nil.
func TestAgentRunReconciler_FoldArtifacts(t *testing.T) {
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	sidecar := builders.StorageFSSidecarName
	artifactEnv := []corev1.EnvVar{{Name: "AGENTFS_ARTIFACTS", Value: `[{"Name":"out","Glob":"out/*"}]`}}

	mb, _ := json.Marshal(agentfs.ArtifactManifest{
		State: agentfs.ArtifactComplete,
		Refs:  []agentfs.ArtifactRef{{Name: "out", Path: "out/r.json", S3Key: "artifacts/ns/r1/out/r.json", SizeBytes: 12, SHA256: "abc"}},
	})
	reported := &corev1.Pod{
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: sidecar, Env: artifactEnv}}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name: sidecar, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: string(mb)}},
		}}},
	}
	run := sampleRun()
	r.foldArtifacts(run, reported)
	if run.Status.Artifacts == nil || run.Status.Artifacts.State != agentfs.ArtifactComplete {
		t.Fatalf("manifest not folded: %+v", run.Status.Artifacts)
	}
	if len(run.Status.Artifacts.Refs) != 1 || run.Status.Artifacts.Refs[0].S3Key != "artifacts/ns/r1/out/r.json" {
		t.Errorf("ref not mapped: %+v", run.Status.Artifacts.Refs)
	}

	// Configured but the sidecar reported nothing → Failed.
	silent := &corev1.Pod{
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: sidecar, Env: artifactEnv}}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name: sidecar, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}},
		}}},
	}
	run2 := sampleRun()
	r.foldArtifacts(run2, silent)
	if run2.Status.Artifacts == nil || run2.Status.Artifacts.State != pure.ArtifactStateFailed {
		t.Errorf("egress requested but unreported must fold to Failed, got %+v", run2.Status.Artifacts)
	}

	// No artifact env → no fold (nil).
	run3 := sampleRun()
	r.foldArtifacts(run3, &corev1.Pod{})
	if run3.Status.Artifacts != nil {
		t.Errorf("unconfigured pod must leave Artifacts nil, got %+v", run3.Status.Artifacts)
	}
}

func getRun(t *testing.T, r *AgentRunReconciler, ns, name string) *amv1.AgentRun {
	t.Helper()
	got := &amv1.AgentRun{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
		t.Fatalf("get run: %v", err)
	}
	return got
}

// knative-agents-1c5: the AgentNetwork Watch's whole point is to re-cage a
// LIVE (already-Pod'd) run, not just one still queued pre-Pod — this is the
// bead's actual acceptance test. It drives a full Reconcile() (not just the
// ensure-helpers directly, which only proves the helper itself is
// update-in-place) against a run whose Pod already exists and is Running,
// with stored NetworkPolicies whose Specs have drifted from what a fresh
// computation would produce, and asserts Reconcile corrects both in place.
func TestReconcile_ExistingPod_RecagesDrift(t *testing.T) {
	agent := sampleAgent()
	run := sampleRun()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: run.Name, Namespace: run.Namespace},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	// Stale egress: an allow-CIDR a fresh (bound-network-less) computation
	// would never grant.
	staleEgress := builders.BuildAgentRunEgressPolicyWithPlan(run, plan.NetworkPlan{})
	staleEgress.Spec.Egress = append(staleEgress.Spec.Egress, networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "198.51.100.0/24"}}},
	})
	// Stale ingress: no PolicyTypes, as if from a broken prior write.
	staleIngress := builders.BuildAgentRunIngressPolicy(run)
	staleIngress.Spec.PolicyTypes = nil

	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, run, pod, staleEgress, staleIngress)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, r, run.Namespace, run.Name)
	if got.Status.State != pure.PhaseRunning {
		t.Fatalf("state = %s, want Running", got.Status.State)
	}

	var egress networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name + "-egress"}, &egress); err != nil {
		t.Fatalf("egress NetworkPolicy: %v", err)
	}
	wantEgress := builders.BuildAgentRunEgressPolicyWithPlan(run, plan.NetworkPlan{})
	if !reflect.DeepEqual(egress.Spec, wantEgress.Spec) {
		t.Errorf("egress Spec not corrected by Reconcile on the existing-Pod path:\ngot  %+v\nwant %+v", egress.Spec, wantEgress.Spec)
	}

	var ingress networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name + "-ingress"}, &ingress); err != nil {
		t.Fatalf("ingress NetworkPolicy: %v", err)
	}
	if len(ingress.Spec.PolicyTypes) != 1 || ingress.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("ingress policyTypes = %v, want [Ingress] (drift not corrected by Reconcile)", ingress.Spec.PolicyTypes)
	}
}

// A terminal run's NetworkPolicy must NOT be rewritten: there is nothing left
// to protect once the pod has exited, and rewriting it on every 5s poll of a
// finished run would just be wasted Updates forever. Proves the
// !run.Status.State.Terminal() gate actually gates.
func TestReconcile_TerminalPod_DoesNotRewritePolicy(t *testing.T) {
	agent := sampleAgent()
	run := sampleRun()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: run.Name, Namespace: run.Namespace},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed},
	}
	staleEgress := builders.BuildAgentRunEgressPolicyWithPlan(run, plan.NetworkPlan{})
	staleEgress.Spec.Egress = append(staleEgress.Spec.Egress, networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "198.51.100.0/24"}}},
	})

	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, run, pod, staleEgress)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, r, run.Namespace, run.Name)
	if !got.Status.State.Terminal() {
		t.Fatalf("state = %s, want a terminal state", got.Status.State)
	}

	var egress networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name + "-egress"}, &egress); err != nil {
		t.Fatalf("egress NetworkPolicy: %v", err)
	}
	found := false
	for _, rule := range egress.Spec.Egress {
		for _, peer := range rule.To {
			if peer.IPBlock != nil && peer.IPBlock.CIDR == "198.51.100.0/24" {
				found = true
			}
		}
	}
	if !found {
		t.Error("a terminal run's stale egress Spec was rewritten; it must be left untouched")
	}
}

// A live run whose bound AgentNetwork has been edited into a conflicting
// state must NOT be marked Pending or Failed over it — the pod is already
// running (and billing) regardless of Status, so that would only lie about
// the run's state without stopping anything. It stays Running, the conflict
// is surfaced on the existing Reason field (no new status field / CRD edit),
// and the NetworkPolicy it was admitted under is left exactly as it was.
func TestReconcile_ExistingPod_NetworkConflict_StaysRunningKeepsExistingPolicy(t *testing.T) {
	agent := sampleAgent()
	agent.Labels = map[string]string{"team": "x"}
	n1 := proxyNet("n1", agent.Namespace, map[string]string{"team": "x"}, 8080, "https://a")
	n2 := proxyNet("n2", agent.Namespace, map[string]string{"team": "x"}, 8080, "https://b") // same port, different gateway → conflict
	run := sampleRun()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: run.Name, Namespace: run.Namespace},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	// The cage the run was admitted under before the AgentNetwork edit made
	// it conflict — any valid prior Spec works; the point is it must survive.
	existingEgress := builders.BuildAgentRunEgressPolicyWithPlan(run, plan.NetworkPlan{})
	existingIngress := builders.BuildAgentRunIngressPolicy(run)

	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, run, pod, n1, n2, existingEgress, existingIngress)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getRun(t, r, run.Namespace, run.Name)
	if got.Status.State != pure.PhaseRunning {
		t.Errorf("state = %s, want Running (a bad AgentNetwork edit must not fail or pend a live run)", got.Status.State)
	}
	if got.Status.Reason != "NetworkConflict" {
		t.Errorf("reason = %q, want NetworkConflict surfaced on the existing Reason field", got.Status.Reason)
	}

	var egress networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name + "-egress"}, &egress); err != nil {
		t.Fatalf("egress NetworkPolicy: %v", err)
	}
	if !reflect.DeepEqual(egress.Spec, existingEgress.Spec) {
		t.Errorf("egress Spec changed despite NetworkConflict:\ngot  %+v\nwant (unchanged) %+v", egress.Spec, existingEgress.Spec)
	}
}

func runPodExists(r *AgentRunReconciler, ns, name string) bool {
	return r.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &corev1.Pod{}) == nil
}

// M4.4: a session.required Agent is served by its resident AgentSession worker,
// so a bare standalone AgentRun (no sessionRef) against it is a misuse — it
// fails fast with reason agent:requires-session and NO pod is created. A
// session-linked turn-run (sessionRef set) skips the gate.
func TestAgentRunReconciler_SessionRequiredRejectsStandaloneRun(t *testing.T) {
	agent := sampleAgent()
	agent.Spec.Session = &pure.SessionSpec{Required: true}
	run := sampleRun() // no SessionRef
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, run)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getRun(t, r, run.Namespace, run.Name)
	if got.Status.State != pure.PhaseFailed || got.Status.TerminationReason != "agent:requires-session" {
		t.Fatalf("standalone run vs resident agent: state=%s reason=%q, want Failed/agent:requires-session",
			got.Status.State, got.Status.TerminationReason)
	}
	if runPodExists(r, run.Namespace, run.Name) {
		t.Error("a rejected resident-agent run must not create a pod")
	}

	// Control: a session-linked turn-run (sessionRef set) is NOT rejected by the
	// gate (it proceeds past it — it is the legitimate per-turn run path).
	turn := sampleRun()
	turn.Name = "turn-001"
	turn.Spec.SessionRef = "sess-1"
	r2 := newRunReconcilerForTest(t, interceptor.Funcs{}, sampleAgentSessionRequired(), turn)
	req2 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: turn.Namespace, Name: turn.Name}}
	if _, err := r2.Reconcile(context.Background(), req2); err != nil {
		t.Fatalf("Reconcile (turn-run): %v", err)
	}
	if got := getRun(t, r2, turn.Namespace, turn.Name); got.Status.TerminationReason == "agent:requires-session" {
		t.Errorf("a session-linked turn-run must not be rejected by the resident-agent gate")
	}
}

func sampleAgentSessionRequired() *amv1.Agent {
	a := sampleAgent()
	a.Spec.Session = &pure.SessionSpec{Required: true}
	return a
}

// M1.12: the per-tenant concurrency gate holds a run Pending when the namespace
// (or Agent) is at its Running-runs cap, and admits otherwise.
func mkRun(name, ns, ref string) *amv1.AgentRun {
	r := &amv1.AgentRun{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	r.Spec.AgentRef = ref
	r.Spec.Input = []byte(`{}`)
	return r
}

func TestAdmitRunConcurrency_NamespaceCap(t *testing.T) {
	quota := &amv1.AgentRunQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "q", Namespace: "tenant-a"},
		Spec:       pure.AgentRunQuotaSpec{MaxConcurrentRuns: 2},
	}
	rec := newRunReconcilerForTest(t, interceptor.Funcs{}, quota, mkRun("run-a", "tenant-a", "alice"), mkRun("run-b", "tenant-a", "bob"))
	for _, n := range []string{"run-a", "run-b"} { // seed both Running
		got := getRun(t, rec, "tenant-a", n)
		got.Status.State = pure.PhaseRunning
		if err := rec.Status().Update(context.Background(), got); err != nil {
			t.Fatalf("seed running: %v", err)
		}
	}
	newRun := mkRun("run-c", "tenant-a", "carol")
	handled, _, _ := rec.admitRunConcurrency(context.Background(), newRun, harnessAgent("carol", "tenant-a"))
	if !handled || newRun.Status.State != pure.PhasePending {
		t.Fatalf("at-cap run must be held Pending; handled=%v state=%q", handled, newRun.Status.State)
	}
	if newRun.Status.TerminationReason != "namespace at concurrency cap 2" {
		t.Errorf("reason = %q", newRun.Status.TerminationReason)
	}
}

func TestAdmitRunConcurrency_UnderCapAdmits(t *testing.T) {
	quota := &amv1.AgentRunQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "q", Namespace: "tenant-a"},
		Spec:       pure.AgentRunQuotaSpec{MaxConcurrentRuns: 5},
	}
	rec := newRunReconcilerForTest(t, interceptor.Funcs{}, quota, mkRun("run-a", "tenant-a", "alice"))
	got := getRun(t, rec, "tenant-a", "run-a")
	got.Status.State = pure.PhaseRunning
	_ = rec.Status().Update(context.Background(), got)

	handled, _, _ := rec.admitRunConcurrency(context.Background(), mkRun("run-b", "tenant-a", "bob"), harnessAgent("bob", "tenant-a"))
	if handled {
		t.Fatalf("under cap must admit (handled=false)")
	}
}

func TestAdmitRunConcurrency_PerAgentCap(t *testing.T) {
	rec := newRunReconcilerForTest(t, interceptor.Funcs{}, mkRun("run-a", "tenant-a", "alice"))
	got := getRun(t, rec, "tenant-a", "run-a")
	got.Status.State = pure.PhaseRunning
	_ = rec.Status().Update(context.Background(), got)

	agent := harnessAgent("alice", "tenant-a")
	agent.Spec.MaxConcurrentRuns = 1 // alice already has 1 Running
	newRun := mkRun("run-b", "tenant-a", "alice")
	handled, _, _ := rec.admitRunConcurrency(context.Background(), newRun, agent)
	if !handled || newRun.Status.State != pure.PhasePending {
		t.Fatalf("per-agent cap must hold the second alice run; handled=%v state=%q", handled, newRun.Status.State)
	}
}

// M5.3: a gated run parks in RequiresAction (no pod, no cost) until a token-
// matched approval lets it proceed; deny → Cancelled; stale token ignored; TTL
// → Expired.
func TestPreRunApproval_GateThenApprove(t *testing.T) {
	agent := harnessAgent("alice", "tenant-a")
	agent.Spec.Approval = &pure.ApprovalPolicy{RequireApprovalBeforeRun: true, ApprovalTimeoutSeconds: 3600}
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, sampleRun())
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "tenant-a", Name: "run-001"}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	got := getRun(t, r, "tenant-a", "run-001")
	if got.Status.State != pure.PhaseRequiresAction {
		t.Fatalf("want RequiresAction, got %q", got.Status.State)
	}
	if got.Status.PendingAction == nil || got.Status.PendingAction.Token == "" || got.Status.PendingAction.Kind != "pre-run" {
		t.Fatalf("pending token not minted: %+v", got.Status.PendingAction)
	}
	if runPodExists(r, "tenant-a", "run-001") {
		t.Fatalf("no pod must exist while awaiting approval")
	}

	got.Spec.Decision = &pure.Decision{Token: got.Status.PendingAction.Token, Approve: true, DecidedBy: "ops"}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatalf("patch decision: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	after := getRun(t, r, "tenant-a", "run-001")
	if after.Status.State == pure.PhaseRequiresAction {
		t.Fatalf("approval should clear the gate; still RequiresAction")
	}
	if after.Status.PendingAction != nil {
		t.Errorf("PendingAction should be cleared after approval, got %+v", after.Status.PendingAction)
	}
}

func TestPreRunApproval_Deny(t *testing.T) {
	agent := harnessAgent("alice", "tenant-a")
	agent.Spec.Approval = &pure.ApprovalPolicy{RequireApprovalBeforeRun: true, ApprovalTimeoutSeconds: 3600}
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, sampleRun())
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "tenant-a", Name: "run-001"}}

	_, _ = r.Reconcile(context.Background(), req)
	got := getRun(t, r, "tenant-a", "run-001")
	got.Spec.Decision = &pure.Decision{Token: got.Status.PendingAction.Token, Approve: false, Reason: "nope"}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatalf("patch decision: %v", err)
	}
	_, _ = r.Reconcile(context.Background(), req)
	after := getRun(t, r, "tenant-a", "run-001")
	if after.Status.State != pure.PhaseCancelled {
		t.Fatalf("deny → want Cancelled, got %q", after.Status.State)
	}
}

func TestPreRunApproval_StaleTokenIgnored(t *testing.T) {
	agent := harnessAgent("alice", "tenant-a")
	agent.Spec.Approval = &pure.ApprovalPolicy{RequireApprovalBeforeRun: true, ApprovalTimeoutSeconds: 3600}
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, sampleRun())
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "tenant-a", Name: "run-001"}}

	_, _ = r.Reconcile(context.Background(), req)
	got := getRun(t, r, "tenant-a", "run-001")
	got.Spec.Decision = &pure.Decision{Token: "wrong-token", Approve: true}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatalf("patch: %v", err)
	}
	_, _ = r.Reconcile(context.Background(), req)
	after := getRun(t, r, "tenant-a", "run-001")
	if after.Status.State != pure.PhaseRequiresAction {
		t.Fatalf("stale token must keep the run parked, got %q", after.Status.State)
	}
}

func TestPreRunApproval_ExpireOnTTL(t *testing.T) {
	agent := harnessAgent("alice", "tenant-a")
	agent.Spec.Approval = &pure.ApprovalPolicy{RequireApprovalBeforeRun: true, ApprovalTimeoutSeconds: 1}
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, agent, sampleRun())
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "tenant-a", Name: "run-001"}}

	// Seed an already-old pending action (the fake client may not persist an
	// initial Status, so set it explicitly).
	seed := getRun(t, r, "tenant-a", "run-001")
	seed.Status.State = pure.PhaseRequiresAction
	seed.Status.PendingAction = &pure.PendingAction{
		Kind: "pre-run", Token: "tok1",
		RequestedAt: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
	}
	if err := r.Status().Update(context.Background(), seed); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	after := getRun(t, r, "tenant-a", "run-001")
	if after.Status.State != pure.PhaseExpired {
		t.Fatalf("past-TTL approval → want Expired, got %q", after.Status.State)
	}
}

// M1.4: a namespace RedactionPolicy masks the folded Status.Output and any
// secret in a Step's tool-call result; with no policy the fold is byte-identical
// (zero-overhead fast path). RedactJSON re-marshals via map[string]any, so key
// order is sorted and the masked output is deterministic.
func TestAgentRunReconciler_FoldRunResult_Redaction(t *testing.T) {
	policy := &amv1.AgentPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "redact", Namespace: "tenant-a"},
		Spec:       pure.AgentPolicySpec{Redaction: &pure.RedactionPolicy{Patterns: []string{`sk-[a-z0-9]+`}}},
	}
	rr := agentruntime.RunResult{
		Phase:  pure.PhaseCompleted,
		Output: json.RawMessage(`{"key":"sk-deadbeef","n":1}`),
		Steps: []pure.Step{{Index: 0, Kind: pure.StepToolCall, ToolCalls: []pure.ToolCallRecord{
			{Tool: "fetch", Result: json.RawMessage(`{"token":"sk-secret99"}`)},
		}}},
	}

	// Policy present → secrets masked.
	r := newRunReconcilerForTest(t, interceptor.Funcs{}, policy)
	run := sampleRun()
	r.foldRunResult(context.Background(), run, nil, runPodWithTerminationMessage(rr))
	if got := string(run.Status.Output); got != `{"key":"[REDACTED]","n":1}` {
		t.Errorf("Output not redacted: %s", got)
	}
	if got := string(run.Status.Steps[0].ToolCalls[0].Result); got != `{"token":"[REDACTED]"}` {
		t.Errorf("Step tool-call result not redacted: %s", got)
	}

	// No policy → byte-identical fold.
	r2 := newRunReconcilerForTest(t, interceptor.Funcs{})
	run2 := sampleRun()
	r2.foldRunResult(context.Background(), run2, nil, runPodWithTerminationMessage(rr))
	if got := string(run2.Status.Output); got != `{"key":"sk-deadbeef","n":1}` {
		t.Errorf("no-policy output must be byte-identical: %s", got)
	}
}

// knative-agents-l3x: a CLI harness (claude-code) runs the provider
// credential in its own subprocess env, so its Error can echo the credential
// back verbatim on an auth failure (e.g. the CLI printing "invalid key
// sk-..." to stderr). TerminationReason must be redacted the same way
// Output/Steps already are.
func TestAgentRunReconciler_FoldRunResult_TerminationReasonRedactedForCLI(t *testing.T) {
	agent := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "tenant-a"}}
	agent.Spec.Mode = pure.ModeHarness
	agent.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessClaudeCode}

	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	rr := agentruntime.RunResult{
		Phase: pure.PhaseFailed,
		Error: "claude: authentication failed for key sk-deadbeefdeadbeef",
	}
	r.foldRunResult(context.Background(), run, agent, runPodWithTerminationMessage(rr))
	if run.Status.TerminationReason != pure.RedactionMask {
		t.Errorf("TerminationReason = %q, want %q", run.Status.TerminationReason, pure.RedactionMask)
	}
}

// An ordinary CLI failure with no secret shape (e.g. a plain "exit status 1")
// must NOT be masked — redaction only fires when a pattern actually matches,
// so routine errors stay diagnostic.
func TestAgentRunReconciler_FoldRunResult_CLIErrorWithoutSecretUnredacted(t *testing.T) {
	agent := &amv1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "tenant-a"}}
	agent.Spec.Mode = pure.ModeHarness
	agent.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessClaudeCode}

	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	rr := agentruntime.RunResult{
		Phase: pure.PhaseFailed,
		Error: "exit status 1",
	}
	r.foldRunResult(context.Background(), run, agent, runPodWithTerminationMessage(rr))
	if run.Status.TerminationReason != "exit status 1" {
		t.Errorf("TerminationReason = %q, want verbatim %q", run.Status.TerminationReason, "exit status 1")
	}
}

// A blind (HTTP) kind such as hermes never lets the harness subprocess touch
// the credential, so its Error is left unredacted even when it happens to
// contain a secret-shaped substring — proving the gate is keyed on kind, not
// blanket-applied.
func TestAgentRunReconciler_FoldRunResult_BlindKindErrorUnredacted(t *testing.T) {
	agent := harnessAgent("alice", "tenant-a") // hermes kind
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	rr := agentruntime.RunResult{
		Phase: pure.PhaseFailed,
		Error: "gateway rejected token sk-deadbeefdeadbeef",
	}
	r.foldRunResult(context.Background(), run, agent, runPodWithTerminationMessage(rr))
	if run.Status.TerminationReason != "gateway rejected token sk-deadbeefdeadbeef" {
		t.Errorf("blind-kind TerminationReason must stay verbatim, got %q", run.Status.TerminationReason)
	}
}

// M2.2: the run's trace summary folds into Status.Trace.
func TestAgentRunReconciler_FoldRunResult_Trace(t *testing.T) {
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	pod := runPodWithTerminationMessage(agentruntime.RunResult{
		Phase: pure.PhaseCompleted,
		Trace: &pure.TraceSummary{StepCount: 3, ToolCallCount: 5, Truncated: true},
	})
	r.foldRunResult(context.Background(), run, nil, pod)
	if run.Status.Trace == nil || run.Status.Trace.StepCount != 3 || run.Status.Trace.ToolCallCount != 5 || !run.Status.Trace.Truncated {
		t.Fatalf("Status.Trace = %+v, want {3,5,truncated}", run.Status.Trace)
	}
}

// A clean RunResult (no Error, no TerminationReason) must not clobber what
// markTerminal already set — for a normal Completed run that's the empty
// string, and the status should land empty.
func TestAgentRunReconciler_FoldRunResult_CleanSuccessLeavesEmpty(t *testing.T) {
	r := newRunReconcilerForTest(t, interceptor.Funcs{})
	run := sampleRun()
	r.markPending(run, "PodPending", "Pod is Pending")
	r.markTerminal(run, pure.PhaseCompleted, "")
	pod := runPodWithTerminationMessage(agentruntime.RunResult{
		Phase:  pure.PhaseCompleted,
		Output: json.RawMessage(`{"answer":42}`),
		Steps:  []pure.Step{{Index: 0, Kind: pure.StepFinal, TokensIn: 60, TokensOut: 40}},
		Usage:  pure.Usage{Steps: 1, Tokens: 100},
	})
	r.foldRunResult(context.Background(), run, nil, pod)
	if run.Status.TerminationReason != "" {
		t.Errorf("clean success should leave empty terminationReason, got %q", run.Status.TerminationReason)
	}
	if string(run.Status.Output) != `{"answer":42}` {
		t.Errorf("output not folded: %s", run.Status.Output)
	}
	if len(run.Status.Steps) != 1 || run.Status.Steps[0].Kind != pure.StepFinal {
		t.Errorf("steps not folded into status: %+v", run.Status.Steps)
	}
}

// newA2ARunReconcilerForTest builds a reconciler whose scheme also carries
// networkingv1 (egress NetworkPolicy) and rbacv1 (A2A Role lookup), and that
// permits the runc runtime class (AllowHostRuntime) so Reconcile runs all the
// way to pod creation without needing a registered kata RuntimeClass /
// AgentNodePool.
func newA2ARunReconcilerForTest(t *testing.T, objs ...client.Object) *AgentRunReconciler {
	t.Helper()
	sch := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, amv1.AddToScheme, networkingv1.AddToScheme, rbacv1.AddToScheme,
	} {
		if err := add(sch); err != nil {
			t.Fatalf("scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(objs...).
		WithStatusSubresource(&amv1.AgentRun{}).Build()
	return &AgentRunReconciler{Client: c, Scheme: sch, AllowHostRuntime: true}
}

// knative-agents-qzy: the run pod's apiserver token (AutomountServiceAccountToken)
// must default to off, and flip on only for an Agent whose A2A Role exists (the
// authoritative signal it declares a kind=agent tool) — never based on any other
// heuristic.
func TestAgentRunReconciler_A2AGrant_ControlsAutomountToken(t *testing.T) {
	t.Run("no A2A Role keeps automount false", func(t *testing.T) {
		agent := harnessAgent("alice", "tenant-a")
		agent.Spec.Sandbox.RuntimeClass = "runc"
		run := sampleRun()
		r := newA2ARunReconcilerForTest(t, agent, run)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		pod := &corev1.Pod{}
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, pod); err != nil {
			t.Fatalf("get pod: %v", err)
		}
		if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
			t.Errorf("automount = %v, want false (no A2A Role)", pod.Spec.AutomountServiceAccountToken)
		}
	})

	t.Run("A2A Role present flips automount true", func(t *testing.T) {
		agent := harnessAgent("bob", "tenant-a")
		agent.Spec.Sandbox.RuntimeClass = "runc"
		role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
			Name: builders.AgentA2ARoleName("bob"), Namespace: "tenant-a",
		}}
		run := sampleRun()
		run.Name = "run-a2a"
		run.Spec.AgentRef = "bob"
		r := newA2ARunReconcilerForTest(t, agent, role, run)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		pod := &corev1.Pod{}
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, pod); err != nil {
			t.Fatalf("get pod: %v", err)
		}
		if pod.Spec.AutomountServiceAccountToken == nil || !*pod.Spec.AutomountServiceAccountToken {
			t.Errorf("automount = %v, want true (A2A Role present)", pod.Spec.AutomountServiceAccountToken)
		}
	})
}
