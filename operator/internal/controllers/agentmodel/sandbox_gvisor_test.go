package agentmodel

import (
	"context"
	"testing"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
)

// gvisorScheme returns a scheme that knows RuntimeClass.
func gvisorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	if err := nodev1.AddToScheme(sch); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return sch
}

// TestResolveSandbox_GVisor proves the run datapath routes to gVisor as a
// HARDENED class (R-SBX-1 / R-PROV-2): when the `gvisor` RuntimeClass is
// registered the run gets runtimeClassName=gvisor without needing
// --allow-host-runtime; when it is NOT registered the run is held Pending
// (fail-closed — never a silent downgrade to the host runtime). This is the
// operator-side complement to the live pod-under-gVisor proof.
func TestResolveSandbox_GVisor(t *testing.T) {
	sch := gvisorScheme(t)
	gvisorRC := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"},
		Handler:    "runsc",
	}

	t.Run("registered gvisor routes to gvisor, no allow-host-runtime needed", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(sch).WithObjects(gvisorRC).Build()
		class, pending, failed := resolveSandbox(context.Background(), c, "gvisor", "kata-fc", false)
		if class != "gvisor" || pending != "" || failed != "" {
			t.Fatalf("got class=%q pending=%q failed=%q; want class=gvisor, no pending/failed", class, pending, failed)
		}
	})

	t.Run("unregistered gvisor is fail-closed Pending (no silent runc downgrade)", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(sch).Build() // gvisor RuntimeClass absent
		class, pending, failed := resolveSandbox(context.Background(), c, "gvisor", "kata-fc", false)
		if class != "" || pending == "" || failed != "" {
			t.Fatalf("got class=%q pending=%q failed=%q; want Pending (refuse to run unisolated)", class, pending, failed)
		}
	})

	t.Run("gvisor is hardened (allowed) while runc still needs allow-host-runtime", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(sch).WithObjects(gvisorRC).Build()
		if _, _, failed := resolveSandbox(context.Background(), c, "gvisor", "kata-fc", false); failed != "" {
			t.Errorf("gvisor must not be failed without allow-host-runtime, got failed=%q", failed)
		}
		if _, _, failed := resolveSandbox(context.Background(), c, "runc", "kata-fc", false); failed == "" {
			t.Error("runc must be failed without allow-host-runtime (fail-closed), got no failure")
		}
	})
}

// TestDangerFlagViolation_GVisorIsKataOnly proves D3 (M3.15): a harness that
// requests a danger flag (approvalMode=never) is admission-refused on the
// shared-kernel gVisor class but permitted on a kata microVM.
func TestDangerFlagViolation_GVisorIsKataOnly(t *testing.T) {
	agent := &amv1.Agent{}
	agent.Spec.Harness = &pure.HarnessSpec{
		Kind: pure.HarnessClaudeCode,
		CLI:  &pure.HarnessCLISpec{ApprovalMode: "never"},
	}

	if reason := dangerFlagViolation(agent, "gvisor"); reason == "" {
		t.Error("danger flag on gvisor (shared kernel) must be refused, got no violation")
	}
	if reason := dangerFlagViolation(agent, "kata-fc"); reason != "" {
		t.Errorf("danger flag on kata microVM must be permitted, got violation %q", reason)
	}

	// A harness with no danger flags is always fine, even on gvisor.
	safe := &amv1.Agent{}
	safe.Spec.Harness = &pure.HarnessSpec{Kind: pure.HarnessClaudeCode, CLI: &pure.HarnessCLISpec{}}
	if reason := dangerFlagViolation(safe, "gvisor"); reason != "" {
		t.Errorf("no-danger-flag harness on gvisor must be permitted, got %q", reason)
	}
}
