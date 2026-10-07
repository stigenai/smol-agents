package agentmodel

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	amv1 "github.com/smol-platform/smol-agents/operator/api/agentmodel/v1"
	"github.com/smol-platform/smol-agents/operator/internal/builders"
	pure "github.com/smol-platform/smol-agents/pkg/agentmodel/v1"
	"github.com/smol-platform/smol-agents/pkg/agentnet/plan"
)

// ModelGatewayReconciler renders an operator-managed model/agent gateway (yxh.2):
// a hardened Deployment + Service + config ConfigMap + egress/ingress
// NetworkPolicies from a ModelGateway CR. The gateway is host-level RCE, so it
// runs under the same sandbox (kata-fc default, fail-closed) + egress floor as
// run pods. Provider="hermes" is implementation #1.
type ModelGatewayReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// DefaultRunRuntimeClass + AllowHostRuntime mirror the AgentRun reconciler:
	// the gateway inherits the run-pod isolation policy.
	DefaultRunRuntimeClass string
	AllowHostRuntime       bool
}

// SetupWithManager wires the controller; Owns() re-reconciles the gateway when
// an owned child (esp. the Deployment's availability) changes.
func (r *ModelGatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&amv1.ModelGateway{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}

func (r *ModelGatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("modelgateway", req.NamespacedName)

	gw := &amv1.ModelGateway{}
	if err := r.Get(ctx, req.NamespacedName, gw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	prev := gw.Status

	if err := pure.ValidateModelGateway(gw.Spec); err != nil {
		return r.writeStatus(ctx, gw, prev, "Failed", "InvalidSpec", err.Error())
	}

	// Tenant boundary (5vr): every Secret the spec references must carry
	// TenantSecretLabel, or the operator refuses to render the gateway.
	if err := verifyGatewaySecrets(ctx, r.Client, gw); err != nil {
		return r.writeStatus(ctx, gw, prev, "Failed", "SecretMissing", err.Error())
	}

	// Resolve the sandbox fail-closed (same policy as run pods): the RCE gateway
	// must not schedule unisolated. failed = runc without --allow-host-runtime;
	// pending = the hardened RuntimeClass isn't registered yet.
	class, pending, failed := resolveSandbox(ctx, r.Client, gw.Spec.Sandbox.RuntimeClass, r.DefaultRunRuntimeClass, r.AllowHostRuntime)
	if failed != "" {
		return r.writeStatus(ctx, gw, prev, "Failed", "SandboxRefused", failed)
	}
	if pending != "" {
		return r.writeStatus(ctx, gw, prev, "Pending", "SandboxPending", pending)
	}

	sel := map[string]string{"runtime.agents.smol-agents.ai/modelgateway": gw.Name}
	objs := []client.Object{
		builders.BuildModelGatewayConfigMap(gw),
		builders.BuildModelGatewayDeployment(gw, class),
		builders.BuildModelGatewayService(gw),
		builders.BuildEgressPolicyWithPlan(builders.ModelGatewayName(gw)+"-egress", gw.Namespace, "modelgateway", sel, plan.NetworkPlan{}),
		builders.BuildModelGatewayIngress(gw),
	}
	if gw.Spec.UI != nil && gw.Spec.UI.Expose {
		objs = append(objs, builders.BuildModelGatewayUIService(gw))
	}
	for _, o := range objs {
		if err := r.ownAndApply(ctx, gw, o); err != nil {
			return ctrl.Result{}, err
		}
	}

	gw.Status.Endpoint = builders.ModelGatewayEndpoint(gw)
	gw.Status.UIEndpoint = builders.ModelGatewayUIEndpoint(gw)

	// Ready once the Deployment reports an available replica; otherwise Pending
	// (the Owns(Deployment) watch re-reconciles on availability change).
	var dep appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{Namespace: gw.Namespace, Name: builders.ModelGatewayName(gw)}, &dep); err == nil && dep.Status.AvailableReplicas >= 1 {
		gw.Status.Phase, gw.Status.Reason, gw.Status.Message = "Ready", "Reconciled", ""
	} else {
		gw.Status.Phase, gw.Status.Reason, gw.Status.Message = "Pending", "Deploying", "gateway Deployment not yet available"
	}
	gw.Status.ObservedGeneration = gw.Generation
	setGatewayConditions(gw, gw.Status.Phase, gw.Status.Reason, gw.Status.Message)
	logger.Info("modelgateway reconciled", "provider", gw.Spec.Provider, "phase", gw.Status.Phase, "endpoint", gw.Status.Endpoint)
	return ctrl.Result{}, r.statusUpdateIfChanged(ctx, gw, prev)
}

// ownAndApply sets the controller ref on a rendered child and creates-or-updates
// it (overwriting the operator-managed spec, preserving apiserver-managed fields).
func (r *ModelGatewayReconciler) ownAndApply(ctx context.Context, gw *amv1.ModelGateway, desired client.Object) error {
	if err := controllerutil.SetControllerReference(gw, desired, r.Scheme); err != nil {
		return fmt.Errorf("set controller ref on %T %s: %w", desired, desired.GetName(), err)
	}
	live := desired.DeepCopyObject().(client.Object)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, live, func() error {
		switch d := desired.(type) {
		case *appsv1.Deployment:
			l := live.(*appsv1.Deployment)
			l.Spec = d.Spec
			l.Labels = d.Labels
		case *corev1.Service:
			l := live.(*corev1.Service)
			clusterIP := l.Spec.ClusterIP
			l.Spec = d.Spec
			if l.Spec.ClusterIP == "" {
				l.Spec.ClusterIP = clusterIP
			}
			l.Labels = d.Labels
		case *corev1.ConfigMap:
			l := live.(*corev1.ConfigMap)
			l.Data = d.Data
			l.Labels = d.Labels
		case *networkingv1.NetworkPolicy:
			l := live.(*networkingv1.NetworkPolicy)
			l.Spec = d.Spec
			l.Labels = d.Labels
		}
		// Re-assert ownership on the live object (CreateOrUpdate fetched it).
		return controllerutil.SetControllerReference(gw, live, r.Scheme)
	})
	return err
}

// verifyGatewaySecrets enforces the tenant boundary (5vr) on every Secret the
// gateway spec references — the env secretRefs (modelGatewayUserEnv) and the
// UI shared-secret / OIDC auth secretRefs — all resolved in the gateway's own
// namespace. A nil/empty ref is skipped (that auth surface isn't configured).
func verifyGatewaySecrets(ctx context.Context, c client.Client, gw *amv1.ModelGateway) error {
	for _, e := range gw.Spec.Env {
		if e.SecretRef == nil || e.SecretRef.SecretName == "" {
			continue
		}
		if err := verifyTenantSecret(ctx, c, gw.Namespace, e.SecretRef.SecretName); err != nil {
			return err
		}
	}
	if gw.Spec.UI == nil {
		return nil
	}
	auth := gw.Spec.UI.Auth
	if auth.SecretRef != nil && auth.SecretRef.SecretName != "" {
		if err := verifyTenantSecret(ctx, c, gw.Namespace, auth.SecretRef.SecretName); err != nil {
			return err
		}
	}
	if auth.OIDC != nil && auth.OIDC.SecretRef != nil && auth.OIDC.SecretRef.SecretName != "" {
		if err := verifyTenantSecret(ctx, c, gw.Namespace, auth.OIDC.SecretRef.SecretName); err != nil {
			return err
		}
	}
	return nil
}

func (r *ModelGatewayReconciler) writeStatus(ctx context.Context, gw *amv1.ModelGateway, prev pure.ModelGatewayStatus, phase, reason, msg string) (ctrl.Result, error) {
	gw.Status.Phase, gw.Status.Reason, gw.Status.Message = phase, reason, msg
	gw.Status.ObservedGeneration = gw.Generation
	setGatewayConditions(gw, phase, reason, msg)
	return ctrl.Result{}, r.statusUpdateIfChanged(ctx, gw, prev)
}

// setGatewayConditions upserts the standard Ready/Progressing conditions from
// the gateway's ad-hoc Phase/Reason/Message.
func setGatewayConditions(gw *amv1.ModelGateway, phase, reason, msg string) {
	ready := metav1.ConditionFalse
	if phase == "Ready" {
		ready = metav1.ConditionTrue
	}
	setReadyCondition(&gw.Status.Conditions, gw.Generation, ready, reason, msg)
	setProgressingCondition(&gw.Status.Conditions, gw.Generation, phase == "Pending", reason, msg)
}

func (r *ModelGatewayReconciler) statusUpdateIfChanged(ctx context.Context, gw *amv1.ModelGateway, prev pure.ModelGatewayStatus) error {
	if equality.Semantic.DeepEqual(prev, gw.Status) {
		return nil
	}
	return r.Status().Update(ctx, gw)
}
