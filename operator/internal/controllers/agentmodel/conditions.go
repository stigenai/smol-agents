package agentmodel

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Standard condition types set by every agent-model controller (Agent,
// AgentRun, AgentSession, ModelGateway, AgentTeam, AgentWorkflow), mirroring
// the ad-hoc Phase/Reason/Message fields each already tracks so
// `kubectl wait --for=condition=Ready` and Argo/Flux health assessment work
// without a bespoke CRD health check. Phase/Reason/Message remain the
// human-readable summary; Conditions is additive.
const (
	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
)

// setReadyCondition upserts the Ready condition, preserving LastTransitionTime
// across reconciles when Status is unchanged (apimeta.SetStatusCondition).
func setReadyCondition(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	})
}

// setProgressingCondition upserts the Progressing condition.
func setProgressingCondition(conditions *[]metav1.Condition, generation int64, progressing bool, reason, message string) {
	status := metav1.ConditionFalse
	if progressing {
		status = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionProgressing,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	})
}
