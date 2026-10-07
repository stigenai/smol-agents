package v1

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ModelRef points at a `ModelProvider` plus a model name on it.
type ModelRef struct {
	// ProviderRef is the namespace-local name of a ModelProvider.
	ProviderRef string `json:"providerRef"`

	// Name is the provider-specific model identifier.
	Name string `json:"name"`

	// Parameters that are forwarded to the provider verbatim.
	Temperature     *float64 `json:"temperature,omitempty"`
	TopP            *float64 `json:"topP,omitempty"`
	MaxOutputTokens *int32   `json:"maxOutputTokens,omitempty"`
}

// ToolRef references a Tool by name. The Agent's allow-list of tools.
type ToolRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"` // empty = same as Agent
}

// IdentitySpec configures the SPIFFE binding for the Run Pod. R-AM-SEC-1.
type IdentitySpec struct {
	// SPIFFEIDPrefix lets tenants share a sub-tree (e.g. group runs by team).
	// The platform appends `/agent/<name>/run/<runid>`.
	SPIFFEIDPrefix string `json:"spiffeIDPrefix,omitempty"`
}

// SandboxSpec configures the pod RuntimeClass (R-AM-SEC-2). Empty inherits the
// operator's --default-run-runtime-class (default kata-fc); runc is rejected
// fail-closed unless the operator runs with --allow-host-runtime. AgentRun has
// no per-run override — it inherits the referenced Agent's sandbox.
type SandboxSpec struct {
	// RuntimeClass overrides the pod RuntimeClassName. Empty = operator default.
	RuntimeClass string `json:"runtimeClass,omitempty"`
}

// AgentSpec — what the agent IS. Implements R-AM-API-1.
type AgentSpec struct {
	// Mode selects the execution path: "loop" (default) or "harness".
	// +optional
	Mode AgentMode `json:"mode,omitempty"`

	// Model is required when Mode==loop and ignored when Mode==harness.
	Model ModelRef `json:"model,omitempty"`

	// Harness is required when Mode==harness and ignored otherwise.
	// +optional
	Harness *HarnessSpec `json:"harness,omitempty"`

	Instructions string `json:"instructions"`

	// Tools is the Agent's allow-list of Tool references. NOTE: loop-mode tool
	// invocation is NOT wired end-to-end as of v0.2.0 — the operator resolves
	// these names but never ships the specs to the pod and the executor has no
	// invokers, so a loop agent's tool call is rejected at runtime. Only
	// harness-mode agents act on tools (via embedded harness logic). See
	// docs/design/tool-kinds-roadmap.md.
	Tools []ToolRef `json:"tools,omitempty"`

	// Storage attaches persistent state (AgentFS today). Required for
	// SessionPersistent harnesses.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	Budget   Budget       `json:"budget"`
	Identity IdentitySpec `json:"identity,omitempty"`
	Sandbox  SandboxSpec  `json:"sandbox,omitempty"`

	// Approval, when set, gates this Agent's runs behind a human pre-run
	// approval (M5). +optional
	Approval *ApprovalPolicy `json:"approval,omitempty"`

	// MaxConcurrentRuns caps how many of this Agent's runs may be Running at
	// once (0 = unlimited). Enforced as a soft admission gate (D10). +optional
	// +kubebuilder:validation:Minimum=0
	MaxConcurrentRuns int32 `json:"maxConcurrentRuns,omitempty"`

	// Session, when set, marks the Agent as requiring a resident session and/or
	// an interactive attach plane (D4). +optional
	Session *SessionSpec `json:"session,omitempty"`

	// Artifacts declares workspace files to publish to S3 on pod shutdown (M2.23,
	// requires AgentFS storage). +optional
	Artifacts *ArtifactSpec `json:"artifacts,omitempty"`

	// ResultSink, when set, is a URI the operator POSTs a CloudEvent to when one of
	// this Agent's runs completes successfully (wbb) — the platform as an event
	// SOURCE, so agent outputs can drive downstream Knative Triggers. Emitted
	// at-least-once with a stable ce-id (the run UID), so consumers dedupe. Empty =
	// no emission. +optional
	ResultSink string `json:"resultSink,omitempty"`
}

// AgentStatus is reported by the controller.
type AgentStatus struct {
	Phase              string   `json:"phase,omitempty"` // "Pending" | "Ready" | "Failed"
	ObservedGeneration int64    `json:"observedGeneration,omitempty"`
	ResolvedTools      []string `json:"resolvedTools,omitempty"`
	ResolvedProvider   string   `json:"resolvedProvider,omitempty"`
	Reason             string   `json:"reason,omitempty"`
	Message            string   `json:"message,omitempty"`

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

// Agent is the namespaced top-level CR.
type Agent struct {
	Spec   AgentSpec   `json:"spec"`
	Status AgentStatus `json:"status,omitempty"`
}

// ToolKind enumerates the transports the runtime knows how to drive.
// R-AM-API-2.
type ToolKind string

const (
	ToolMCP  ToolKind = "mcp"  // external MCP server (no production invoker yet)
	ToolHTTP ToolKind = "http" // generic HTTP+JSON endpoint (no production invoker yet)
	// ToolAgent (agent-to-agent) and ToolFunction (in-process) are RESERVED: no
	// production invoker exists and loop-mode tool invocation is unimplemented as
	// of v0.2.0 (ToolFunction is test-only). See docs/design/tool-kinds-roadmap.md.
	ToolAgent    ToolKind = "agent"
	ToolFunction ToolKind = "function"
	// ToolTask is the shared team task list (multi-agent orchestration P1): the
	// TaskInvoker lets a team member list/claim/complete work on the team's NATS
	// KV task list. Its invoker is wired only inside a team context
	// (WireTaskInvoker) — outside a team the call fail-closes, like A2A.
	ToolTask ToolKind = "task"
	// ToolTeammate is the peer mailbox (multi-agent orchestration P2): the
	// TeammateInvoker lets a member message another member by name + drain its own
	// inbox. Wired only inside a team context (WireTeammateInvoker); a per-member
	// NATS credential makes "read only your own inbox" the enforced boundary.
	ToolTeammate ToolKind = "teammate"
	// ToolTeamBus is the team message bus (multi-agent orchestration P5): the
	// TeamBusInvoker lets a member publish/subscribe team topics for emergent
	// pub/sub workflows. Wired only inside a team context (WireTeamBusInvoker);
	// the per-member bus credential confines it to the team's bus subtree.
	ToolTeamBus ToolKind = "teambus"
	// ToolFanout is Send-style runtime map-reduce (LangGraph Send API): one tool
	// call spawns N parallel child AgentRuns over a runtime-computed list and
	// folds their outputs via a reducer. The A2A blocking-and-fold generalized
	// from 1 child to N, with a hard width cap. Wired only with in-cluster access
	// (WireFanoutInvoker), fail-closed off-cluster like A2A.
	ToolFanout ToolKind = "fanout"
)

// Valid returns true if k is a known ToolKind.
func (k ToolKind) Valid() bool {
	switch k {
	case ToolMCP, ToolHTTP, ToolAgent, ToolFunction, ToolTask, ToolTeammate, ToolTeamBus, ToolFanout:
		return true
	}
	return false
}

// SupportedLoopToolKinds is the single source of truth for the tool kinds with
// a production invoker on the loop-mode datapath: HTTP + MCP, ToolAgent (A2A —
// the AgentRunInvoker spawns a child AgentRun; M3 A1), and ToolTask (the team
// shared task list; P1). ToolFunction remains reserved (test-only invoker) and
// is rejected for loop-mode agents — fail-closed (D3) rather than silently
// no-op'ing the call.
func SupportedLoopToolKinds() map[ToolKind]bool {
	return map[ToolKind]bool{ToolHTTP: true, ToolMCP: true, ToolAgent: true, ToolTask: true, ToolTeammate: true, ToolTeamBus: true, ToolFanout: true}
}

// MCPSpec describes an MCP transport target. R-AM-TOOL-3.
type MCPSpec struct {
	URL  string   `json:"url"`            // mcp://… or http(s)://…/mcp
	Auth *AuthRef `json:"auth,omitempty"` // optional secret broker reference
}

// HTTPSpec describes a generic HTTP+JSON tool target.
type HTTPSpec struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"` // default POST
	Auth    *AuthRef          `json:"auth,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// AgentTargetSpec lets one Agent invoke another (A2A). MaxTokens/TimeoutSeconds
// bound each child invocation — the fork-bomb / runaway-delegation guard for
// multi-tenant A2A (D1); 0 means "inherit the caller's remaining budget".
type AgentTargetSpec struct {
	Ref ToolRef `json:"ref"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxTokens int64 `json:"maxTokens,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// FunctionSpec is for in-process functions (test only).
type FunctionSpec struct {
	Name string `json:"name"`
}

// AuthRef points at a Kubernetes Secret resolved by the operator at pod-build
// time — into the broker config (harness env) or a secretKeyRef (AgentFS backup
// creds). The raw value is never written into the pod spec.
type AuthRef struct {
	SecretName string `json:"secretName"`

	// Key is the key within the referenced Secret that holds the value. When
	// empty and the Secret has exactly one key, that key is used. Consumed by
	// the broker config generator (R-SEC) to populate the static backend.
	// +optional
	Key string `json:"key,omitempty"`
}

// ToolSpec is a discriminated union by Kind.
type ToolSpec struct {
	Kind         ToolKind         `json:"kind"`
	Description  string           `json:"description,omitempty"`
	InputSchema  json.RawMessage  `json:"inputSchema"`
	OutputSchema json.RawMessage  `json:"outputSchema"`
	MCP          *MCPSpec         `json:"mcp,omitempty"`
	HTTP         *HTTPSpec        `json:"http,omitempty"`
	Agent        *AgentTargetSpec `json:"agent,omitempty"`
	Function     *FunctionSpec    `json:"function,omitempty"`
	// Fanout configures a kind=fanout tool (Send-style runtime map-reduce). +optional
	Fanout *FanoutTargetSpec `json:"fanout,omitempty"`
}

// FanoutReducer is how a fanout folds its N child outputs into one observation.
type FanoutReducer string

const (
	// FanoutConcat returns the children's outputs as a JSON array (+ an errors
	// count for any that failed).
	FanoutConcat FanoutReducer = "concat"
	// FanoutMerge deep-merges the children's JSON objects (key-last-wins) (+ an
	// errors count).
	FanoutMerge FanoutReducer = "merge"
	// FanoutFirstSuccess returns the first Completed child's output and
	// cancel-deletes the rest.
	FanoutFirstSuccess FanoutReducer = "first-success"
)

// FanoutTargetSpec configures a kind=fanout tool: one LLM tool call spawns one
// child AgentRun (of Ref) per item in a runtime list, bounded by MaxParallel
// (and the operator's hard FANOUT_MAX_WIDTH), then folds the children's outputs
// via Reduce. The A2A child-spawn-and-fold generalized from 1 to N.
type FanoutTargetSpec struct {
	// Ref is the Agent each child runs, a bare name in the tool's namespace
	// (no cross-namespace reference, D1).
	Ref ToolRef `json:"ref"`
	// MaxParallel caps concurrent children (the run still passes the per-namespace
	// admission queue; the operator's FANOUT_MAX_WIDTH is the hard clamp). 0 → 1.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`
	// Reduce folds the children's outputs (default concat).
	// +kubebuilder:validation:Enum=concat;merge;first-success
	// +optional
	Reduce FanoutReducer `json:"reduce,omitempty"`
	// PerItemMaxTokens caps each child's tokens (a budgetOverride). 0 = the child
	// Agent's own budget. +optional
	// +kubebuilder:validation:Minimum=0
	PerItemMaxTokens int64 `json:"perItemMaxTokens,omitempty"`
}

// EffectiveReduce defaults to concat.
func (f FanoutTargetSpec) EffectiveReduce() FanoutReducer {
	if f.Reduce == "" {
		return FanoutConcat
	}
	return f.Reduce
}

// Tool is a namespaced reusable capability.
type Tool struct {
	Name string   `json:"name"`
	Spec ToolSpec `json:"spec"`
}

// ModelProvider — credentials + endpoint. R-AM-API-3.
type ModelProvider struct {
	Name   string              `json:"name"`
	Spec   ModelProviderSpec   `json:"spec"`
	Status ModelProviderStatus `json:"status,omitempty"`
}

// ModelProviderSpec holds the provider family + brokered credential. Kind drives
// credential brokering (operator/internal/controllers/agentmodel/secrets.go) and
// endpoint defaults. As of v0.2.0 the only loop-mode LLM client is the
// OpenAI-compatible one (pkg/agentruntime/openaillm); bedrock/vertex have no
// dedicated client, and anthropic is reachable only via an OpenAI/Anthropic-
// compatible endpoint or a harness (e.g. claude-code).
type ModelProviderSpec struct {
	// Kind: openai | anthropic | bedrock | vertex | local. Only openai-compatible
	// (and "local") have a working loop-mode client today.
	Kind string `json:"kind"`
	// Endpoint overrides the kind's default base URL; required for local.
	Endpoint string `json:"endpoint,omitempty"`
	// ChatPath overrides the path appended to Endpoint for chat completions
	// (default /v1/chat/completions). Set it when a provider's path differs —
	// e.g. z.ai's Coding Plan at /api/coding/paas/v4/chat/completions — so it
	// works without an in-cluster path-rewriting proxy. +optional
	ChatPath string `json:"chatPath,omitempty"`
	// SecretRef is the API-key Secret, leased by the broker at runtime — never
	// embedded in the pod spec.
	SecretRef AuthRef `json:"secretRef"`
}

// ModelProviderStatus is the observed provider state.
type ModelProviderStatus struct {
	// Phase: Ready | Pending | Failed.
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// AgentRun — single bounded execution. R-AM-API-4.
type AgentRun struct {
	Name   string       `json:"name"`
	Spec   AgentRunSpec `json:"spec"`
	Status RunStatus    `json:"status,omitempty"`
}

// AgentRunSpec is a single bounded execution of the referenced Agent. The pod
// sandbox (RuntimeClass) and egress policy are inherited from the Agent — there
// is no per-run sandbox override.
type AgentRunSpec struct {
	AgentRef string `json:"agentRef"`

	// SessionRef optionally associates this run with an AgentSession.
	SessionRef string `json:"sessionRef,omitempty"`

	// ResumeSessionID resumes a prior harness conversation (M3.19): a claude-code
	// session_id / codex thread captured in an earlier run's
	// status.harnessSessionID. The harness reloads that transcript (--resume /
	// exec resume). Empty = a fresh session. +optional
	ResumeSessionID string `json:"resumeSessionID,omitempty"`

	// Input is the run's JSON payload. It rides the run spec, which the operator
	// marshals into a ~1 MiB ConfigMap — use Inputs[].secretRef for large or
	// secret payloads instead of inlining them here.
	Input json.RawMessage `json:"input"`

	// Seed is a best-effort determinism hint forwarded to backends that honor it
	// (the OpenAI-compatible `seed` field). It is NOT a guarantee of bit-exact
	// reproduction: providers may ignore it under load, and temperature, model
	// version drift, and gateway-side loops all reintroduce nondeterminism. For
	// exact offline reproduction, use record/replay (see
	// docs/specs/determinism-and-replay.md). 0 omits the field entirely.
	Seed int64 `json:"seed,omitempty"`

	// BudgetOverride escalates the Agent's budget for THIS run. In harness mode
	// only maxTokens/maxWallClockSeconds bind at runtime; maxSteps and
	// maxToolCalls are computed post-hoc from the harness result.
	BudgetOverride *Budget `json:"budgetOverride,omitempty"`

	Cancel bool `json:"cancel,omitempty"`

	// PlacementFallback controls what happens when the run's sandbox class is a
	// microVM (kata) but no AgentNodePool provides that isolation. "Pending"
	// (default, fail-closed) holds the run until capacity appears; "Schedule"
	// lets it schedule without node affinity (a dev/unlabelled-cluster escape
	// hatch — the RuntimeClass is still pinned).
	// +kubebuilder:validation:Enum=Pending;Schedule
	// +optional
	PlacementFallback string `json:"placementFallback,omitempty"`

	// Decision is a human's pre-run approval verdict (M5), patched onto a run
	// that is waiting in RequiresAction. +optional
	Decision *Decision `json:"decision,omitempty"`

	// RequireApprovalBeforeRun overrides the Agent's ApprovalPolicy for THIS run
	// (nil = inherit). +optional
	RequireApprovalBeforeRun *bool `json:"requireApprovalBeforeRun,omitempty"`

	// Priority orders this run in the per-namespace admission queue (M1.13) when
	// the namespace is at its concurrency cap: higher priority is admitted first,
	// ties broken by creation time (oldest first). Clamped to the operator's
	// MaxPriority. 0 (default) is normal priority. +optional
	Priority int32 `json:"priority,omitempty"`

	// MemoryRetrieverRef is the namespace-local name of a MemoryRetriever CR
	// whose filesystem store should be mounted into the agent pod when
	// MemoryRetriever.spec.mount.enabled is true (R-MEM-FS-2).
	// The run controller resolves this to a MemoryRetriever and calls
	// builders.AttachMemoryFS on the pod before creating it.
	// Empty means no memory filesystem is mounted.
	// +optional
	MemoryRetrieverRef string `json:"memoryRetrieverRef,omitempty"`

	// Inputs are files materialized into the agent's workspace before the run
	// executes, so a harness/loop can work on "the files I gave you". Requires a
	// workspace (the Agent's storage.agentfs mount or harness.cli.workingDir);
	// a run with inputs but no workspace fails loud. Large or secret payloads
	// should use secretRef rather than inline (inputs ride the run spec, which
	// the operator marshals into a ~1 MiB ConfigMap).
	// +optional
	Inputs []RunInputFile `json:"inputs,omitempty"`
}

// RunInputFile is a single file seeded into the run workspace. Exactly one
// source must be set: Inline (UTF-8 text), InlineBase64 (binary), or SecretRef
// (leased from the broker at runtime, never written into the AgentRun spec).
type RunInputFile struct {
	// Path is the destination relative to the workspace root. Must be relative
	// with no ".." segment.
	Path string `json:"path"`

	// Inline is literal UTF-8 file content.
	// +optional
	Inline string `json:"inline,omitempty"`

	// InlineBase64 is base64-encoded file content (for binary payloads).
	// +optional
	InlineBase64 string `json:"inlineBase64,omitempty"`

	// SecretRef leases the file content from the broker at runtime, so the value
	// never sits in the AgentRun spec.
	// +optional
	SecretRef *AuthRef `json:"secretRef,omitempty"`
}

// Step is a single plan-act-observe iteration.
type Step struct {
	Index     int32            `json:"index"`
	Kind      StepKind         `json:"kind"`
	StartedAt metav1.Time      `json:"startedAt"`
	EndedAt   metav1.Time      `json:"endedAt,omitempty"`
	TokensIn  int64            `json:"tokensIn"`
	TokensOut int64            `json:"tokensOut"`
	ToolCalls []ToolCallRecord `json:"toolCalls,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// StepKind discriminates Step types.
type StepKind string

const (
	StepPlan                StepKind = "Plan"                // LLM decision
	StepToolCall            StepKind = "ToolCall"            // tool invocation
	StepToolCallRejected    StepKind = "ToolCallRejected"    // failed input validation
	StepObservation         StepKind = "Observation"         // tool result accepted
	StepObservationRejected StepKind = "ObservationRejected" // tool result failed schema
	StepFinal               StepKind = "Final"               // emitted final answer
)

// ToolCallRecord captures a single tool invocation inside a Step.
type ToolCallRecord struct {
	Tool       string          `json:"tool"`
	Arguments  json.RawMessage `json:"arguments"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	DurationMs int64           `json:"durationMs"`
	// ArgsBytes / ResultBytes record the original payload sizes when the
	// termination-message clamp elides Arguments/Result, so the trace summary
	// stays honest about what was dropped (M2.3).
	ArgsBytes   int64 `json:"argsBytes,omitempty"`
	ResultBytes int64 `json:"resultBytes,omitempty"`
}

// RunStatus is the live state of an AgentRun.
type RunStatus struct {
	State             Phase           `json:"state"`
	StartedAt         *metav1.Time    `json:"startedAt,omitempty"`
	EndedAt           *metav1.Time    `json:"endedAt,omitempty"`
	Steps             []Step          `json:"steps,omitempty"`
	Usage             Usage           `json:"usage"`
	TerminationReason string          `json:"terminationReason,omitempty"`
	Output            json.RawMessage `json:"output,omitempty"`

	// Reason is a short machine-readable cause for the current State (e.g.
	// "ConcurrencyLimited", "SandboxNotReady"). Set alongside the human message
	// in TerminationReason; consumed by the M1.13 admission queue to find the
	// runs queued on concurrency. +optional
	Reason string `json:"reason,omitempty"`

	// PendingAction is set while State==RequiresAction (M5): it records the
	// approval token a human must echo in spec.decision. +optional
	PendingAction *PendingAction `json:"pendingAction,omitempty"`

	// Trace is compact step/tool-call metadata that survives even when the full
	// Steps are clamped out of the termination message (M2.2). +optional
	Trace *TraceSummary `json:"trace,omitempty"`

	// Artifacts is the folded result of workspace-file egress (M2.26): the state
	// the agentfs-sidecar reported plus per-file refs. Observability-only; nil
	// when the Agent declares no spec.artifacts. +optional
	Artifacts *ArtifactsStatus `json:"artifacts,omitempty"`

	// HarnessSessionID is the harness's own session/thread id from this run
	// (claude-code session_id, codex thread) — surfaced so a later run can resume
	// the conversation via spec.resumeSessionID (M3.19). +optional
	HarnessSessionID string `json:"harnessSessionID,omitempty"`

	// Networks are the AgentNetworks bound to this run (M1.19), empty when only
	// the default-deny egress floor applies. +optional
	Networks []string `json:"networks,omitempty"`
	// EgressEnforcement summarizes the run's egress posture: "default-deny" (the
	// floor only), "tiered" (a bound AgentNetwork allow-list layered on), or
	// "unenforced" when the cluster CNI does not enforce NetworkPolicy so the
	// created policies are silent no-ops (rv1.2). +optional
	EgressEnforcement string `json:"egressEnforcement,omitempty"`

	// Conditions follows the standard Kubernetes condition pattern (Ready /
	// Progressing), enabling `kubectl wait --for=condition=Ready` and
	// Argo/Flux health assessment. State/Reason/TerminationReason remain the
	// human-readable summary; Conditions is additive. +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// TraceSummary is compact metadata about a run's step/tool-call trace, surfaced
// in Status even when the full Steps are clamped out of the bounded termination
// message (M2.2). OverflowRef points at the full trace offloaded to object
// storage when one was written (M2.9).
type TraceSummary struct {
	StepCount     int32  `json:"stepCount"`
	ToolCallCount int32  `json:"toolCallCount"`
	Truncated     bool   `json:"truncated,omitempty"`
	DroppedBytes  int64  `json:"droppedBytes,omitempty"`
	OverflowRef   string `json:"overflowRef,omitempty"`
}

// AgentSession — long-running aggregation. R-AM-API-5.
type AgentSession struct {
	Name   string             `json:"name"`
	Spec   AgentSessionSpec   `json:"spec"`
	Status AgentSessionStatus `json:"status,omitempty"`
}

type AgentSessionSpec struct {
	AgentRef string `json:"agentRef"`

	// IdleTimeoutSeconds parks then exits the session worker after this idle
	// period so it can scale to zero; 0 (default) keeps it resident.
	// +optional
	IdleTimeoutSeconds int32 `json:"idleTimeoutSeconds,omitempty"`

	// Turn-scaling knobs (M2.17). All default-preserve today's behavior (a
	// serial, unbounded-history worker); read them via the accessors below so 0
	// means "the default", not "zero".
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrentTurns int32 `json:"maxConcurrentTurns,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	TurnRetentionSeconds int32 `json:"turnRetentionSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	TurnHistoryLimit int32 `json:"turnHistoryLimit,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxTurnInputBytes int32 `json:"maxTurnInputBytes,omitempty"`

	// TurnBatchSize is how many queued turns the worker pulls per poll (0 → 1,
	// today's one-at-a-time pull). Observability/tuning knob for the NATS path.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TurnBatchSize int32 `json:"turnBatchSize,omitempty"`
	// TurnPollIntervalMs is the worker's inbox poll cadence in ms (0 → 500ms).
	// +kubebuilder:validation:Minimum=0
	// +optional
	TurnPollIntervalMs int32 `json:"turnPollIntervalMs,omitempty"`
	// TurnDeliveryTimeoutSeconds bounds how long a single turn may take before the
	// worker abandons it: the operator renders it as serve-session --turn-timeout,
	// so turnCtx = min(this, the turn's budget) (M2.18). 0 = no worker-enforced
	// per-turn cap (today's behavior; the turn's own budget still applies). When
	// set, must be ≤ turnRetentionSeconds (M2.22).
	// +kubebuilder:validation:Minimum=0
	// +optional
	TurnDeliveryTimeoutSeconds int32 `json:"turnDeliveryTimeoutSeconds,omitempty"`

	// MemoryScope is an optional cross-turn memory partition key for Hermes
	// sessions (M3.12) — injected as HERMES_SESSION_KEY so the gateway can scope
	// (or share) provider-side memory beyond the per-session id. Empty = the
	// session-id alone scopes memory. +optional
	MemoryScope string `json:"memoryScope,omitempty"`

	// Resources is the compute request/limit for the resident session worker
	// container (M1.11). A session has NO wall-clock deadline (the idle timeout
	// bounds it), so right-sizing the worker is done here rather than via a
	// budget. nil leaves the operator's default sizing. Quantities are strings
	// (e.g. "500m", "512Mi") so the pure model stays free of k8s core/v1; the
	// operator parses them.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`
}

// ResourceRequirements is a pure (k8s-core-free) mirror of compute resources for
// a container: a map from resource name (cpu, memory, …) to a quantity string.
// The operator translates these into corev1.ResourceRequirements, parsing each
// value with resource.ParseQuantity; the AgentSession webhook rejects an
// unparseable quantity at admission.
type ResourceRequirements struct {
	// +optional
	Limits map[string]string `json:"limits,omitempty"`
	// +optional
	Requests map[string]string `json:"requests,omitempty"`
}

// ConcurrentTurns is the turn-processing width (default 1 — the proven serial
// path, so maxConcurrentTurns is opt-in).
func (s AgentSessionSpec) ConcurrentTurns() int32 {
	if s.MaxConcurrentTurns > 0 {
		return s.MaxConcurrentTurns
	}
	return 1
}

// HistoryLimit is the max retained turns in the worker's in-memory history
// (0 = unbounded, today's behavior).
func (s AgentSessionSpec) HistoryLimit() int32 { return s.TurnHistoryLimit }

// RetentionSeconds is the NATS turn-stream retention (default 3600s).
func (s AgentSessionSpec) RetentionSeconds() int32 {
	if s.TurnRetentionSeconds > 0 {
		return s.TurnRetentionSeconds
	}
	return 3600
}

// InputBytesCap is the per-turn input ceiling (default 1 MiB).
func (s AgentSessionSpec) InputBytesCap() int32 {
	if s.MaxTurnInputBytes > 0 {
		return s.MaxTurnInputBytes
	}
	return 1 << 20
}

// BatchSize is how many queued turns the worker pulls per poll (default 1).
func (s AgentSessionSpec) BatchSize() int32 {
	if s.TurnBatchSize > 0 {
		return s.TurnBatchSize
	}
	return 1
}

// PollIntervalMs is the worker's inbox poll cadence in milliseconds (default 500).
func (s AgentSessionSpec) PollIntervalMs() int32 {
	if s.TurnPollIntervalMs > 0 {
		return s.TurnPollIntervalMs
	}
	return 500
}

// DeliveryTimeoutSeconds bounds a single turn's processing time (default 300s).
func (s AgentSessionSpec) DeliveryTimeoutSeconds() int32 {
	if s.TurnDeliveryTimeoutSeconds > 0 {
		return s.TurnDeliveryTimeoutSeconds
	}
	return 300
}

type AgentSessionStatus struct {
	// Phase mirrors the session worker's lifecycle as observed by the operator
	// (Pending until the worker is available, then Running).
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// ObservedGeneration is the spec generation the controller last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Runs are the names of AgentRuns this session aggregated (legacy field).
	// +optional
	Runs []string `json:"runs,omitempty"`

	// Networks are the AgentNetworks bound to this session (M1.19), empty when
	// only the default-deny egress floor applies. +optional
	Networks []string `json:"networks,omitempty"`
	// EgressEnforcement summarizes the session's egress posture: "default-deny"
	// (the floor only), "tiered" (a bound AgentNetwork allow-list), or
	// "unenforced" when the cluster CNI does not enforce NetworkPolicy (rv1.2).
	// +optional
	EgressEnforcement string `json:"egressEnforcement,omitempty"`

	// Transport summarizes the active turn-delivery path: "nats" when the
	// operator routes turns through NATS (the gateway path), or "on-disk" when
	// it does not — a DEGRADED state in which the worker falls back to an
	// unreachable on-disk inbox, so gateway /turns never reach it. Surfacing it
	// turns a silent no-op into a visible signal (c5r.11). +optional
	Transport string `json:"transport,omitempty"`

	// Reason is a short machine-readable cause for the current Phase (e.g.
	// AgentMissing, SandboxNotReady, SecretMissing, NoKVMCapacity, Reconciled) —
	// parity with AgentRun's status so a Pending/Failed session is not opaque.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable elaboration of Reason. +optional
	Message string `json:"message,omitempty"`

	// Usage is the session's cumulative resource accounting across all turns,
	// mirrored field-wise from the worker's checkpoint (M2.19 — NOT via Usage.Add).
	// Observability only. +optional
	Usage Usage `json:"usage,omitempty"`
	// Turns is the monotonic count of turns the worker has processed (survives
	// in-memory history compaction). +optional
	Turns int64 `json:"turns,omitempty"`
	// FailedTurns is how many of those turns ended in error. +optional
	FailedTurns int64 `json:"failedTurns,omitempty"`
	// LastTurnTime is when the most recent turn completed. +optional
	LastTurnTime *metav1.Time `json:"lastTurnTime,omitempty"`

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

// AgentPolicy — guardrails. R-AM-API-6.
type AgentPolicy struct {
	Name string          `json:"name"`
	Spec AgentPolicySpec `json:"spec"`
}

type AgentPolicySpec struct {
	AllowedProviders []string         `json:"allowedProviders,omitempty"`
	AllowedTools     []string         `json:"allowedTools,omitempty"` // tool names
	MaxBudget        *Budget          `json:"maxBudget,omitempty"`
	Redaction        *RedactionPolicy `json:"redaction,omitempty"`
}

type RedactionPolicy struct {
	Patterns []string `json:"patterns,omitempty"` // regex patterns
}

// ApprovalPolicy gates an Agent's runs behind a human pre-run approval. Only the
// cheap pre-run gate ships near-term — the run pauses in RequiresAction before
// any pod or cost exists. Loop-mode mid-run continuation is deferred post-GA
// (D6), so this carries only the pre-run knobs.
type ApprovalPolicy struct {
	// RequireApprovalBeforeRun holds every run for this Agent in RequiresAction
	// until a human approves it via the run's spec.decision.
	// +optional
	RequireApprovalBeforeRun bool `json:"requireApprovalBeforeRun,omitempty"`
	// ApprovalTimeoutSeconds expires an un-decided run after this long. 0 falls
	// back to the operator's --default-approval-timeout.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ApprovalTimeoutSeconds int32 `json:"approvalTimeoutSeconds,omitempty"`
}

// SessionSpec marks an Agent as requiring a resident session and/or an
// interactive attach plane (D4). Required ⇒ a warm session pod that outlives a
// single turn; Interactive ⇒ a human attach plane (which implies Required and,
// at the serving layer, a non-Knative deployment). One-shot AgentRuns leave
// this nil.
type SessionSpec struct {
	// +optional
	Required bool `json:"required,omitempty"`
	// +optional
	Interactive bool `json:"interactive,omitempty"`
}

// Decision is a human's verdict on a gated run, patched onto the run spec. Token
// must match Status.PendingAction.Token — a stale/mismatched token is ignored so
// an approval can't apply to a different pending state.
type Decision struct {
	Token   string `json:"token"`
	Approve bool   `json:"approve"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	DecidedBy string `json:"decidedBy,omitempty"`
}

// PendingAction records why a run sits in RequiresAction. Near-term the only
// Kind is "pre-run"; loop-mode "tool-call" gating (with Tool/Arguments/StepIndex)
// is deferred post-GA (D6).
type PendingAction struct {
	Kind        string      `json:"kind"`
	Token       string      `json:"token"`
	RequestedAt metav1.Time `json:"requestedAt"`
	// +optional
	Reason string `json:"reason,omitempty"`
}
