package v1

import (
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ModelGatewaySpec declares an operator-managed model/agent gateway (yxh.2): a
// thin lifecycle+isolation wrapper that has the operator render+harden the
// gateway's Deployment+Service+config, instead of deploying it as a manual,
// unmanaged workload. Provider="hermes" is the first implementation (the
// NousResearch nousresearch/hermes-agent OpenAI-compatible gateway).
//
// The CRD is deliberately THIN: the request surface (chat/responses/runs, model,
// server-side levers) stays in the harness + the gateway's own config file —
// only the deployment lifecycle and isolation are modeled here. A gateway is
// host-level RCE, so the operator runs it under the same sandbox + egress floor
// as run pods (kata-fc by default).
type ModelGatewaySpec struct {
	// Provider selects the gateway implementation. Only "hermes" exists today;
	// it sets the container args ("gateway run"), the API_SERVER_*/HERMES_HOME
	// env conventions, and the config-seed init container.
	// +kubebuilder:validation:Enum=hermes
	Provider string `json:"provider"`

	// Image is the gateway container image (e.g. nousresearch/hermes-agent:latest).
	Image string `json:"image"`

	// Port is the gateway's listen port. 0 defaults per provider (hermes: 8642).
	// +kubebuilder:validation:Minimum=0
	// +optional
	Port int32 `json:"port,omitempty"`

	// Config is the gateway's config-file content, rendered into a ConfigMap and
	// seeded into the gateway's data dir by an init container. For hermes this is
	// config.yaml (the model source + server-side levers). +optional
	Config string `json:"config,omitempty"`

	// Env are extra environment variables for the gateway container — provider
	// keys (via secretRef, broker/agent-blind), base URLs, model selectors. The
	// operator adds the provider's own conventions on top. +optional
	Env []HarnessEnvVar `json:"env,omitempty"`

	// Sandbox overrides the pod RuntimeClass. Empty inherits the operator's
	// --default-run-runtime-class (kata-fc). The gateway is RCE, so a microVM is
	// recommended; runc requires the operator's --allow-host-runtime. +optional
	Sandbox SandboxSpec `json:"sandbox,omitempty"`

	// Replicas is the gateway Deployment replica count (default 1). +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// UI opts the gateway's web UI (e.g. the Hermes dashboard) into a human-facing,
	// authenticated exposure. Off by default — the gateway port is machine-only
	// (agents dial it cross-pod with a bearer token). +optional
	UI *GatewayUISpec `json:"ui,omitempty"`
}

// GatewayUISpec exposes the gateway's web UI to humans behind a platform-managed
// auth front. The operator renders an auth-proxy sidecar in front of the gateway
// port plus a dedicated "<name>-ui" Service; a human reaches it via port-forward
// (today) or a real ingress (later). Generic across providers (Hermes dashboard,
// pi HTTP, …) — the gateway is unchanged; only the front is added.
type GatewayUISpec struct {
	// Expose turns on the authenticated UI surface. +optional
	Expose bool `json:"expose,omitempty"`

	// Port is the auth-proxy listen port (the UI Service port). 0 defaults to 8643.
	// Must differ from the gateway port. +kubebuilder:validation:Minimum=0 +optional
	Port int32 `json:"port,omitempty"`

	// UpstreamPort is the in-pod port the auth-front proxies to — the gateway's
	// UI/dashboard, which may differ from the API port. 0 uses the provider default
	// (hermes: the 9119 dashboard, which the operator also enables loopback-bound),
	// falling back to the gateway port if the provider has none.
	// +kubebuilder:validation:Minimum=0 +optional
	UpstreamPort int32 `json:"upstreamPort,omitempty"`

	// Auth selects how humans authenticate to the UI.
	Auth GatewayUIAuth `json:"auth"`
}

// GatewayUIAuth is the human-auth front for the gateway UI.
type GatewayUIAuth struct {
	// Mode selects the auth front. "sharedSecret" = HTTP basic-auth from an
	// htpasswd Secret. "oidcProxy" = an oauth2-proxy sidecar that authenticates
	// humans against an OIDC IdP (bundled Dex/Keycloak per decision D9), cookie-based.
	// "oidcNative" = no sidecar; the gateway's own dashboard authenticates directly
	// against the OIDC IdP (Hermes' bundled self_hosted PKCE provider). Native mode
	// is required for dashboards whose real-time transport (WebSocket/SSE) only works
	// under the gateway's own gated cookie session — a fronting proxy can't carry it.
	// +kubebuilder:validation:Enum=sharedSecret;oidcProxy;oidcNative
	Mode string `json:"mode"`

	// SecretRef points at the auth material. For sharedSecret: a Secret whose key
	// (default "htpasswd") holds one or more htpasswd lines (user:bcrypt). +optional
	SecretRef *AuthRef `json:"secretRef,omitempty"`

	// OIDC configures mode=oidcProxy and mode=oidcNative. +optional
	OIDC *GatewayUIOIDC `json:"oidc,omitempty"`
}

// GatewayUIOIDC configures the oauth2-proxy sidecar (mode=oidcProxy). The proxy
// fronts the loopback dashboard with a cookie session minted after an OIDC login.
// Front-channel (login/redirect) uses the public issuer; the back-channel
// (token/jwks) can be pinned to in-cluster URLs so the proxy never has to trust
// the issuer's TLS — handy when the IdP sits behind a private-CA gateway.
type GatewayUIOIDC struct {
	// Issuer is the OIDC issuer URL (iss), e.g. https://dex.example.com.
	Issuer string `json:"issuer"`
	// ClientID is the OIDC client id registered at the IdP.
	ClientID string `json:"clientID"`
	// RedirectURL is the public callback the IdP redirects back to. oidcProxy:
	// https://hermes.example.com/oauth2/callback. oidcNative: the dashboard's own
	// callback https://hermes.example.com/auth/callback — the operator derives the
	// dashboard's public base URL (HERMES_DASHBOARD_PUBLIC_URL) from its origin.
	RedirectURL string `json:"redirectURL"`
	// SecretRef is a Secret holding "client-secret" and "cookie-secret" keys.
	// Required for oidcProxy; unused for oidcNative (a public PKCE client). +optional
	SecretRef *AuthRef `json:"secretRef,omitempty"`
	// LoginURL/RedeemURL/JWKSURL pin the OIDC endpoints (skips discovery). Set the
	// back-channel ones to in-cluster URLs (e.g. http://dex.dex.svc:5556/token) so
	// the proxy avoids the issuer's private-CA TLS. All-or-nothing. +optional
	LoginURL  string `json:"loginURL,omitempty"`
	RedeemURL string `json:"redeemURL,omitempty"`
	JWKSURL   string `json:"jwksURL,omitempty"`
	// EmailDomain restricts which IdP emails may log in ("*" = any). Default "*".
	// +optional
	EmailDomain string `json:"emailDomain,omitempty"`
}

// GatewayUIDefaultPort is the default auth-proxy listen port.
const GatewayUIDefaultPort int32 = 8643

// EffectiveUIPort resolves the UI auth-proxy listen port, defaulting to 8643.
func (s ModelGatewaySpec) EffectiveUIPort() int32 {
	if s.UI != nil && s.UI.Port > 0 {
		return s.UI.Port
	}
	return GatewayUIDefaultPort
}

// ModelGatewayStatus is the observed gateway state.
type ModelGatewayStatus struct {
	// Phase: Pending | Ready | Failed.
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Endpoint is the in-cluster base URL agents point harness.http.url at
	// (append the API path, e.g. /v1/chat/completions). Empty until the gateway
	// Service is rendered. +optional
	Endpoint string `json:"endpoint,omitempty"`
	// UIEndpoint is the in-cluster base URL of the authenticated UI Service (set
	// only when spec.ui.expose=true). Port-forward this to reach the dashboard.
	// +optional
	UIEndpoint string `json:"uiEndpoint,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`

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

// HermesDefaultPort is the nousresearch/hermes-agent API server port.
const HermesDefaultPort int32 = 8642

// EffectivePort resolves the gateway listen port, defaulting per provider.
func (s ModelGatewaySpec) EffectivePort() int32 {
	if s.Port > 0 {
		return s.Port
	}
	if s.Provider == "hermes" {
		return HermesDefaultPort
	}
	return 0
}

// firstSecretLine scans config line by line for the first line matching one of
// DefaultPatterns() (the same well-known credential shapes redact.go uses),
// returning its 1-based line number. It reuses DefaultPatterns() rather than a
// second pattern list so the two checks can never drift apart.
func firstSecretLine(config string) (int, bool) {
	if config == "" {
		return 0, false
	}
	pats := DefaultPatterns()
	for i, line := range strings.Split(config, "\n") {
		if anyMatch(line, pats) {
			return i + 1, true
		}
	}
	return 0, false
}

// ValidateModelGateway enforces the spec invariants: a known provider, an image,
// a resolvable port, and well-formed env (a secretRef OR a literal, not both).
func ValidateModelGateway(s ModelGatewaySpec) error {
	var errs []error
	switch s.Provider {
	case "hermes":
	case "":
		errs = append(errs, errors.New("modelGateway.provider is required"))
	default:
		errs = append(errs, fmt.Errorf("modelGateway.provider=%q is invalid (only hermes today)", s.Provider))
	}
	if strings.TrimSpace(s.Image) == "" {
		errs = append(errs, errors.New("modelGateway.image is required"))
	}
	if s.EffectivePort() <= 0 {
		errs = append(errs, errors.New("modelGateway.port is required (no provider default)"))
	}
	if line, ok := firstSecretLine(s.Config); ok {
		// Never include the matched value or the offending line's content here:
		// this error lands verbatim in ModelGatewayStatus.Message (a plaintext,
		// widely-readable object), so echoing the match would recreate the exact
		// secret disclosure this validation exists to prevent.
		errs = append(errs, fmt.Errorf("modelGateway.config line %d contains a secret-shaped value; provider credentials must come from spec.env[].secretRef, never inline", line))
	}
	for i, e := range s.Env {
		if strings.TrimSpace(e.Name) == "" {
			errs = append(errs, fmt.Errorf("modelGateway.env[%d].name is required", i))
		}
		if e.SecretRef != nil && strings.TrimSpace(e.SecretRef.SecretName) == "" {
			errs = append(errs, fmt.Errorf("modelGateway.env[%d].secretRef.secretName is required", i))
		}
	}
	if s.UI != nil && s.UI.Expose {
		switch s.UI.Auth.Mode {
		case "sharedSecret":
			if s.UI.Auth.SecretRef == nil || strings.TrimSpace(s.UI.Auth.SecretRef.SecretName) == "" {
				errs = append(errs, errors.New("modelGateway.ui.auth.secretRef.secretName is required for mode=sharedSecret"))
			}
		case "oidcProxy":
			o := s.UI.Auth.OIDC
			if o == nil {
				errs = append(errs, errors.New("modelGateway.ui.auth.oidc is required for mode=oidcProxy"))
			} else {
				if strings.TrimSpace(o.Issuer) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.issuer is required"))
				}
				if strings.TrimSpace(o.ClientID) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.clientID is required"))
				}
				if strings.TrimSpace(o.RedirectURL) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.redirectURL is required"))
				}
				if o.SecretRef == nil || strings.TrimSpace(o.SecretRef.SecretName) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.secretRef.secretName is required (client-secret + cookie-secret)"))
				}
				// Pinned back-channel endpoints are all-or-nothing.
				pinned := 0
				for _, u := range []string{o.LoginURL, o.RedeemURL, o.JWKSURL} {
					if strings.TrimSpace(u) != "" {
						pinned++
					}
				}
				if pinned != 0 && pinned != 3 {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc loginURL/redeemURL/jwksURL must be set together (or all empty for discovery)"))
				}
			}
		case "oidcNative":
			// The dashboard is its own OIDC RP (Hermes self_hosted PKCE, public
			// client): needs issuer + clientID + a redirect URL (whose origin becomes
			// HERMES_DASHBOARD_PUBLIC_URL). No client secret (public PKCE); no pinned
			// back-channel (the plugin discovers endpoints from the issuer).
			o := s.UI.Auth.OIDC
			if o == nil {
				errs = append(errs, errors.New("modelGateway.ui.auth.oidc is required for mode=oidcNative"))
			} else {
				if strings.TrimSpace(o.Issuer) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.issuer is required"))
				}
				if strings.TrimSpace(o.ClientID) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.clientID is required"))
				}
				if strings.TrimSpace(o.RedirectURL) == "" {
					errs = append(errs, errors.New("modelGateway.ui.auth.oidc.redirectURL is required"))
				}
			}
		case "":
			errs = append(errs, errors.New("modelGateway.ui.auth.mode is required when ui.expose=true"))
		default:
			errs = append(errs, fmt.Errorf("modelGateway.ui.auth.mode=%q is invalid (sharedSecret|oidcProxy|oidcNative)", s.UI.Auth.Mode))
		}
		if s.EffectiveUIPort() == s.EffectivePort() {
			errs = append(errs, errors.New("modelGateway.ui.port must differ from the gateway port"))
		}
	}
	return errors.Join(errs...)
}
