{{/*
Expand the name of the chart.
*/}}
{{- define "loki-vl-proxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "loki-vl-proxy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
loki-vl-proxy.image — every container image reference of the chart, from a dict
of repository, tag and digest: repository:tag, repository:tag@digest or
repository@digest.
*/}}
{{- define "loki-vl-proxy.image" -}}
{{- $tag := .tag | default "" | toString -}}
{{- $digest := .digest | default "" | toString -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "image.digest %q is not sha256:<64 lowercase hex characters>" $digest) -}}
{{- end -}}
{{- if or (contains "@" (toString .repository)) (contains "@" $tag) -}}
{{- fail "image.digest is set, so image.repository and image.tag must not hold a digest of their own" -}}
{{- end -}}
{{- end -}}
{{- .repository -}}
{{- with $tag }}:{{ . }}{{ end -}}
{{- with $digest }}@{{ . }}{{ end -}}
{{- end }}

{{/*
The proxy's image. Without a digest the tag defaults to the chart's appVersion;
with one, only a tag set by the operator is kept, so the reference never names a
version the digest may not be.
*/}}
{{- define "loki-vl-proxy.proxyImage" -}}
{{- $tag := .Values.image.tag -}}
{{- if and (not $tag) (not .Values.image.digest) -}}
{{- $tag = .Chart.AppVersion -}}
{{- end -}}
{{- include "loki-vl-proxy.image" (dict "repository" .Values.image.repository "tag" $tag "digest" .Values.image.digest) -}}
{{- end }}

{{/*
Common labels
*/}}
{{- define "loki-vl-proxy.labels" -}}
helm.sh/chart: {{ include "loki-vl-proxy.name" . }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "loki-vl-proxy.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.extraLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "loki-vl-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "loki-vl-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "loki-vl-proxy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "loki-vl-proxy.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Peer-cache headless service name.
*/}}
{{- define "loki-vl-proxy.peerServiceName" -}}
{{- default (include "loki-vl-proxy.headlessServiceName" .) .Values.peerCache.serviceName }}
{{- end }}

{{/*
Resolved workload kind.
*/}}
{{- define "loki-vl-proxy.workloadKind" -}}
{{- default "Deployment" .Values.workload.kind -}}
{{- end }}

{{/*
Stateful/headless service name.
*/}}
{{- define "loki-vl-proxy.headlessServiceName" -}}
{{- default (printf "%s-headless" (include "loki-vl-proxy.fullname" .)) .Values.workload.statefulSet.serviceName }}
{{- end }}

{{/*
Render Service trafficDistribution with strict enum validation.
*/}}
{{- define "loki-vl-proxy.renderTrafficDistribution" -}}
{{- $value := default "" .value -}}
{{- $allowed := list "" "PreferSameZone" "PreferSameNode" "PreferClose" -}}
{{- if not (has $value $allowed) -}}
{{- fail (printf "%s must be one of: PreferSameZone, PreferSameNode, PreferClose (deprecated), or empty" .field) -}}
{{- end -}}
{{- if ne $value "" -}}
trafficDistribution: {{ $value | quote }}
{{- end -}}
{{- end }}

{{/*
Persistence claim name for standalone PVC / existing claim mode.
*/}}
{{- define "loki-vl-proxy.persistenceClaimName" -}}
{{- default (printf "%s-cache" (include "loki-vl-proxy.fullname" .)) .Values.persistence.existingClaim }}
{{- end }}

{{/*
Canonicalize volumeClaimTemplates to immutable-shape fields we care about.
*/}}
{{- define "loki-vl-proxy.canonicalizeVolumeClaimTemplates" -}}
{{- $out := list -}}
{{- range $tpl := default (list) .volumeClaimTemplates -}}
{{- $meta := default (dict) $tpl.metadata -}}
{{- $out = append $out (dict "name" (default "" $meta.name) "spec" (default (dict) $tpl.spec)) -}}
{{- end -}}
{{- toYaml $out -}}
{{- end }}

{{/*
Desired canonical volumeClaimTemplates for StatefulSet mode.
Must stay aligned with templates/deployment.yaml generation logic.
*/}}
{{- define "loki-vl-proxy.desiredVolumeClaimTemplates" -}}
{{- $out := list -}}
{{- if and .Values.persistence.enabled (not .Values.persistence.existingClaim) -}}
{{- $spec := dict
    "accessModes" (list .Values.persistence.accessMode)
    "resources" (dict "requests" (dict "storage" .Values.persistence.size))
-}}
{{- if .Values.persistence.storageClass -}}
{{- $_ := set $spec "storageClassName" .Values.persistence.storageClass -}}
{{- end -}}
{{- $out = append $out (dict "name" "cache-data" "spec" $spec) -}}
{{- end -}}
{{- range $tpl := default (list) .Values.persistence.extraVolumeClaimTemplates -}}
{{- $meta := default (dict) $tpl.metadata -}}
{{- $out = append $out (dict "name" (default "" $meta.name) "spec" (default (dict) $tpl.spec)) -}}
{{- end -}}
{{- toYaml $out -}}
{{- end }}

{{/*
Default disk cache path when persistence is enabled.
*/}}
{{- define "loki-vl-proxy.diskCachePath" -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.persistence.mountPath) .Values.persistence.fileName -}}
{{- end }}

{{/*
Resolve ConfigMap name for file-based custom patterns.
*/}}
{{- define "loki-vl-proxy.customPatternsConfigMapName" -}}
{{- if .Values.patternsCustom.file.existingConfigMap -}}
{{- .Values.patternsCustom.file.existingConfigMap -}}
{{- else -}}
{{- default (printf "%s-custom-patterns" (include "loki-vl-proxy.fullname" .)) .Values.patternsCustom.file.configMapName -}}
{{- end -}}
{{- end }}

{{/*
Render default custom patterns file content.
Priority:
1) explicit patternsCustom.file.content
2) JSON from patternsCustom.inline
3) empty JSON array
*/}}
{{- define "loki-vl-proxy.customPatternsFileContent" -}}
{{- if .Values.patternsCustom.file.content -}}
{{- .Values.patternsCustom.file.content -}}
{{- else if gt (len .Values.patternsCustom.inline) 0 -}}
{{- toJson .Values.patternsCustom.inline -}}
{{- else -}}
[]
{{- end -}}
{{- end }}

{{/*
Render a custom manifest entry from values.customManifests.
Supports either raw YAML objects/maps or templated string snippets.
*/}}
{{- define "loki-vl-proxy.renderCustomManifest" -}}
{{- $manifest := .manifest -}}
{{- $context := .context -}}
{{- if kindIs "string" $manifest -}}
{{- tpl $manifest $context -}}
{{- else -}}
{{- tpl (toYaml $manifest) $context -}}
{{- end -}}
{{- end }}

{{/*
Resolve effective GOMEMLIMIT value.
Priority:
1) explicit .Values.goMemLimit
2) computed percentage of .Values.resources.limits.memory
The computed value is emitted as bytes.
*/}}
{{- define "loki-vl-proxy.goMemLimitValue" -}}
{{- if .Values.goMemLimit -}}
{{- .Values.goMemLimit -}}
{{- else -}}
{{- $raw := toString (default "" .Values.resources.limits.memory) -}}
{{- $pct := int64 (default 0 .Values.goMemLimitPercent) -}}
{{- $re := "^([0-9]+)(Ei|Pi|Ti|Gi|Mi|Ki|E|P|T|G|M|K)?$" -}}
{{- if and $raw (gt $pct 0) (le $pct 100) (regexMatch $re $raw) -}}
{{- $num := int64 (regexReplaceAll $re $raw "${1}") -}}
{{- $unit := regexReplaceAll $re $raw "${2}" -}}
{{- $multiplier := int64 1 -}}
{{- if eq $unit "Ki" -}}{{- $multiplier = 1024 -}}{{- end -}}
{{- if eq $unit "Mi" -}}{{- $multiplier = 1048576 -}}{{- end -}}
{{- if eq $unit "Gi" -}}{{- $multiplier = 1073741824 -}}{{- end -}}
{{- if eq $unit "Ti" -}}{{- $multiplier = 1099511627776 -}}{{- end -}}
{{- if eq $unit "Pi" -}}{{- $multiplier = 1125899906842624 -}}{{- end -}}
{{- if eq $unit "Ei" -}}{{- $multiplier = 1152921504606846976 -}}{{- end -}}
{{- if eq $unit "K" -}}{{- $multiplier = 1000 -}}{{- end -}}
{{- if eq $unit "M" -}}{{- $multiplier = 1000000 -}}{{- end -}}
{{- if eq $unit "G" -}}{{- $multiplier = 1000000000 -}}{{- end -}}
{{- if eq $unit "T" -}}{{- $multiplier = 1000000000000 -}}{{- end -}}
{{- if eq $unit "P" -}}{{- $multiplier = 1000000000000000 -}}{{- end -}}
{{- if eq $unit "E" -}}{{- $multiplier = 1000000000000000000 -}}{{- end -}}
{{- $bytes := mul $num $multiplier -}}
{{- div (mul $bytes $pct) 100 -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
loki-vl-proxy.metricsPort — the effective port /metrics is served on, derived
from extraArgs.metrics-listen (the binary's dedicated metrics listener), e.g.
":9091" or "0.0.0.0:9091" → 9091. Falls back to .Values.service.metrics.port.
Deriving the deployment containerPort (and, through the named port, the Service
targetPort and ServiceMonitor target) from one source keeps them in lock-step
with where the proxy actually listens — see loki-vl-proxy.validateMetrics.
*/}}
{{- define "loki-vl-proxy.metricsPort" -}}
{{- $listen := "" -}}
{{- with .Values.extraArgs -}}{{- $listen = (index . "metrics-listen" | toString) -}}{{- end -}}
{{- if and $listen (contains ":" $listen) -}}
{{- splitList ":" $listen | last -}}
{{- else -}}
{{- .Values.service.metrics.port -}}
{{- end -}}
{{- end -}}

{{/*
loki-vl-proxy.validateMetrics — fail-fast on a metrics config that renders
cleanly but is internally inconsistent: the Service/ServiceMonitor would target
a metrics port the binary never serves, or the binary would refuse to start
(cmd/proxy/main.go validateMetricsListen). Invoked from the deployment.
*/}}
{{- define "loki-vl-proxy.validateMetrics" -}}
{{- if .Values.service.metrics.enabled -}}
{{- $listen := "" -}}{{- with .Values.extraArgs -}}{{- $listen = (index . "metrics-listen" | toString) -}}{{- end -}}
{{- $reg := "" -}}{{- with .Values.extraArgs -}}{{- $reg = (index . "server.register-instrumentation" | toString) -}}{{- end -}}
{{- if not (trim $listen) -}}
{{- fail "service.metrics.enabled=true requires extraArgs.metrics-listen (e.g. \":9091\") so /metrics is served on the dedicated metrics port the Service and ServiceMonitor target; set it, or disable service.metrics." -}}
{{- end -}}
{{- if eq $reg "false" -}}
{{- fail "service.metrics.enabled=true requires extraArgs.\"server.register-instrumentation\"=true; otherwise the metrics listener only returns 404 and the proxy fails to start (validateMetricsListen)." -}}
{{- end -}}
{{- /* Reject a loopback-bound metrics listener: it renders cleanly (the port is
       extracted fine) but binds 127.x/localhost/::1, unreachable from the pod IP
       the Service/ServiceMonitor target — scrapes silently time out. */ -}}
{{- $host := regexReplaceAll ":[0-9]+$" $listen "" | trimPrefix "[" | trimSuffix "]" -}}
{{- if or (eq $host "localhost") (eq $host "::1") (hasPrefix "127." $host) -}}
{{- fail (printf "service.metrics.enabled=true with extraArgs.metrics-listen=%q binds /metrics to loopback (%s), unreachable from the pod network — Service/ServiceMonitor scrapes will fail. Use \":9091\", \"0.0.0.0:9091\", or disable service.metrics." $listen $host) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
loki-vl-proxy.cacheKeyGenerated — how the chart provides the cache key secret
when cacheKeySecret.existingSecret is unset: "reuse" when <release>-cache-key
already holds one (lookup), "create" when the render reaches a cluster but
finds none (first install, or the Secret was deleted), "" otherwise. A render
without cluster access (helm template, Argo CD, any renderer where lookup
returns nothing) gets "" and renders no key at all, so rendering never
replaces the key with a new random one. Off with cacheKeySecret.generate=false
or when extraArgs set -cache-key-secret(-file) themselves.
*/}}
{{- define "loki-vl-proxy.cacheKeyGenerated" -}}
{{- $cfg := default (dict) .Values.cacheKeySecret -}}
{{- $extra := default (dict) .Values.extraArgs -}}
{{- if and (not $cfg.existingSecret) (ne (toString $cfg.generate) "false") (not (hasKey $extra "cache-key-secret")) (not (hasKey $extra "cache-key-secret-file")) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (printf "%s-cache-key" .Release.Name) -}}
{{- if and $existing $existing.data (hasKey $existing.data "secret") -}}
reuse
{{- else if not (empty (lookup "v1" "Secret" .Release.Namespace "")) -}}
{{- /* Listing Secrets answers only with cluster access (Helm itself keeps
       its releases as Secrets of this namespace, so it may list them). */ -}}
create
{{- end -}}
{{- end -}}
{{- end }}

{{/*
loki-vl-proxy.secretFiles — the secrets the chart takes from a Secret, as a
JSON list. Each is mounted as one read-only file and passed to the proxy as
-<flag>=<dir>/<path>.
*/}}
{{- define "loki-vl-proxy.secretFiles" -}}
{{- $out := list -}}
{{- if .Values.peerCache.enabled -}}
{{- $name := .Values.peerCache.existingSecret -}}
{{- $key := .Values.peerCache.existingSecretKey | default "token" -}}
{{- if not $name -}}
{{- $name = printf "%s-peer-auth%s" .Release.Name (ternary "-literal" "" (not (empty .Values.peerCache.authToken))) -}}
{{- $key = "token" -}}
{{- end -}}
{{- $out = append $out (dict "flag" "peer-auth-token-file" "volume" "secret-peer-auth" "path" "token" "secretName" $name "key" $key) -}}
{{- end -}}
{{- range $spec := list
    (dict "values" "adminAuth" "flag" "server.admin-auth-token-file" "volume" "secret-admin-auth" "path" "token")
    (dict "values" "deleteAuth" "flag" "delete-api-auth-token-file" "volume" "secret-delete-auth" "path" "token")
    (dict "values" "backendAuth" "flag" "backend-basic-auth-file" "volume" "secret-backend-auth" "path" "basic-auth")
    (dict "values" "backendVersionAuth" "flag" "backend-version-basic-auth-file" "volume" "secret-backend-version-auth" "path" "basic-auth")
    (dict "values" "cacheKeySecret" "flag" "cache-key-secret-file" "volume" "secret-cache-key" "path" "secret") -}}
{{- $auth := default (dict) (get $.Values $spec.values) -}}
{{- if $auth.existingSecret -}}
{{- $out = append $out (dict "flag" $spec.flag "volume" $spec.volume "path" $spec.path "secretName" $auth.existingSecret "key" (default $spec.path $auth.existingSecretKey)) -}}
{{- end -}}
{{- end -}}
{{- if and (not (default (dict) .Values.cacheKeySecret).existingSecret) (include "loki-vl-proxy.cacheKeyGenerated" .) -}}
{{- $out = append $out (dict "flag" "cache-key-secret-file" "volume" "secret-cache-key" "path" "secret" "secretName" (printf "%s-cache-key" .Release.Name) "key" "secret") -}}
{{- end -}}
{{- range $secret := $out -}}
{{- $_ := set $secret "dir" (printf "/var/run/secrets/loki-vl-proxy/%s" (trimPrefix "secret-" $secret.volume)) -}}
{{- end -}}
{{- toJson $out -}}
{{- end }}

{{/*
loki-vl-proxy.validateSecrets — fail-fast when a secret is configured twice:
once through extraArgs (value or -file flag) or observability.otlp.headers, and
once from an existing Secret. The proxy refuses a value flag next to a file
flag, and for OTLP headers the plain flag would silently win over the
Secret-backed env var. Invoked from the deployment.
*/}}
{{- define "loki-vl-proxy.validateSecrets" -}}
{{- $extraArgs := default (dict) .Values.extraArgs -}}
{{- range $spec := list
    (dict "values" "adminAuth" "flag" "server.admin-auth-token" "quoted" true)
    (dict "values" "backendAuth" "flag" "backend-basic-auth")
    (dict "values" "backendVersionAuth" "flag" "backend-version-basic-auth")
    (dict "values" "cacheKeySecret" "flag" "cache-key-secret")
    (dict "values" "deleteAuth" "flag" "delete-api-auth-token") -}}
{{- $auth := default (dict) (get $.Values $spec.values) -}}
{{- range $flag := list $spec.flag (printf "%s-file" $spec.flag) -}}
{{- if and $auth.existingSecret (hasKey $extraArgs $flag) -}}
{{- $key := ternary (printf "extraArgs.%q" $flag) (printf "extraArgs.%s" $flag) (hasKey $spec "quoted") -}}
{{- fail (printf "%s.existingSecret cannot be combined with %s; the chart renders -%s-file from the Secret. Remove the extraArgs entry." $spec.values $key $spec.flag) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $deleteAuth := default (dict) .Values.deleteAuth -}}
{{- /* The delete API answers only requests that carry its own token; the
proxy refuses to start with it enabled and no token (validateDeleteAPI). */ -}}
{{- if and (eq (toString (get $extraArgs "delete-api-enabled")) "true") (not $deleteAuth.existingSecret) (eq (trim (toString (default "" (get $extraArgs "delete-api-auth-token")))) "") (eq (trim (toString (default "" (get $extraArgs "delete-api-auth-token-file")))) "") -}}
{{- fail "extraArgs.delete-api-enabled=true requires a delete token: set deleteAuth.existingSecret (or extraArgs.delete-api-auth-token / extraArgs.delete-api-auth-token-file). Every delete request must carry it in X-Delete-Token or Authorization: Bearer." -}}
{{- end -}}
{{- /* Per-tenant limits are keyed by X-Scope-OrgID, which the proxy ignores while
tenancy is off; it refuses to start then (validateTenantLimitsNeedTenancy). */ -}}
{{- if ne (trim (toString (default "" (get $extraArgs "tenant-limits")))) "" -}}
{{- $tenancy := or (ne (trim (toString (default "" (get $extraArgs "tenant-map")))) "") (ne (trim (toString (default "" (get $extraArgs "tenant-map-file")))) "") (ne (trim (toString (default "" (get $extraArgs "tenant-label")))) "") (eq (toString (get $extraArgs "auth.enabled")) "true") (eq (toString (get $extraArgs "require-tenant-header")) "true") (eq (toString (get $extraArgs "tenant.numeric-passthrough")) "true") -}}
{{- if not $tenancy -}}
{{- fail "extraArgs.tenant-limits is keyed by X-Scope-OrgID, but tenancy is off: set extraArgs.tenant.numeric-passthrough \"true\", tenant-map, tenant-map-file, tenant-label, auth.enabled or require-tenant-header, or use tenant-default-limits." -}}
{{- end -}}
{{- end -}}
{{- $observability := default (dict) .Values.observability -}}
{{- $otlp := default (dict) (get $observability "otlp") -}}
{{- if and $otlp.existingSecret (or (hasKey $extraArgs "otlp-headers") (ne (default "" $otlp.headers | toString) "")) -}}
{{- fail "observability.otlp.existingSecret cannot be combined with observability.otlp.headers or extraArgs.otlp-headers; the OTLP headers are read from the Secret. Remove the plain value." -}}
{{- end -}}
{{- end -}}

{{/*
terminationGracePeriodSeconds: the configured value, raised with the restart
hand-over on to cover the preStop sleep (the chart's, or a configured
lifecycle.preStop.sleep), the proxy's HTTP drain of the main listener (30 s)
and of the admin and metrics listeners (30 s), the shutdown flush and 5 s for
the disk cache to close. A configured exec or httpGet preStop is not counted.
*/}}
{{- define "loki-vl-proxy.preStopSleepSeconds" -}}
{{- if .Values.lifecycle -}}
{{- $sleep := dig "preStop" "sleep" "seconds" 0 .Values.lifecycle -}}
{{- int $sleep -}}
{{- else if semverCompare ">=1.30-0" .Capabilities.KubeVersion.Version -}}
{{- int .Values.cacheHandover.preStopSleepSeconds -}}
{{- else -}}
0
{{- end -}}
{{- end -}}

{{- define "loki-vl-proxy.terminationGracePeriodSeconds" -}}
{{- $grace := int .Values.terminationGracePeriodSeconds -}}
{{- if .Values.cacheHandover.enabled -}}
{{- $preStop := int (include "loki-vl-proxy.preStopSleepSeconds" .) -}}
{{- $need := add $preStop 60 (int .Values.cacheHandover.shutdownFlushTimeoutSeconds) 5 -}}
{{- if gt (int $need) $grace -}}{{- $grace = int $need -}}{{- end -}}
{{- end -}}
{{- $grace -}}
{{- end -}}

{{/*
"true" when the proxy runs with a disk cache the chart can see: a non-empty
extraArgs.disk-cache-path, or persistence.enabled without that key (the chart
then passes the flag). Empty otherwise.
*/}}
{{- define "loki-vl-proxy.diskCacheEnabled" -}}
{{- if hasKey .Values.extraArgs "disk-cache-path" -}}
{{- if ne (toString (default "" (get .Values.extraArgs "disk-cache-path"))) "" -}}true{{- end -}}
{{- else if .Values.persistence.enabled -}}
true
{{- end -}}
{{- end -}}

{{/*
"true" when a startup probe is rendered: probe.startup.enabled as set, or,
unset, with a disk cache or the restart hand-over (the disk cache index is
rebuilt before the listener opens). Empty otherwise.
*/}}
{{- define "loki-vl-proxy.startupProbeEnabled" -}}
{{- $enabled := .Values.probe.startup.enabled -}}
{{- if kindIs "invalid" $enabled -}}
{{- $enabled = or (eq (include "loki-vl-proxy.diskCacheEnabled" .) "true") .Values.cacheHandover.enabled -}}
{{- end -}}
{{- if $enabled -}}true{{- end -}}
{{- end -}}

{{/*
The startup probe's failureThreshold: the configured value, else 120 with a disk
cache (605 s with the default delay and period), else 60 with the restart
hand-over, else 30.
*/}}
{{- define "loki-vl-proxy.startupFailureThreshold" -}}
{{- $default := ternary 60 30 .Values.cacheHandover.enabled -}}
{{- if eq (include "loki-vl-proxy.diskCacheEnabled" .) "true" -}}{{- $default = 120 -}}{{- end -}}
{{- .Values.probe.startup.failureThreshold | default $default -}}
{{- end -}}

{{/*
A Deployment's progressDeadlineSeconds: workload.progressDeadlineSeconds as
set. Unset, with a disk cache and a startup probe, the longest one replica may
take to be replaced (grace period of the old pod, the new pod's startup window
and, with the restart hand-over, its warm-up timeout and minReadySeconds) plus
300 s for scheduling, volume attach, image pull and the readiness probe;
empty when that is within the Kubernetes default of 600 s, and without a disk
cache.
*/}}
{{- define "loki-vl-proxy.progressDeadlineSeconds" -}}
{{- if not (kindIs "invalid" .Values.workload.progressDeadlineSeconds) -}}
{{- int .Values.workload.progressDeadlineSeconds -}}
{{- else if and (eq (include "loki-vl-proxy.diskCacheEnabled" .) "true") (eq (include "loki-vl-proxy.startupProbeEnabled" .) "true") -}}
{{- $window := add (int .Values.probe.startup.initialDelaySeconds) (mul (int .Values.probe.startup.periodSeconds) (int (include "loki-vl-proxy.startupFailureThreshold" .))) -}}
{{- $total := add (int (include "loki-vl-proxy.terminationGracePeriodSeconds" .)) $window 300 -}}
{{- if .Values.cacheHandover.enabled -}}
{{- $total = add $total (int .Values.cacheHandover.startupWarmTimeoutSeconds) (int .Values.cacheHandover.minReadySeconds) -}}
{{- end -}}
{{- if gt (int $total) 600 -}}{{- $total -}}{{- end -}}
{{- end -}}
{{- end -}}

