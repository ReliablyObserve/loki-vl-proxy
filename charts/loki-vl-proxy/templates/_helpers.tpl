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
