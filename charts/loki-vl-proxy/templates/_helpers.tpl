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
Persistence claim name for standalone PVC / existing claim mode.
*/}}
{{- define "loki-vl-proxy.persistenceClaimName" -}}
{{- default (printf "%s-cache" (include "loki-vl-proxy.fullname" .)) .Values.persistence.existingClaim }}
{{- end }}

{{/*
Default disk cache path when persistence is enabled.
*/}}
{{- define "loki-vl-proxy.diskCachePath" -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.persistence.mountPath) .Values.persistence.fileName -}}
{{- end }}

{{/*
Convert a Kubernetes memory quantity to bytes.
Supports plain integers plus decimal/binary suffixes commonly used in charts.
*/}}
{{- define "loki-vl-proxy.memoryQuantityToBytes" -}}
{{- $raw := toString . | trim -}}
{{- if not $raw -}}
{{- "" -}}
{{- else -}}
{{- $num := mustRegexFind "^[0-9]+" $raw | int64 -}}
{{- $unit := "" -}}
{{- if regexMatch "[A-Za-z]+$" $raw -}}
{{- $unit = mustRegexFind "[A-Za-z]+$" $raw -}}
{{- end -}}
{{- if eq $unit "" -}}
{{- $num -}}
{{- else if eq $unit "Ki" -}}
{{- mul $num 1024 -}}
{{- else if eq $unit "Mi" -}}
{{- mul $num 1048576 -}}
{{- else if eq $unit "Gi" -}}
{{- mul $num 1073741824 -}}
{{- else if eq $unit "Ti" -}}
{{- mul $num 1099511627776 -}}
{{- else if eq $unit "K" -}}
{{- mul $num 1000 -}}
{{- else if eq $unit "M" -}}
{{- mul $num 1000000 -}}
{{- else if eq $unit "G" -}}
{{- mul $num 1000000000 -}}
{{- else if eq $unit "T" -}}
{{- mul $num 1000000000000 -}}
{{- else -}}
{{- fail (printf "unsupported memory unit for goMemLimitPercent: %q" $raw) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Resolve the runtime GOMEMLIMIT value.
Explicit goMemLimit wins; otherwise derive bytes from resources.limits.memory.
*/}}
{{- define "loki-vl-proxy.goMemLimit" -}}
{{- if .Values.goMemLimit -}}
{{- .Values.goMemLimit -}}
{{- else -}}
{{- $memoryLimit := dig "limits" "memory" "" .Values.resources -}}
{{- if $memoryLimit -}}
{{- $limitBytes := include "loki-vl-proxy.memoryQuantityToBytes" $memoryLimit | int64 -}}
{{- div (mul $limitBytes (.Values.goMemLimitPercent | int64)) 100 -}}
{{- end -}}
{{- end -}}
{{- end }}
