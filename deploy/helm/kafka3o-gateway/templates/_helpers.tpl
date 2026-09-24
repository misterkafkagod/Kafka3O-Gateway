{{- define "kafka3o-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kafka3o-gateway.fullname" -}}
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

{{- define "kafka3o-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka3o-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "kafka3o-gateway.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "kafka3o-gateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "kafka3o-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafka3o-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "kafka3o-gateway.secretName" -}}
{{- default (include "kafka3o-gateway.fullname" .) .Values.auth.existingSecret }}
{{- end }}

{{- define "kafka3o-gateway.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/*
config.yaml for the gateway, in its own key names (configs/config.example.yaml).
auth.keys, the probe key, and SASL credentials are not here: they come from
the Secret as environment variables.
*/}}
{{- define "kafka3o-gateway.config" -}}
{{- $maxTime := int .Values.bounds.maxTimeMs }}
{{- $tls := dict "enabled" .Values.kafka.tls.enabled }}
{{- if .Values.kafka.tls.enabled }}
{{- $_ := set $tls "ca_file" "/etc/kafka3o/tls/ca.crt" }}
{{- if .Values.kafka.tls.mutual }}
{{- $_ := set $tls "cert_file" "/etc/kafka3o/tls/tls.crt" }}
{{- $_ := set $tls "key_file" "/etc/kafka3o/tls/tls.key" }}
{{- end }}
{{- end }}
{{- $cfg := dict
  "kafka" (dict
    "bootstrap" .Values.kafka.bootstrap
    "request_timeout" .Values.kafka.requestTimeout
    "tls" $tls
    "sasl" (dict "mechanism" .Values.kafka.sasl.mechanism))
  "http" (dict
    "addr" (printf ":%d" (int .Values.service.port))
    "trusted_proxies" .Values.trustedProxies
    "cors" (dict "origins" .Values.cors.origins))
  "auth" (dict "enabled" .Values.auth.enabled)
  "policy" (dict
    "read_only_mode" .Values.policy.readOnlyMode
    "data_plane_lock" .Values.policy.dataPlaneLock
    "disabled_operations" .Values.policy.disabledOperations)
  "bounds" (dict "scan" (dict "max_time" (dict
    "default" (printf "%dms" (min 10000 $maxTime))
    "ceiling" (printf "%dms" $maxTime))))
  "audit" (dict "sink" .Values.audit.sink "topic" .Values.audit.topic)
  "telemetry" (dict
    "otlp" (dict "endpoint" .Values.otlp.endpoint "protocol" .Values.otlp.protocol)
    "log_level" .Values.logLevel)
}}
{{- toYaml (mergeOverwrite $cfg (deepCopy .Values.extraConfig)) }}
{{- end }}
