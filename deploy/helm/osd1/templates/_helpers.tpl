{{- define "osd1.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "osd1.fullname" -}}
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

{{- define "osd1.headless" -}}
{{- printf "%s-raft" (include "osd1.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "osd1.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "osd1.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "osd1.selectorLabels" -}}
app.kubernetes.io/name: {{ include "osd1.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "osd1.serviceAccountName" -}}
{{- printf "%s" (include "osd1.fullname" .) }}
{{- end }}

{{- define "osd1.tokenSecret" -}}
{{- .Values.auth.existingSecret | default (include "osd1.fullname" .) }}
{{- end }}

{{- define "osd1.raftSecret" -}}
{{- if .Values.raft.existingSecret }}
{{- .Values.raft.existingSecret }}
{{- else }}
{{- include "osd1.tokenSecret" . }}
{{- end }}
{{- end }}

{{- define "osd1.litestreamSecret" -}}
{{- .Values.litestream.existingSecret | default (printf "%s-litestream" (include "osd1.fullname" .)) }}
{{- end }}

{{- define "osd1.validate" -}}
{{- if and (not .Values.raft.enabled) (ne (int .Values.replicaCount) 1) }}
{{- fail "simple mode runs one process for one database file. Set replicaCount to 1, or set raft.enabled to true." }}
{{- end }}
{{- if not (or .Values.auth.existingSecret .Values.auth.token) }}
{{- fail "Set auth.token or auth.existingSecret." }}
{{- end }}
{{- if .Values.raft.enabled }}
{{- $voters := int .Values.raft.voters }}
{{- if or (lt $voters 3) (eq (mod $voters 2) 0) }}
{{- fail "raft.voters must be an odd number of 3 or more." }}
{{- end }}
{{- if lt (int .Values.replicaCount) $voters }}
{{- fail "replicaCount must be at least raft.voters. Pods past that count join as learners." }}
{{- end }}
{{- if and (not .Values.raft.existingSecret) (not .Values.auth.existingSecret) (not .Values.raft.secret) }}
{{- fail "Set raft.secret when raft is enabled." }}
{{- end }}
{{- end }}
{{- if .Values.litestream.enabled }}
{{- if not .Values.litestream.bucket }}
{{- fail "Set litestream.bucket when litestream is enabled." }}
{{- end }}
{{- if not (or .Values.litestream.existingSecret (and .Values.litestream.accessKeyId .Values.litestream.secretAccessKey)) }}
{{- fail "Set litestream.existingSecret, or both access keys, when litestream is enabled." }}
{{- end }}
{{- end }}
{{- end }}
