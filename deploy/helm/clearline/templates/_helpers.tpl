{{/* Base name: the release name if it already says clearline, else <release>-clearline. */}}
{{- define "clearline.fullname" -}}
{{- if contains "clearline" .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-clearline" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "clearline.labels" -}}
app.kubernetes.io/part-of: clearline
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "clearline.selector" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/name: {{ .name }}
{{- end -}}

{{- define "clearline.secretName" -}}
{{- if .Values.existingSecret -}}{{ .Values.existingSecret }}{{- else -}}{{ include "clearline.fullname" . }}-db{{- end -}}
{{- end -}}

{{/* Locked-down container defaults: non-root, read-only root fs, no capabilities. */}}
{{- define "clearline.containerSecurity" -}}
securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
  seccompProfile:
    type: RuntimeDefault
{{- end -}}

{{- define "clearline.servicesImage" -}}
{{ .Values.images.services.repository }}:{{ .Values.images.services.tag }}
{{- end -}}

{{/* Environment every Go binary reads (internal/env). */}}
{{- define "clearline.env" -}}
envFrom:
  - configMapRef:
      name: {{ include "clearline.fullname" . }}-config
env:
  - name: CLEARLINE_DATABASE_URL
    valueFrom:
      secretKeyRef:
        name: {{ include "clearline.secretName" . }}
        key: DATABASE_URL
{{- end -}}

{{/*
Init containers for app pods: apply migrations and create topics before the
service starts. Both are idempotent; migrate holds a Postgres advisory lock,
so pods starting together serialize. They fail (and are retried with
backoff) until Postgres and Kafka are reachable.
*/}}
{{- define "clearline.initContainers" -}}
initContainers:
  - name: migrate
    image: {{ include "clearline.servicesImage" . }}
    imagePullPolicy: {{ .Values.images.pullPolicy }}
    command: ["migrate"]
    {{- include "clearline.env" . | nindent 4 }}
    {{- include "clearline.containerSecurity" . | nindent 4 }}
    resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 128Mi}}
  - name: topics
    image: {{ include "clearline.servicesImage" . }}
    imagePullPolicy: {{ .Values.images.pullPolicy }}
    command: ["topics"]
    {{- include "clearline.env" . | nindent 4 }}
    {{- include "clearline.containerSecurity" . | nindent 4 }}
    resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 128Mi}}
{{- end -}}

{{/*
One Go service: Deployment + Service. Called with a dict:
  root      the chart context ($)
  name      component name (gateway, settlement, ...)
  port      HTTP port (API or /metrics)
  probe     HTTP path for readiness/liveness
  cfg       the component's values (.Values.<component>)
  replicas  override (optional)
  recreate  true to stop the old pod before starting the new one
*/}}
{{- define "clearline.goService" -}}
{{- $root := .root -}}
{{- $full := include "clearline.fullname" $root -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $full }}-{{ .name }}
  labels:
    {{- include "clearline.labels" $root | nindent 4 }}
    app.kubernetes.io/name: {{ .name }}
spec:
  replicas: {{ default .cfg.replicas .replicas | default 1 }}
  {{- if .recreate }}
  strategy:
    type: Recreate
  {{- end }}
  selector:
    matchLabels:
      {{- include "clearline.selector" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "clearline.labels" $root | nindent 8 }}
        app.kubernetes.io/name: {{ .name }}
      annotations:
        checksum/config: {{ include (print $root.Template.BasePath "/config.yaml") $root | sha256sum }}
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
      {{- include "clearline.initContainers" $root | nindent 6 }}
      containers:
        - name: {{ .name }}
          image: {{ include "clearline.servicesImage" $root }}
          imagePullPolicy: {{ $root.Values.images.pullPolicy }}
          command: [{{ .name | quote }}]
          ports:
            - name: http
              containerPort: {{ .port }}
          {{- include "clearline.env" $root | nindent 10 }}
          readinessProbe:
            httpGet: {path: {{ .probe }}, port: http}
            periodSeconds: 5
          livenessProbe:
            httpGet: {path: {{ .probe }}, port: http}
            initialDelaySeconds: 20
            periodSeconds: 10
            failureThreshold: 6
          {{- include "clearline.containerSecurity" $root | nindent 10 }}
          resources:
            {{- toYaml .cfg.resources | nindent 12 }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $full }}-{{ .name }}
  labels:
    {{- include "clearline.labels" $root | nindent 4 }}
    app.kubernetes.io/name: {{ .name }}
spec:
  selector:
    {{- include "clearline.selector" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .port }}
      targetPort: http
{{- end -}}
