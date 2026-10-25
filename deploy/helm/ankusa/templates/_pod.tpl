{{/*
The pod template of a node (wal.type disk | dual | none), shared by the StatefulSet
and the Deployment. Call with the root context.
*/}}
{{- define "ankusa.nodePodTemplate" -}}
{{- $wal := include "ankusa.wal" . -}}
{{- $stateful := ne $wal "none" -}}
metadata:
  labels:
    {{- include "ankusa.labels" . | nindent 4 }}
    app.kubernetes.io/component: node
    {{- with .Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  annotations:
    # A changed ConfigMap rolls the pods: the file is mounted with a subPath, which
    # the kubelet never refreshes in place.
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  serviceAccountName: {{ include "ankusa.serviceAccountName" . }}
  # The pods need no Kubernetes API, and a Service named `ankusa` would otherwise
  # inject ANKUSA_PORT=tcp://... next to the ANKUSA_* settings the image reads.
  automountServiceAccountToken: false
  enableServiceLinks: false
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  # The drain's own time plus a margin (docs/deployment.md, "Draining on SIGTERM").
  # There is no preStop sleep: the node flips /ready itself and keeps serving for
  # listener.drain_delay_ms before it closes the listener.
  terminationGracePeriodSeconds: {{ include "ankusa.grace" . }}
  {{- with include "ankusa.scheduling" . }}
  {{- . | nindent 2 }}
  {{- end }}
  {{- with include "ankusa.topologySpread" (dict "ctx" . "component" "node") }}
  topologySpreadConstraints:
    {{- . | nindent 4 }}
  {{- end }}
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  containers:
    - name: ankusa
      image: {{ include "ankusa.image" . | quote }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      {{- if gt (int .Values.nofile) 0 }}
      # The image's own entrypoint, behind a cap on open files: see `nofile` in values.yaml.
      command: ["/bin/sh", "-c", "ulimit -n {{ int .Values.nofile }} && exec /sbin/tini -- /usr/local/bin/docker-entrypoint start"]
      {{- end }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      ports:
        - {name: http, containerPort: 4000}
        {{- if .Values.admin.expose }}
        - {name: admin, containerPort: 4002}
        {{- end }}
      env:
        - name: POD_NAME
          valueFrom:
            fieldRef: {fieldPath: metadata.name}
        # The release's tmp, under a read-only root file system.
        - {name: RELEASE_TMP, value: /tmp}
        {{- if eq $wal "dual" }}
        # The node's identity in the intake log: stable across a replacement.
        - {name: ANKUSA_INTAKE_NODE_ID, value: "$(POD_NAME)"}
        {{- end }}
        {{- if and $stateful .Values.perPodKeyPrefix }}
        - {name: ANKUSA_STORAGE_KEY_PREFIX, value: "$(POD_NAME)/"}
        {{- end }}
        {{- with include "ankusa.tokenEnv" (dict "var" "ANKUSA_ADMIN_TOKEN" "ref" .Values.admin.tokenSecret) }}
        {{- . | trim | nindent 8 }}
        {{- end }}
        {{- with .Values.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.extraEnvFrom }}
      envFrom:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      # /health: the process answers. /ready: the node can take a hook (503 while the
      # store refuses writes, the intake log is down, or a stop is draining it).
      startupProbe:
        httpGet: {path: /health, port: http}
        {{- toYaml .Values.probes.startup | nindent 8 }}
      livenessProbe:
        httpGet: {path: /health, port: http}
        {{- toYaml .Values.probes.liveness | nindent 8 }}
      readinessProbe:
        httpGet: {path: /ready, port: http}
        {{- toYaml .Values.probes.readiness | nindent 8 }}
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
      volumeMounts:
        - {name: config, mountPath: /etc/ankusa/ankusa.yml, subPath: ankusa.yml, readOnly: true}
        - {name: data, mountPath: /var/lib/ankusa}
        - {name: tmp, mountPath: /tmp}
        {{- with .Values.extraVolumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
  volumes:
    - name: config
      configMap: {name: {{ include "ankusa.fullname" . }}}
    - name: tmp
      emptyDir: {sizeLimit: 256Mi}
    {{- if not $stateful }}
    # Nothing here is worth keeping: the quarantine pen, API-managed sources and
    # rate-limit overrides of a stateless node are lost with the pod.
    - name: data
      emptyDir: {sizeLimit: {{ .Values.persistence.size | quote }}}
    {{- end }}
    {{- with .Values.extraVolumes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end }}
