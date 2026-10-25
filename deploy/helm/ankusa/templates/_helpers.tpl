{{/* Chart name, release-qualified name. */}}
{{- define "ankusa.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "ankusa.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "ankusa.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ankusa.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/* Labels every object carries. */}}
{{- define "ankusa.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "ankusa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Labels that select every pod of the release (the NetworkPolicy's target). */}}
{{- define "ankusa.releaseSelector" -}}
app.kubernetes.io/name: {{ include "ankusa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Labels that select one workload's pods. Call with
(dict "ctx" . "component" "node") or "claim-check".
*/}}
{{- define "ankusa.selector" -}}
{{ include "ankusa.releaseSelector" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/* wal.type, validated: disk, dual or none. */}}
{{- define "ankusa.wal" -}}
{{- $t := toString .Values.wal.type -}}
{{- if not (has $t (list "disk" "dual" "none")) -}}
{{- fail (printf "wal.type must be disk, dual or none, got %q" $t) -}}
{{- end -}}
{{- $t -}}
{{- end }}

{{/* The image reference: a digest wins over a tag, the tag defaults to appVersion. */}}
{{- define "ankusa.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end }}

{{/*
One listener's exposure. Call with (dict "port" 4002 "scope" .Values.admin "what" "admin").
A token Secret authenticates it (the server hashes ANKUSA_*_TOKEN at load, and the
env replaces the whole `auth` block); without one the chart refuses to bind a
pod address unless allowInsecure says something else authenticates the callers.
*/}}
{{- define "ankusa.listener" -}}
{{- $l := dict "port" .port "ip" "0.0.0.0" -}}
{{- if not .scope.tokenSecret.name -}}
{{- if not .scope.allowInsecure -}}
{{- fail (printf "%s is exposed on the pod address: set %s.tokenSecret.name, or %s.allowInsecure: true when a mesh or the NetworkPolicy authenticates its callers" .what .what .what) -}}
{{- end -}}
{{- $_ := set $l "auth" (dict "mode" "none" "allow_insecure" true) -}}
{{- end -}}
{{- toYaml $l -}}
{{- end }}

{{/*
The server's ankusa.yml: what the chart derives, with .Values.config merged over
it (the user's keys win, maps merge).
*/}}
{{- define "ankusa.config" -}}
{{- $base := dict
  "node" (dict "data_dir" "/var/lib/ankusa")
  "log" (dict "format" "json" "banner" false)
  "wal" (dict "type" (include "ankusa.wal" .))
  "listener" (dict "drain_delay_ms" (int .Values.drain.delayMs) "drain_timeout_ms" (int .Values.drain.timeoutMs))
  "admin" (dict "enabled" true "port" 4002 "ip" "127.0.0.1") -}}
{{- if .Values.admin.expose -}}
{{- $_ := set $base "admin" (merge (dict "enabled" true) (fromYaml (include "ankusa.listener" (dict "port" 4002 "scope" .Values.admin "what" "admin")))) -}}
{{- end -}}
{{- if .Values.claimCheck.enabled -}}
{{- $_ := set $base "claim_check" (fromYaml (include "ankusa.listener" (dict "port" 4001 "scope" .Values.claimCheck "what" "claimCheck"))) -}}
{{- end -}}
{{- toYaml (mustMergeOverwrite $base (deepCopy .Values.config)) -}}
{{- end }}

{{/*
terminationGracePeriodSeconds of a node pod, from the drain the server runs on
SIGTERM (docs/deployment.md): (drain_delay_ms + drain_timeout_ms) / 1000 + 10,
plus backup.final_timeout_ms when the node backs up. Read from the merged config,
so a drain set under `config.listener` is the one the grace period follows.
*/}}
{{- define "ankusa.grace" -}}
{{- if .Values.terminationGracePeriodSeconds -}}
{{- .Values.terminationGracePeriodSeconds -}}
{{- else -}}
{{- $cfg := fromYaml (include "ankusa.config" .) -}}
{{- $delay := int (dig "listener" "drain_delay_ms" 5000 $cfg) -}}
{{- $timeout := int (dig "listener" "drain_timeout_ms" 30000 $cfg) -}}
{{- $grace := add (div (add $delay $timeout) 1000) 10 -}}
{{- if eq (toString (dig "backup" "enabled" false $cfg)) "true" -}}
{{- $grace = add $grace (div (int (dig "backup" "final_timeout_ms" 20000 $cfg)) 1000) -}}
{{- end -}}
{{- $grace -}}
{{- end -}}
{{- end }}

{{/* topologySpreadConstraints for one workload (see values.yaml). Call with (dict "ctx" . "component" "node"). */}}
{{- define "ankusa.topologySpread" -}}
{{- $sel := dict "matchLabels" (fromYaml (include "ankusa.selector" .)) -}}
{{- $out := list -}}
{{- range $name, $c := .ctx.Values.topologySpread -}}
{{- if $c -}}
{{- $out = append $out (merge (dict "labelSelector" $sel) $c) -}}
{{- end -}}
{{- end -}}
{{- range .ctx.Values.extraTopologySpreadConstraints -}}
{{- $out = append $out (merge (dict "labelSelector" $sel) .) -}}
{{- end -}}
{{- if $out -}}
{{- toYaml $out -}}
{{- end -}}
{{- end }}

{{/* Pod scheduling knobs shared by every workload. */}}
{{- define "ankusa.scheduling" -}}
{{- with .Values.priorityClassName }}
priorityClassName: {{ . | quote }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/* The token env a listener's Secret becomes. Call with (dict "var" "ANKUSA_ADMIN_TOKEN" "ref" .Values.admin.tokenSecret). */}}
{{- define "ankusa.tokenEnv" -}}
{{- if .ref.name }}
- name: {{ .var }}
  valueFrom:
    secretKeyRef:
      name: {{ .ref.name | quote }}
      key: {{ default "token" .ref.key | quote }}
{{- end }}
{{- end }}
