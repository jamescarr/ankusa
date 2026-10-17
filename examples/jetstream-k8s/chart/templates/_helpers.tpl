{{/* "repository:tag" for one entry of .Values.images. */}}
{{- define "demo.image" -}}
{{ .repository }}:{{ .tag }}
{{- end }}

{{/* Pod/selector labels for a component. */}}
{{- define "demo.labels" -}}
app.kubernetes.io/name: {{ . }}
app.kubernetes.io/part-of: ankusa-jetstream-demo
{{- end }}

{{/*
`claim_check.store`: the S3 bucket (floci) that holds claimed bodies. The edge
pods write to it, the claim-check gateway reads from it, so both ConfigMaps
render this one block.
*/}}
{{- define "demo.claimStore" -}}
type: s3
s3:
  bucket: {{ .Values.s3.bucket | quote }}
  region: {{ .Values.s3.region | quote }}
  endpoint: "http://floci:4566"
  access_key_id: {{ .Values.s3.accessKeyId | quote }}
  secret_access_key: {{ .Values.s3.secretAccessKey | quote }}
{{- end }}
