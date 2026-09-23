import Config

# Runtime config from environment variables (12-factor)

roles =
  System.get_env("ANKUSA_ROLES", "edge,dispatch,storage")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_atom/1)

port = String.to_integer(System.get_env("PORT", "4000"))
claim_check_port = String.to_integer(System.get_env("CLAIM_CHECK_PORT", "4001"))

# Kafka sink configuration
kafka_brokers =
  System.get_env("KAFKA_BROKERS", "localhost:9092")
  |> String.split(",", trim: true)

kafka_topic = System.get_env("KAFKA_TOPIC", "ankusa.events")
inline_max_bytes = String.to_integer(System.get_env("INLINE_MAX_BYTES", "8192"))

# S3 blob store configuration
s3_bucket = System.fetch_env!("S3_BUCKET")
s3_region = System.get_env("S3_REGION", "us-east-1")
s3_endpoint = System.get_env("S3_ENDPOINT")
s3_access_key_id = System.fetch_env!("S3_ACCESS_KEY_ID")
s3_secret_access_key = System.fetch_env!("S3_SECRET_ACCESS_KEY")

s3_opts = [
  bucket: s3_bucket,
  region: s3_region,
  access_key_id: s3_access_key_id,
  secret_access_key: s3_secret_access_key
]

s3_opts = if s3_endpoint, do: Keyword.put(s3_opts, :endpoint, s3_endpoint), else: s3_opts

claim_check_token = System.get_env("CLAIM_CHECK_TOKEN", "dev-claim-check-token")

config :ankusa, :default,
  roles: roles,
  port: port,
  max_body_bytes: 8_000_000,
  route_resolver: {Ankusa.RouteResolver.Path, []},
  source_store:
    {Ankusa.SourceStore.Static,
     sources: %{
       "demo" => %{
         id: "demo",
         tenant_id: "example",
         verifier: {Ankusa.Verifier.None, []}
       }
     }},
  storage: %{
    blob_store: {Ankusa.BlobStore.S3, s3_opts}
  },
  sinks: [
    {Ankusa.Sink.Kafka,
     brokers: kafka_brokers, topic: kafka_topic, inline_max_bytes: inline_max_bytes}
  ],
  claim_check: %{
    port: claim_check_port,
    api_tokens: %{
      "dev-client" => claim_check_token
    },
    adapter: {Ankusa.ClaimCheck.Direct, blob_store: {Ankusa.BlobStore.S3, s3_opts}}
  }
