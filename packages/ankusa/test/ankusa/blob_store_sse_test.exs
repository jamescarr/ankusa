defmodule Ankusa.BlobStore.SSETest do
  @moduledoc """
  Server-side encryption on the object-store adapters: what each one puts on
  the wire for `:sse` (captured with `Req.Test`), that it is only ever on a
  `PUT`, and that a bad option is a boot error rather than an unencrypted
  object. That backups, claims and segments all reach the adapter through its
  `put/4` is covered against the S3 emulator in
  `test/ankusa/store/backup_s3_integration_test.exs` (`sse header on put`).
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore
  alias Ankusa.BlobStore.{Azure, GCS, OCI, S3, SSE}
  alias Ankusa.Test.OCIKey

  @kms_key "arn:aws:kms:us-east-1:111122223333:key/0000-aaaa"

  @s3 [
    bucket: "b",
    region: "us-east-1",
    endpoint: "http://localhost:4566",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  @gcs [bucket: "b", endpoint: "http://gcs.test"]
  @azure [account_name: "acct", container: "cont", endpoint: "http://localhost:10000/acct"]

  @oci [
    region: "us-ashburn-1",
    namespace: "ns",
    bucket: "b",
    tenancy_ocid: "ocid1.tenancy.oc1..t",
    user_ocid: "ocid1.user.oc1..u",
    key_fingerprint: "aa:bb:cc",
    endpoint: "http://objectstorage.test"
  ]

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, _body, conn} = Plug.Conn.read_body(conn)

      Agent.update(capture, fn seen ->
        [{conn.method, conn.request_path, conn.query_string, Map.new(conn.req_headers)} | seen]
      end)

      Plug.Conn.send_resp(conn, 200, "ok")
    end)

    plug = [req_options: [plug: {Req.Test, __MODULE__}]]

    %{
      capture: capture,
      s3: @s3 ++ plug,
      gcs: @gcs ++ plug,
      azure: @azure ++ plug,
      oci: Keyword.put(@oci, :private_key, OCIKey.private_key_pem()) ++ plug
    }
  end

  defp seen(capture), do: capture |> Agent.get(& &1) |> Enum.reverse()

  defp signed_headers(%{"authorization" => authorization}) do
    [_, names] = Regex.run(~r/SignedHeaders=([^,]+),/, authorization)
    String.split(names, ";")
  end

  describe "S3" do
    test "aes256: x-amz-server-side-encryption: AES256, signed with the request", ctx do
      assert :ok = S3.put(:i, "seg/a", "data", [sse: %{type: :aes256}] ++ ctx.s3)

      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      assert headers["x-amz-server-side-encryption"] == "AES256"
      refute Map.has_key?(headers, "x-amz-server-side-encryption-aws-kms-key-id")
      # an S3 request whose x-amz-* header is not signed is refused by S3
      assert "x-amz-server-side-encryption" in signed_headers(headers)
    end

    test "kms with a key: both headers, both signed", ctx do
      assert :ok = S3.put(:i, "seg/a", "data", [sse: %{type: :kms, key_id: @kms_key}] ++ ctx.s3)

      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      assert headers["x-amz-server-side-encryption"] == "aws:kms"
      assert headers["x-amz-server-side-encryption-aws-kms-key-id"] == @kms_key
      assert "x-amz-server-side-encryption" in signed_headers(headers)
      assert "x-amz-server-side-encryption-aws-kms-key-id" in signed_headers(headers)
    end

    test "kms without a key id leaves the choice to the bucket's default key", ctx do
      assert :ok = S3.put(:i, "seg/a", "data", [sse: [type: :kms]] ++ ctx.s3)

      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      assert headers["x-amz-server-side-encryption"] == "aws:kms"
      refute Map.has_key?(headers, "x-amz-server-side-encryption-aws-kms-key-id")
    end

    test "no :sse sends no x-amz-server-side-encryption", ctx do
      assert :ok = S3.put(:i, "seg/a", "data", ctx.s3)
      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      refute Enum.any?(Map.keys(headers), &String.starts_with?(&1, "x-amz-server-side"))
    end

    test "only a PUT carries it: the provider decrypts reads, and deletes name no key", ctx do
      opts = [sse: %{type: :kms, key_id: @kms_key}] ++ ctx.s3

      assert {:ok, "ok"} = S3.get(:i, "seg/a", opts)
      assert {:ok, "ok"} = S3.get_range(:i, "seg/a", 0, 2, opts)
      assert :ok = S3.delete(:i, "seg/a", opts)

      for {_method, _path, _query, headers} <- seen(ctx.capture) do
        refute Enum.any?(Map.keys(headers), &String.starts_with?(&1, "x-amz-server-side"))
      end
    end

    test "a bad option raises instead of writing the object unencrypted", ctx do
      assert_raise ArgumentError, ~r/sse type must be :aes256 or :kms/, fn ->
        S3.put(:i, "seg/a", "data", [sse: %{type: :aes}] ++ ctx.s3)
      end

      assert seen(ctx.capture) == []
    end
  end

  describe "GCS" do
    test "kms: the key name as a header and as the upload's kmsKeyName", ctx do
      key = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
      assert :ok = GCS.put(:i, "seg/a", "data", [sse: %{type: :kms, key_id: key}] ++ ctx.gcs)

      assert [{"POST", path, query, headers}] = seen(ctx.capture)
      assert path == "/upload/storage/v1/b/b/o"
      assert headers["x-goog-encryption-kms-key-name"] == key

      assert %{"uploadType" => "media", "name" => "seg/a", "kmsKeyName" => ^key} =
               URI.decode_query(query)
    end

    test "aes256 sends nothing: GCS encrypts every object itself", ctx do
      assert :ok = GCS.put(:i, "seg/a", "data", [sse: %{type: :aes256}] ++ ctx.gcs)

      assert [{"POST", _path, query, headers}] = seen(ctx.capture)
      refute Map.has_key?(headers, "x-goog-encryption-kms-key-name")
      refute query =~ "kmsKeyName"
    end
  end

  describe "Azure" do
    test "kms: the encryption scope", ctx do
      assert :ok =
               Azure.put(
                 :i,
                 "seg/a",
                 "data",
                 [sse: %{type: :kms, key_id: "ankusa-scope"}] ++ ctx.azure
               )

      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      assert headers["x-ms-encryption-scope"] == "ankusa-scope"
      assert headers["x-ms-blob-type"] == "BlockBlob"
    end

    test "aes256 sends nothing", ctx do
      assert :ok = Azure.put(:i, "seg/a", "data", [sse: %{type: :aes256}] ++ ctx.azure)
      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      refute Map.has_key?(headers, "x-ms-encryption-scope")
    end
  end

  describe "OCI" do
    test "kms: opc-sse-kms-key-id", ctx do
      ocid = "ocid1.key.oc1.iad.example"
      assert :ok = OCI.put(:i, "seg/a", "data", [sse: %{type: :kms, key_id: ocid}] ++ ctx.oci)

      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      assert headers["opc-sse-kms-key-id"] == ocid
      assert headers["authorization"] =~ ~s|headers="date (request-target) host|
    end

    test "aes256 sends nothing", ctx do
      assert :ok = OCI.put(:i, "seg/a", "data", [sse: %{type: :aes256}] ++ ctx.oci)
      assert [{"PUT", _path, _query, headers}] = seen(ctx.capture)
      refute Map.has_key?(headers, "opc-sse-kms-key-id")
    end
  end

  describe "config validation" do
    defp config(storage: store), do: Ankusa.Config.new(storage: %{blob_store: store})

    test "every adapter accepts its shapes" do
      for store <- [
            {S3, @s3 ++ [sse: %{type: :aes256}]},
            {S3, @s3 ++ [sse: %{type: :kms}]},
            {S3, @s3 ++ [sse: %{type: :kms, key_id: @kms_key}]},
            {S3, @s3 ++ [sse: [type: :kms, key_id: @kms_key]]},
            {S3, @s3 ++ [sse: nil]},
            {GCS, @gcs ++ [sse: %{type: :kms, key_id: "projects/p/k"}]},
            {GCS, @gcs ++ [sse: %{type: :aes256}]},
            {Azure, @azure ++ [sse: %{type: :kms, key_id: "scope"}]},
            {OCI, @oci ++ [sse: %{type: :kms, key_id: "ocid1.key"}]},
            {Ankusa.BlobStore.LocalFS, []},
            {SomeThirdPartyStore, [sse: :whatever_it_means]}
          ] do
        assert :ok = BlobStore.validate_config!(config(storage: store))
      end
    end

    test "a bad option names the key that holds it" do
      for {store, message} <- [
            {{S3, @s3 ++ [sse: %{type: :bogus}]},
             "storage.blob_store: sse type must be :aes256 or :kms, got :bogus"},
            {{S3, @s3 ++ [sse: "AES256"]}, "storage.blob_store: sse must be a map"},
            {{S3, @s3 ++ [sse: %{type: :kms, kms: "x"}]}, "sse has unknown keys [:kms]"},
            {{S3, @s3 ++ [sse: %{type: :aes256, key_id: "x"}]}, "key_id is only for type :kms"},
            {{S3, @s3 ++ [sse: %{type: :kms, key_id: ""}]}, "key_id must be a non-empty string"},
            {{GCS, @gcs ++ [sse: %{type: :kms}]},
             "storage.blob_store: sse type :kms needs a key_id on gcs"},
            {{Azure, @azure ++ [sse: %{type: :kms}]}, "needs a key_id on azure"},
            {{OCI, @oci ++ [sse: %{type: :kms}]}, "needs a key_id on oci"},
            {{Ankusa.BlobStore.LocalFS, [sse: %{type: :aes256}]},
             "not supported by Ankusa.BlobStore.LocalFS"}
          ] do
        error =
          assert_raise ArgumentError, fn -> BlobStore.validate_config!(config(storage: store)) end

        assert error.message =~ message
      end
    end

    test "the backup store and every claim store are checked too" do
      backup = Ankusa.Config.new(backup: %{blob_store: {GCS, @gcs ++ [sse: %{type: :kms}]}})

      assert_raise ArgumentError,
                   ~r/backup.blob_store: sse type :kms needs a key_id on gcs/,
                   fn ->
                     BlobStore.validate_config!(backup)
                   end

      claims =
        Ankusa.Config.new(
          claim_check: %{blob_stores: [{S3, @s3}, {Azure, @azure ++ [sse: %{type: :kms}]}]}
        )

      assert_raise ArgumentError,
                   ~r/claim_check.blob_stores\[1\]: sse type :kms needs a key_id on azure/,
                   fn ->
                     BlobStore.validate_config!(claims)
                   end
    end

    test "headers/2 is empty without the option" do
      for adapter <- [:s3, :gcs, :azure, :oci], do: assert(SSE.headers([], adapter) == [])
      assert SSE.query([], :gcs) == []
    end
  end
end
