defmodule Ankusa.Crypto.Envelope do
  @moduledoc """
  Envelope encryption for the secrets Ankusa stores: a source's `verify.secret`
  and its sinks' credentials (URLs with a password, header values, tokens, keys),
  which `Ankusa.SourceStore.Persistent` keeps in this node's RocksDB store (and
  so in every backup of it) and `Ankusa.SourceStore.Redis` keeps in Redis.

  ## Value format

  One encrypted value is

      <<"AENC", 1::8, key_id_len::8, key_id::binary, iv::96, tag::128, ciphertext::binary>>

  AES-256-GCM (`:crypto.crypto_one_time_aead/7`) under the key-encryption key
  (KEK) named `key_id`, with a fresh random 96-bit IV per value. The bytes up to
  and including `key_id` are the authenticated data, so a value cannot be
  re-labelled with another key's id. In a stored JSON spec each hidden string is
  replaced by `"AENC:" <> Base.encode64(value)`.

  ## Which fields

  The fields `Ankusa.Admin.Redact` hides from the admin API are the fields this
  module encrypts (`Ankusa.Admin.Redact.reduce_hidden/3`): one allowlist decides
  both what the API may show and what may rest in the clear. Identity and
  structure (`type`, `tenant`, `signature_header`, topics, regions, …) stay
  readable, so a stored spec can still be inspected and diffed.

  ## Keys

  `Ankusa.Config`'s `secrets` section:

    * `:kek` — the current key: 32 random bytes, base64 (`openssl rand -base64
      32`), optionally `"<id>:<base64>"`. The id names the key in every value it
      encrypts; without one it is the first eight hex characters of the key's
      SHA-256.
    * `:kek_provider` — `{module, fun, args}` instead of `:kek`, called once when
      a store starts, returning `{:ok, [kek | previous]}`: each element is a
      `:kek` string or an `{id, 32-byte key}` pair, the first one current.
    * `:kek_previous` — keys that may still decrypt, never encrypt, so a rotation
      can be rolled out node by node and rows rewritten with
      `mix ankusa.secrets.rewrap`.

  With no KEK new rows are stored as plain JSON, as before; rows written earlier
  under a KEK are then **refused at boot** rather than served as ciphertext.
  """

  alias Ankusa.Admin.Redact
  alias Ankusa.Config

  @version 1
  @iv_bytes 12
  @tag_bytes 16
  @key_bytes 32
  @prefix "AENC:"
  @id_format ~r/\A[A-Za-z0-9._-]{1,32}\z/

  defmodule Keyring do
    @moduledoc """
    The keys resolved from a `secrets` section (`Ankusa.Crypto.Envelope.keyring/1`):
    the current key and the id it is named by, and every key that may decrypt.
    `inspect/1` shows the current key's id only, so a crash report that prints a
    store's state never prints a key.
    """

    @derive {Inspect, only: [:current_id]}
    defstruct current_id: nil, current: nil, keys: %{}

    @type t :: %__MODULE__{
            current_id: String.t() | nil,
            current: binary() | nil,
            keys: %{String.t() => binary()}
          }
  end

  @type keyring :: Keyring.t()

  @typedoc "Why a value could not be read or a keyring built. Never carries key bytes."
  @type reason ::
          :no_kek
          | :not_encrypted
          | :malformed
          | :tampered
          | {:unknown_key, String.t()}
          | {:unsupported_version, non_neg_integer()}
          | {:bad_kek, :not_base64 | :wrong_size | :bad_id | :not_a_key | :duplicate_id}
          | {:kek_provider, term()}

  # ── keyring ─────────────────────────────────────────────────────────────────

  @doc """
  Resolve a config's `secrets` section into a keyring. Calls `:kek_provider`
  once, if there is one.
  """
  @spec keyring(map()) :: {:ok, keyring()} | {:error, reason()}
  def keyring(secrets) when is_map(secrets) do
    with {:ok, current} <- current_keys(secrets),
         {:ok, previous} <- parse_all(Map.get(secrets, :kek_previous) || []),
         {:ok, keys} <- index(current ++ previous) do
      case current do
        [{id, key} | _] -> {:ok, %Keyring{current_id: id, current: key, keys: keys}}
        [] -> {:ok, %Keyring{keys: keys}}
      end
    end
  end

  defp current_keys(%{kek_provider: {module, fun, args}})
       when is_atom(module) and is_atom(fun) and is_list(args),
       do: provided(module, fun, args)

  defp current_keys(%{kek: kek}) when is_binary(kek), do: parse_all([kek])
  defp current_keys(_secrets), do: {:ok, []}

  # A provider is operator code that may reach a KMS: whatever it raises or
  # returns is reported as its failure, never as a crash of the store that asked.
  defp provided(module, fun, args) do
    case apply(module, fun, args) do
      {:ok, [_ | _] = keys} -> parse_all(keys)
      {:ok, _empty} -> {:error, {:kek_provider, :no_keys}}
      {:error, reason} -> {:error, {:kek_provider, reason}}
      _other -> {:error, {:kek_provider, :bad_return}}
    end
  rescue
    error -> {:error, {:kek_provider, {:raised, error.__struct__}}}
  catch
    kind, _reason -> {:error, {:kek_provider, kind}}
  end

  defp parse_all(keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case parse_kek(key) do
        {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      error -> error
    end
  end

  # Two entries with one id must be one key.
  defp index(parsed) do
    Enum.reduce_while(parsed, {:ok, %{}}, fn {id, key}, {:ok, acc} ->
      case acc do
        %{^id => ^key} -> {:cont, {:ok, acc}}
        %{^id => _other} -> {:halt, {:error, {:bad_kek, :duplicate_id}}}
        _ -> {:cont, {:ok, Map.put(acc, id, key)}}
      end
    end)
  end

  @doc false
  @spec parse_kek(term()) :: {:ok, {String.t(), binary()}} | {:error, reason()}
  def parse_kek({id, key}) when is_binary(id) and is_binary(key) do
    cond do
      byte_size(key) != @key_bytes -> {:error, {:bad_kek, :wrong_size}}
      not Regex.match?(@id_format, id) -> {:error, {:bad_kek, :bad_id}}
      true -> {:ok, {id, key}}
    end
  end

  def parse_kek(value) when is_binary(value) do
    {id, encoded} =
      case String.split(value, ":", parts: 2) do
        [id, encoded] -> {id, encoded}
        [encoded] -> {nil, encoded}
      end

    with {:ok, key} <- decode_key(encoded) do
      parse_kek({id || default_id(key), key})
    end
  end

  def parse_kek(_other), do: {:error, {:bad_kek, :not_a_key}}

  defp decode_key(encoded) do
    case Base.decode64(String.trim(encoded)) do
      {:ok, key} -> {:ok, key}
      :error -> {:error, {:bad_kek, :not_base64}}
    end
  end

  defp default_id(key),
    do: :sha256 |> :crypto.hash(key) |> Base.encode16(case: :lower) |> binary_part(0, 8)

  @doc "The id of the key new values are encrypted under, or `nil` with no KEK."
  @spec current_key_id(keyring()) :: String.t() | nil
  def current_key_id(%Keyring{current_id: id}), do: id

  @doc "Is there a key to encrypt with?"
  @spec configured?(keyring()) :: boolean()
  def configured?(%Keyring{current: current}), do: current != nil

  # ── config ──────────────────────────────────────────────────────────────────

  @doc """
  Check the `secrets` section's shape. Never calls `:kek_provider` (that is
  I/O, and happens once when a store starts), and never prints a key.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{secrets: secrets}) do
    kek = secrets.kek
    provider = secrets.kek_provider
    previous = secrets.kek_previous

    unless kek == nil or is_binary(kek), do: bad!("kek", "a string")
    unless is_list(previous), do: bad!("kek_previous", "a list of strings")

    unless provider == nil or
             match?({m, f, a} when is_atom(m) and is_atom(f) and is_list(a), provider),
           do: bad!("kek_provider", "nil or {module, fun, args}")

    if kek != nil and provider != nil,
      do: raise(ArgumentError, "secrets.kek and secrets.kek_provider are mutually exclusive")

    if previous != [] and kek == nil and provider == nil,
      do: raise(ArgumentError, "secrets.kek_previous needs secrets.kek or secrets.kek_provider")

    check_static!(
      if(kek, do: [{"kek", kek}], else: []) ++ Enum.map(previous, &{"kek_previous", &1})
    )

    :ok
  end

  defp check_static!(entries) do
    parsed =
      Enum.map(entries, fn {name, value} ->
        case parse_kek(value) do
          {:ok, parsed} -> parsed
          {:error, {:bad_kek, why}} -> bad_key!(name, why)
        end
      end)

    with {:error, {:bad_kek, why}} <- index(parsed), do: bad_key!("kek", why)
    :ok
  end

  defp bad!(key, expected), do: raise(ArgumentError, "secrets.#{key} must be #{expected}")

  defp bad_key!(name, why) do
    raise ArgumentError,
          "secrets.#{name} must be a base64-encoded 32-byte key, optionally \"<id>:<base64>\" " <>
            "with an id of 1-32 characters from [A-Za-z0-9._-] (#{why})"
  end

  @doc """
  One line for the startup log: how the secrets rest. Does not call
  `:kek_provider`, so it names the provider, not its key.
  """
  @spec describe(Config.t()) :: String.t()
  def describe(%Config{secrets: %{kek_provider: {module, fun, args}}}),
    do:
      "secrets envelope-encrypted by the key provider #{Exception.format_mfa(module, fun, length(args))}"

  def describe(%Config{secrets: %{kek: kek}}) when is_binary(kek) do
    case parse_kek(kek) do
      {:ok, {id, _key}} -> "secrets envelope-encrypted with key #{id}"
      {:error, _reason} -> "secrets.kek is invalid"
    end
  end

  def describe(%Config{}), do: "secrets are stored in plaintext (no secrets.kek is configured)"

  # ── values ──────────────────────────────────────────────────────────────────

  @doc "Encrypt `plaintext` under the current key. Raises with no KEK."
  @spec encrypt(keyring(), iodata()) :: binary()
  def encrypt(%Keyring{current: nil}, _plaintext),
    do: raise(ArgumentError, "no secrets.kek is configured")

  def encrypt(%Keyring{current_id: id, current: key}, plaintext) do
    iv = :crypto.strong_rand_bytes(@iv_bytes)
    header = <<"AENC", @version::8, byte_size(id)::8, id::binary>>

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, header, @tag_bytes, true)

    <<header::binary, iv::binary, tag::binary, ciphertext::binary>>
  end

  @doc "Decrypt a value `encrypt/2` produced, under whichever of the keyring's keys named it."
  @spec decrypt(keyring(), binary()) :: {:ok, binary()} | {:error, reason()}
  def decrypt(
        %Keyring{keys: keys},
        <<"AENC", @version::8, id_len::8, id::binary-size(id_len), iv::binary-size(@iv_bytes),
          tag::binary-size(@tag_bytes), ciphertext::binary>>
      ) do
    case keys do
      %{^id => key} ->
        header = <<"AENC", @version::8, id_len::8, id::binary>>

        case :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, header, tag, false) do
          plaintext when is_binary(plaintext) -> {:ok, plaintext}
          _error -> {:error, :tampered}
        end

      _ when map_size(keys) == 0 ->
        {:error, :no_kek}

      _ ->
        {:error, {:unknown_key, id}}
    end
  end

  def decrypt(_keyring, <<"AENC", version::8, _rest::binary>>) when version != @version,
    do: {:error, {:unsupported_version, version}}

  def decrypt(_keyring, <<"AENC", _rest::binary>>), do: {:error, :malformed}
  def decrypt(_keyring, _other), do: {:error, :not_encrypted}

  @doc "Is this binary an encrypted value (by its magic)?"
  @spec encrypted?(binary()) :: boolean()
  def encrypted?(<<"AENC", _rest::binary>>), do: true
  def encrypted?(_other), do: false

  # ── specs ───────────────────────────────────────────────────────────────────

  @doc """
  Encrypt every hidden field of a source spec (see the module doc). With no KEK
  the spec is returned as it is.
  """
  @spec seal(keyring(), map()) :: map()
  def seal(%Keyring{current: nil}, spec), do: spec
  def seal(%Keyring{} = ring, spec), do: Redact.map_hidden(spec, &seal_string(ring, &1))

  defp seal_string(ring, string), do: @prefix <> Base.encode64(encrypt(ring, string))

  @doc """
  Decrypt every encrypted string of a stored spec. A plain spec comes back
  unchanged; an encrypted one that no key of the keyring opens is an error
  (`:no_kek`, `{:unknown_key, id}`, `:tampered`), never half-decrypted.
  """
  @spec unseal(keyring(), map()) :: {:ok, map()} | {:error, reason()}
  def unseal(%Keyring{} = ring, spec) when is_map(spec) do
    with {:ok, plain, _ids} <- walk(ring, spec, MapSet.new()), do: {:ok, plain}
  end

  @doc """
  The spec to store when a row is moved to the current key: `{:ok, sealed}`
  when it holds a plaintext hidden field or a value under another key, `:current`
  when there is nothing to do.
  """
  @spec rewrap_spec(keyring(), map()) :: {:ok, map()} | :current | {:error, reason()}
  def rewrap_spec(%Keyring{current: nil}, _spec), do: {:error, :no_kek}

  def rewrap_spec(%Keyring{current_id: current} = ring, spec) when is_map(spec) do
    with {:ok, plain, ids} <- walk(ring, spec, MapSet.new()) do
      {_spec, plaintext?} =
        Redact.reduce_hidden(spec, false, fn string, acc ->
          {string, acc or not sealed_string?(string)}
        end)

      if plaintext? or Enum.any?(ids, &(&1 != current)),
        do: {:ok, seal(ring, plain)},
        else: :current
    end
  end

  defp sealed_string?(@prefix <> encoded) do
    match?({:ok, <<"AENC", _rest::binary>>}, Base.decode64(encoded))
  end

  defp sealed_string?(_string), do: false

  # Every string, not only the hidden positions: a value that was sealed rests
  # wherever it was put, and a string that merely starts with the prefix but is
  # no envelope is somebody's plaintext and stays as it is.
  defp walk(ring, map, ids) when is_map(map) do
    Enum.reduce_while(map, {:ok, %{}, ids}, fn {key, value}, {:ok, acc, ids} ->
      case walk(ring, value, ids) do
        {:ok, value, ids} -> {:cont, {:ok, Map.put(acc, key, value), ids}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp walk(ring, list, ids) when is_list(list) do
    list
    |> Enum.reduce_while({:ok, [], ids}, fn value, {:ok, acc, ids} ->
      case walk(ring, value, ids) do
        {:ok, value, ids} -> {:cont, {:ok, [value | acc], ids}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc, ids} -> {:ok, Enum.reverse(acc), ids}
      error -> error
    end
  end

  defp walk(ring, @prefix <> encoded = string, ids) do
    case Base.decode64(encoded) do
      {:ok, <<"AENC", @version::8, id_len::8, id::binary-size(id_len), _rest::binary>> = blob} ->
        with {:ok, plain} <- decrypt(ring, blob), do: {:ok, plain, MapSet.put(ids, id)}

      # An unknown version or a truncated value: `decrypt/2` can only refuse it.
      {:ok, <<"AENC", _rest::binary>> = blob} ->
        {:error, _reason} = decrypt(ring, blob)

      _not_an_envelope ->
        {:ok, string, ids}
    end
  end

  defp walk(_ring, other, ids), do: {:ok, other, ids}
end
