defmodule AsyncApiSpex.Spec do
  @moduledoc """
  Behaviour for a module that returns a static AsyncAPI document.

  `AsyncApiSpex.Plug.RenderSpec` and `mix async_api_spex.gen` both call
  `spec/0`. Applications whose channels are known only at runtime do not
  implement this behaviour; they build the `AsyncApiSpex.Document` themselves.

  ## Assembling the document from channel modules

  `use AsyncApiSpex.Spec` implements `spec/0` for you by collecting every module
  that declares a channel with `use AsyncApiSpex.Channel`:

      defmodule Shop.AsyncApi do
        use AsyncApiSpex.Spec,
          otp_app: :shop,
          info: [title: "Shop", version: "1.0.0"]
      end

  Options:

    * `:info` (required) — a keyword list literal of `AsyncApiSpex.Info` fields
      that includes `:title` and `:version`. Values are evaluated when the
      document is built.
    * `:otp_app` — an atom literal; every module of that application that uses
      `AsyncApiSpex.Channel` is included, in module-name order. The application
      must be loaded. Zero modules yield a document with no channels.
    * `:channels` — a non-empty list of channel modules to include, instead of
      `:otp_app`. Exactly one of `:otp_app` and `:channels` is required.
    * `:id`, `:default_content_type`, `:extensions` — the same fields of
      `AsyncApiSpex.Document`.

  Two channels may share a server only if both declare the same
  `AsyncApiSpex.Server`; two channels with the same id, or two different
  servers with the same id, raise `ArgumentError` when the document is built.

  `spec/0` is overridable, so an application can extend the generated document:

      def spec do
        doc = super()
        %{doc | extensions: Map.put(doc.extensions, "x-team", "payments")}
      end

  A hand-written `spec/0` that builds the `AsyncApiSpex.Document` itself remains
  supported; implement the behaviour without `use`.
  """

  @allowed [:info, :otp_app, :channels, :id, :default_content_type, :extensions]

  @doc "Returns the AsyncAPI document to serve or serialize."
  @callback spec() :: AsyncApiSpex.Document.t()

  @doc """
  Implements `spec/0` by assembling a document from channel modules.

  See the module documentation for the options.
  """
  defmacro __using__(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "use AsyncApiSpex.Spec expects a keyword list, got: #{inspect(opts)}"
    end

    case Keyword.keys(opts) -- @allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown option(s) #{inspect(unknown)} for use AsyncApiSpex.Spec; " <>
                "allowed options: #{inspect(@allowed)}"
    end

    info_ast = info!(opts)
    source = source!(opts, __CALLER__)

    build_opts =
      [{:info, info_ast}, source] ++ Keyword.take(opts, [:id, :default_content_type, :extensions])

    quote do
      @behaviour AsyncApiSpex.Spec

      @doc "Returns the AsyncAPI document assembled from the declared channels."
      @impl AsyncApiSpex.Spec
      def spec, do: AsyncApiSpex.Spec.Builder.build(unquote(build_opts))

      defoverridable spec: 0
    end
  end

  defp info!(opts) do
    info =
      case Keyword.fetch(opts, :info) do
        {:ok, info} when is_list(info) ->
          if Keyword.keyword?(info) do
            info
          else
            raise ArgumentError, "use AsyncApiSpex.Spec :info must be a keyword list"
          end

        {:ok, _other} ->
          raise ArgumentError, "use AsyncApiSpex.Spec :info must be a keyword list literal"

        :error ->
          raise ArgumentError, "use AsyncApiSpex.Spec :info requires :title and :version"
      end

    allowed = Map.keys(%AsyncApiSpex.Info{}) -- [:__struct__]

    case Keyword.keys(info) -- allowed do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown key(s) #{inspect(unknown)} in use AsyncApiSpex.Spec :info; " <>
                "allowed keys: #{inspect(allowed)}"
    end

    unless Keyword.has_key?(info, :title) and Keyword.has_key?(info, :version) do
      raise ArgumentError, "use AsyncApiSpex.Spec :info requires :title and :version"
    end

    info
  end

  defp source!(opts, caller) do
    case {Keyword.fetch(opts, :channels), Keyword.fetch(opts, :otp_app)} do
      {{:ok, [_ | _] = channels}, :error} ->
        {:channels, channels}

      {{:ok, _other}, :error} ->
        raise ArgumentError, "use AsyncApiSpex.Spec :channels must be a non-empty list of modules"

      {:error, {:ok, ast}} ->
        case Macro.expand(ast, caller) do
          app when is_atom(app) and app not in [nil, true, false] ->
            {:otp_app, app}

          _other ->
            raise ArgumentError, "use AsyncApiSpex.Spec :otp_app must be an atom literal"
        end

      _ ->
        raise ArgumentError, "use AsyncApiSpex.Spec requires exactly one of :channels or :otp_app"
    end
  end
end
