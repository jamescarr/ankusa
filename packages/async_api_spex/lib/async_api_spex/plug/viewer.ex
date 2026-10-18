if Code.ensure_loaded?(Plug) do
  defmodule AsyncApiSpex.Plug.Viewer do
    @moduledoc """
    A Plug that serves an HTML page rendering an AsyncAPI document in the
    browser with the [AsyncAPI React component](https://github.com/asyncapi/asyncapi-react):
    servers, channels, operations, messages, and schemas.

    The page fetches the document from the `:path` it is given, so mount it
    next to `AsyncApiSpex.Plug.RenderSpec`:

        # Plug.Router
        forward "/asyncapi.json", to: AsyncApiSpex.Plug.RenderSpec, init_opts: [spec: Shop.AsyncApi]
        forward "/asyncapi",      to: AsyncApiSpex.Plug.Viewer,     init_opts: [path: "/asyncapi.json"]

        # Phoenix router
        forward "/asyncapi.json", AsyncApiSpex.Plug.RenderSpec, spec: Shop.AsyncApi
        forward "/asyncapi",      AsyncApiSpex.Plug.Viewer,     path: "/asyncapi.json"

    The browser resolves `:path` relative to the page, so give it the absolute
    path the document is mounted at (including any `scope` prefix).

    Options:

      * `:path` (required) — the URL the browser fetches the document from.
      * `:title` — the page's `<title>`; defaults to `"AsyncAPI"`.
      * `:bundle_url` — the component's standalone bundle; defaults to
        `@asyncapi/react-component` 3.2.1's `browser/standalone/index.js` on unpkg.
      * `:stylesheet_url` — the component's stylesheet; defaults to the same
        version's `styles/default.min.css` on unpkg.
      * `:config` — a map passed to the component as its `config` prop;
        defaults to `%{"show" => %{"sidebar" => true}}`.

    The bundle and stylesheet are loaded from unpkg; point `:bundle_url` and
    `:stylesheet_url` at a self-hosted copy to keep the page off a CDN. Under a
    Content Security Policy the page also needs its one inline `<script>` (the
    `render` call carrying `:path` and `:config`) allowed, with
    `'unsafe-inline'` or that script's hash in `script-src`.
    """

    @behaviour Plug

    @defaults [
      title: "AsyncAPI",
      bundle_url: "https://unpkg.com/@asyncapi/react-component@3.2.1/browser/standalone/index.js",
      stylesheet_url: "https://unpkg.com/@asyncapi/react-component@3.2.1/styles/default.min.css",
      config: %{"show" => %{"sidebar" => true}}
    ]

    @allowed [:path | Keyword.keys(@defaults)]

    @doc """
    Builds the HTML page.

    Raises `ArgumentError` when the `:path` option is missing or an option is
    unknown.
    """
    @impl Plug
    @spec init(keyword()) :: binary()
    def init(opts) do
      case Keyword.keys(opts) -- @allowed do
        [] ->
          :ok

        unknown ->
          raise ArgumentError,
                "unknown option(s) #{inspect(unknown)} for AsyncApiSpex.Plug.Viewer; " <>
                  "allowed options: #{inspect(@allowed)}"
      end

      path =
        case Keyword.fetch(opts, :path) do
          {:ok, path} -> path
          :error -> raise ArgumentError, "AsyncApiSpex.Plug.Viewer requires a :path option"
        end

      opts = Keyword.merge(@defaults, opts)

      """
      <!doctype html>
      <html lang="en"><head><meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>#{html(opts[:title])}</title>
      <link rel="stylesheet" href="#{html(opts[:stylesheet_url])}">
      </head><body>
      <div id="asyncapi"></div>
      <script src="#{html(opts[:bundle_url])}"></script>
      <script>
      AsyncApiStandalone.render({schema: {url: #{script_json(path)}, options: {method: "GET", mode: "cors"}}, config: #{script_json(opts[:config])}}, document.getElementById("asyncapi"));
      </script>
      </body></html>
      """
    end

    @doc """
    Sends the page as a `200` `text/html` response and halts the connection.
    """
    @impl Plug
    @spec call(Plug.Conn.t(), binary()) :: Plug.Conn.t()
    def call(conn, html) do
      conn
      |> Plug.Conn.put_resp_content_type("text/html")
      |> Plug.Conn.send_resp(200, html)
      |> Plug.Conn.halt()
    end

    defp html(value), do: value |> to_string() |> Plug.HTML.html_escape()

    # JSON inside a <script> block: "</" would let a string close the block.
    defp script_json(value), do: value |> JSON.encode!() |> String.replace("</", "<\\/")
  end
end
