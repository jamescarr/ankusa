defmodule AnkusaServer.Banner do
  @moduledoc """
  The boot banner: the goad, the name, the tagline, the versions. Printed once
  by `AnkusaServer.Application` when the server starts (unless `log.banner` is
  false), never by the `bin/ankusa eval` commands, whose stdout is parsed.
  """

  # Lists of lines, not heredocs, so the leading spaces survive `mix format`.
  # The `▓` runs are the yellow bands of the logo.
  @goad [
    "         ▄▄▄▄▄▄▄▄",
    "       ▄█▀▀▀▀▀▀▀▀█▄",
    "      ▟▛          ▀█▖",
    "    ▗▟▛            ▐█",
    "   ▐▓▓▌            ▐▛",
    "   ▟█▛            ▗▛",
    "  ▟█▛             ▘",
    " ▟█▛",
    "▐▓▓▌",
    "██▛",
    "▀▀"
  ]

  @title [
    " █████╗ ███╗   ██╗██╗  ██╗██╗   ██╗███████╗ █████╗",
    "██╔══██╗████╗  ██║██║ ██╔╝██║   ██║██╔════╝██╔══██╗",
    "███████║██╔██╗ ██║█████╔╝ ██║   ██║███████╗███████║",
    "██╔══██║██║╚██╗██║██╔═██╗ ██║   ██║╚════██║██╔══██║",
    "██║  ██║██║ ╚████║██║  ██╗╚██████╔╝███████║██║  ██║",
    "╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝"
  ]

  @tagline "Don't fight the traffic. Steer it."
  @goad_width 22

  @doc "Print the banner to stdout, in colour when the terminal takes it."
  @spec print() :: :ok
  def print do
    AnkusaServer.Application.version()
    |> render(AnkusaServer.Application.core_version(), IO.ANSI.enabled?())
    |> IO.puts()
  end

  @doc """
  The banner as a string: a blank line, eleven rows, a trailing newline. With
  `ansi?` false the result is plain glyphs, so `docker logs` without a TTY gets
  no escape codes.
  """
  @spec render(String.t(), String.t(), boolean()) :: String.t()
  def render(server_version, core_version, ansi?) do
    rows =
      @goad
      |> Enum.zip(right_column(server_version, core_version))
      |> Enum.map(fn
        {goad, nil} -> goad_part(goad, ansi?)
        {goad, {style, text}} -> goad_part(pad(goad), ansi?) <> format([style, text], ansi?)
      end)

    "\n" <> Enum.join(rows, "\n") <> "\n"
  end

  # One entry per goad row: nil leaves the row to the goad alone.
  defp right_column(server_version, core_version) do
    [nil, nil] ++
      Enum.map(@title, &{[:bright, :light_blue], &1}) ++
      [
        nil,
        {:yellow, "  " <> @tagline},
        {:faint, "  ankusa_server #{server_version} · ankusa #{core_version}"}
      ]
  end

  defp pad(goad), do: String.pad_trailing(goad, @goad_width) <> "  "

  defp goad_part(line, ansi?) do
    segments =
      ~r/(▓+)/u
      |> Regex.split(line, include_captures: true, trim: true)
      |> Enum.map(fn
        "▓" <> _ = band -> [:yellow, band, :light_blue]
        text -> text
      end)

    format([:light_blue | segments], ansi?)
  end

  defp format(chardata, ansi?), do: chardata |> IO.ANSI.format(ansi?) |> IO.iodata_to_binary()
end
