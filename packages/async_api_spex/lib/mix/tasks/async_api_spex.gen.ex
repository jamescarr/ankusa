defmodule Mix.Tasks.AsyncApiSpex.Gen do
  @shortdoc "Writes an AsyncAPI document from a spec module"

  @moduledoc """
  Writes an AsyncAPI 3.0 document to a file.

      mix async_api_spex.gen --spec MyApp.AsyncApi --output asyncapi.json

  `--spec` is a module implementing `AsyncApiSpex.Spec`; `--output` defaults to
  `asyncapi.json`. The document is validated first, and an invalid document
  stops the task with the validation errors.
  """

  use Mix.Task

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [spec: :string, output: :string])

    spec = opts[:spec] || Mix.raise("missing required --spec MODULE")
    output = opts[:output] || "asyncapi.json"

    Mix.Task.run("compile")

    module = Module.concat([spec])

    unless Code.ensure_loaded?(module) and function_exported?(module, :spec, 0) do
      Mix.raise("#{spec} does not implement AsyncApiSpex.Spec (no spec/0)")
    end

    document = module.spec()

    case AsyncApiSpex.validate(document) do
      :ok ->
        :ok

      {:error, messages} ->
        Mix.raise("invalid AsyncAPI document:\n" <> Enum.join(messages, "\n"))
    end

    File.write!(output, AsyncApiSpex.encode!(document))
    Mix.shell().info("wrote #{output}")
  end
end
