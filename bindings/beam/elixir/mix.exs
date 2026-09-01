defmodule Omq.MixProject do
  use Mix.Project

  def project do
    [
      app: :omq_elixir,
      version: "0.1.0",
      elixir: "~> 1.16",
      description: description(),
      package: package(),
      source_url: "https://github.com/paddor/omq.rs",
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:omq, "~> 0.1"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Elixir wrapper for OMQ.beam, backed by the Erlang OMQ NIF package."
  end

  defp package do
    [
      licenses: ["ISC"],
      links: %{
        "GitHub" => "https://github.com/paddor/omq.rs",
        "OMQ.beam" => "https://github.com/paddor/omq.rs/tree/main/bindings/beam"
      },
      files: ~w(lib mix.exs README.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "main",
      source_url: "https://github.com/paddor/omq.rs"
    ]
  end
end
