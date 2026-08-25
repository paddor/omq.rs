beam_root = Path.expand("../..", __DIR__)
Code.prepend_path(Path.join([beam_root, "_build/default/lib/omq/ebin"]))

source =
  [
    Path.join([beam_root, "native/target/debug/libomq_beam_native.so"]),
    Path.join([beam_root, "native/target/release/libomq_beam_native.so"]),
    Path.join([beam_root, "priv/omq_beam_native.so"])
  ]
  |> Enum.find(fn path ->
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} -> size > 0
      _ -> false
    end
  end)

target = Path.join([beam_root, "_build/default/lib/omq/priv/omq_beam_native.so"])
File.mkdir_p!(Path.dirname(target))
File.cp!(source, target)

ExUnit.start()
