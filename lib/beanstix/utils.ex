defmodule Beanstix.Utils do

  def to_command(cmd) when is_atom(cmd) do
    cmd
    |> Atom.to_string()
    |> String.replace("_", "-")
  end

end
