defmodule Beanstix do

  @type command :: {atom}
  @default_timeout 5000

  @spec start_link(binary | Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(args \\ [], opts \\ []) do
    Beanstix.Connection.start_link(args, opts)
  end

  @spec stop(GenServer.server) :: :ok
  def stop(pid) do
    Beanstix.Connection.stop(pid)
  end

  def command(pid, command, opts \\ []) do
    Beanstix.Connection.command(pid, command, opts[:timeout] || @default_timeout)
  end

end
