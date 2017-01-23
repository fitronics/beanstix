ExUnit.start()

host = '127.0.0.1'
port = 11300

case :gen_tcp.connect(host, port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Failed to connect to Beanstalkd (tcp://#{host}:#{port}): #{:inet.format_error(reason)}"
end

defmodule Beanstix.TestHelpers do
  use ExUnit.Case

  @host '127.0.0.1'
  @port 11300

  def setup_connection(context) do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {:ok, pid} = Beanstix.start_link(host: @host, port: @port)
      {m, s, ms} = :os.timestamp
      tube = "Beanstix_#{m}_#{s}_#{ms}"
      Beanstix.command(pid, {:use, tube})
      Beanstix.command(pid, {:watch, tube})
      Beanstix.command(pid, {:ignore, "default"})
      on_exit(fn -> Beanstix.stop(pid) end)
      {:ok, %{pid: pid, tube: tube}}
    end
  end

end
