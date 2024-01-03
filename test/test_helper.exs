ExUnit.start()

host = ~c"127.0.0.1"
port = 11300

case :gen_tcp.connect(host, port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)

  {:error, reason} ->
    Mix.raise("Failed to connect to Beanstalkd (tcp://#{host}:#{port}): #{:inet.format_error(reason)}")
end

defmodule Beanstix.TestHelpers do
  use ExUnit.Case

  def setup_connection(context) do
    host = ~c"127.0.0.1"
    port = 11300

    if context[:no_setup] do
      {:ok, %{}}
    else
      {m, s, ms} = :os.timestamp()
      tube = "Beanstix_#{m}_#{s}_#{ms}"
      {:ok, pid} = Beanstix.connect(host, port)
      Beanstix.command(pid, {:use, tube})
      Beanstix.command(pid, {:watch, tube})
      Beanstix.command(pid, {:ignore, "default"})
      {:ok, %{pid: pid, tube: tube}}
    end
  end
end
