ExUnit.configure(exclude: [broken: true])
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

  def setup_connection(context) do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {m, s, ms} = :os.timestamp
      tube = "Beanstix_#{m}_#{s}_#{ms}"
      pool_name = String.to_atom(tube)
      Beanstix.Application.start(nil, [pool_name: pool_name])
      Beanstix.command(pool_name, {:use, tube})
      Beanstix.command(pool_name, {:watch, tube})
      Beanstix.command(pool_name, {:ignore, "default"})
      on_exit(fn -> Beanstix.Application.stop(pool_name) end)
      {:ok, %{pool_name: pool_name, tube: tube}}
    end
  end

end
