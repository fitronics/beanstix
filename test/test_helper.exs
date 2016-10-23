ExUnit.start()

host = '127.0.0.1'
port = 11300

case :gen_tcp.connect(host, port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Failed to connect to Beanstalkd (tcp://#{host}:#{port}): #{:inet.format_error(reason)}"
end

# defmodule Beanstix.TestHelpers do
#
# end
