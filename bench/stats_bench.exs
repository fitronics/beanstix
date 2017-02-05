defmodule StatsBench do
  use Benchfella

  @host '127.0.0.1'
  @port 11300

  setup_all do
    {:ok, elixir_talk_pid} = ElixirTalk.connect(@host, @port)
    {:ok, beanstix_pid} = Beanstix.connect(host: @host, port: @port)
    {:ok, %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid}}
  end

  teardown_all %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid} do
    ElixirTalk.quit(elixir_talk_pid)
    Beanstix.disconnect(beanstix_pid)
  end

  bench "Beanstix", [context: bench_context]do
    {:ok, _stats} = Beanstix.command(context.beanstix_pid, :stats)
    :ok
  end

end
