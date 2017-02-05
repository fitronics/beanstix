defmodule ReserveBench do
  use Benchfella

  @host '127.0.0.1'
  @port 11300
  @data "{\"action\": \"create\", \"type\": \"user\", \"data\": {\"id\": 1, \"name\": \"Joe\"}}"

  setup_all do
    {:ok, elixir_talk_pid} = ElixirTalk.connect(@host, @port)
    {:ok, beanstix_pid} = Beanstix.connect(host: @host, port: @port)
    data = for _ <- 1..10000, do: {:put, @data}
    Beanstix.pipeline(beanstix_pid, data)
    {:ok, %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid}}
  end

  teardown_all %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid} do
    ElixirTalk.quit(elixir_talk_pid)
    Beanstix.disconnect(beanstix_pid)
  end

  bench "ElixirTalk", [context: bench_context] do
    {:reserved, _job_id, _data} = ElixirTalk.reserve(context.elixir_talk_pid)
    :ok
  end

  bench "Beanstix", [context: bench_context] do
    {:ok, {_job_id, _data}} = Beanstix.command(context.beanstix_pid, :reserve)
    :ok
  end

end
