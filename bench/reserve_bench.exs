defmodule ReserveBench do
  use Benchfella

  @host '127.0.0.1'
  @port 11300
  @data "{\"action\": \"create\", \"type\": \"user\", \"data\": {\"id\": 1, \"name\": \"Joe\"}}"

  setup_all do
    {:ok, elixir_talk_pid} = ElixirTalk.connect(@host, @port)
    {m, s, ms} = :os.timestamp
    pool_name = String.to_atom("Beanstix_#{m}_#{s}_#{ms}")
    Application.ensure_all_started(:shackle)
    Beanstix.Application.start(nil, [pool_name: pool_name])
    data = for _ <- 1..10000, do: {:put, @data}
    Beanstix.pipeline(pool_name, data)
    {:ok, %{elixir_talk_pid: elixir_talk_pid, pool_name: pool_name}}
  end

  teardown_all %{elixir_talk_pid: elixir_talk_pid, pool_name: pool_name} do
    ElixirTalk.quit(elixir_talk_pid)
    Beanstix.Application.stop(pool_name)
  end

  bench "ElixirTalk", [context: bench_context] do
    {:reserved, _job_id, _data} = ElixirTalk.reserve(context.elixir_talk_pid)
    :ok
  end

  bench "Beanstix", [context: bench_context] do
    {:ok, {_job_id, _data}} = Beanstix.command(context.pool_name, :reserve)
    :ok
  end

end
