defmodule PutBench do
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
    {:ok, %{elixir_talk_pid: elixir_talk_pid, pool_name: pool_name}}
  end

  teardown_all %{elixir_talk_pid: elixir_talk_pid, pool_name: pool_name} do
    ElixirTalk.quit(elixir_talk_pid)
    Beanstix.Application.stop(pool_name)
  end

  bench "ElixirTalk", [context: bench_context] do
    {:inserted, _job_id} = ElixirTalk.put(context.elixir_talk_pid, @data)
    :ok
  end

  bench "Beanstix", [context: bench_context] do
    {:ok, _job_id} = Beanstix.command(context.pool_name, {:put, @data})
    :ok
  end

  bench "ElixirTalk Async", [context: bench_context, data: gen_data(64)] do
    Task.async_stream(data, fn(d) ->
      {:inserted, _} = ElixirTalk.put(context.elixir_talk_pid, d)
    end, max_concurrency: 32)
    |> Enum.to_list()
    :ok
  end

  bench "Beanstix Async", [context: bench_context, data: gen_data(64)] do
    Task.async_stream(data, fn(d) ->
      {:ok, _} = Beanstix.command(context.pool_name, {:put, d})
    end, max_concurrency: 32)
    |> Enum.to_list()
    :ok
  end

  defp gen_data(num) do
    for _ <- 1..num do
      @data
    end
  end

end
