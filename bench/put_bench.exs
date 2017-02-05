defmodule PutBench do
  use Benchfella

  @host '127.0.0.1'
  @port 11300
  @data "{\"action\": \"create\", \"type\": \"user\", \"data\": {\"id\": 1, \"name\": \"Joe\"}}"

  setup_all do
    {:ok, elixir_talk_pid} = ElixirTalk.connect(@host, @port)
    {:ok, beanstix_pid} = Beanstix.connect(host: @host, port: @port)
    {:ok, %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid}}
  end

  teardown_all %{elixir_talk_pid: elixir_talk_pid, beanstix_pid: beanstix_pid} do
    ElixirTalk.quit(elixir_talk_pid)
    Beanstix.disconnect(beanstix_pid)
  end

  bench "ElixirTalk", [context: bench_context] do
    {:inserted, _job_id} = ElixirTalk.put(context.elixir_talk_pid, @data)
    :ok
  end

  bench "Beanstix", [context: bench_context] do
    {:ok, _job_id} = Beanstix.command(context.beanstix_pid, {:put, @data})
    :ok
  end

  bench "ElixirTalk Async", [context: bench_context, data: gen_data(64)] do
    Task.async_stream(data, fn(d) ->
      ElixirTalk.put(context.elixir_talk_pid, d)
    end, max_concurrency: 32)
    |> Enum.to_list()
    :ok
  end

  bench "Beanstix Async", [context: bench_context, data: gen_data(64)] do
    Task.async_stream(data, fn(d) ->
      Beanstix.command(context.beanstix_pid, {:put, d})
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
