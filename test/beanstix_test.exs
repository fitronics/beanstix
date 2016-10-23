defmodule BeanstixTest do
  use ExUnit.Case
  doctest Beanstix

  @host '127.0.0.1'
  @port 11300

  setup context do
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
      {:ok, %{pid: pid}}
    end
  end

  test "command/2", %{pid: pid} do
    assert {:error, :unknown_command} = Beanstix.command(pid, :peak_ready)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
  end

  test "put", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, ^job_id, ^data} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

end
