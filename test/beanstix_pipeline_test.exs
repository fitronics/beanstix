defmodule BeanstixPipelineTest do
  use ExUnit.Case
  doctest Beanstix

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "pipeline/2", %{pid: pid} do
    commands = [{:put, 1}, {:put, 2}]
    result = Beanstix.pipeline(pid, commands)
    assert {:ok, [job_id1, job_id2]} = result
    assert is_integer(job_id1) and is_integer(job_id2)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id1})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id2})
  end

  test "pipeline put", %{pid: pid} do
    data1 = "3"
    data2 = "4"
    commands = [{:put, data1}, {:put, data2}]
    assert {:ok, [job_id1, job_id2]} = Beanstix.pipeline(pid, commands)
    assert {:ok, {^job_id1, ^data1}} = Beanstix.command(pid, :reserve)
    assert {:ok, {^job_id2, ^data2}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id1})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id2})
  end

end
