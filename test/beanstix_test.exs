defmodule BeanstixTest do
  use ExUnit.Case
  doctest Beanstix

  @moduletag :simple
  @data "simple"

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "put", %{pid: pid} do
    assert {:ok, job_id} = Beanstix.put(pid, @data)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(pid)
    assert {:ok, :deleted} = Beanstix.delete(pid, job_id)
  end

  test "put!", %{pid: pid} do
    job_id = Beanstix.put!(pid, @data)
    assert {^job_id, @data} = Beanstix.reserve!(pid)
    assert :deleted = Beanstix.delete!(pid, job_id)
  end

  test "put_in_tube", %{pid: pid, tube: tube} do
    assert {:ok, job_id} = Beanstix.put_in_tube(pid, tube, @data)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(pid)
    assert {:ok, :deleted} = Beanstix.delete(pid, job_id)
  end

  test "put_in_tube!", %{pid: pid, tube: tube} do
    job_id = Beanstix.put_in_tube!(pid, tube, @data)
    assert {^job_id, @data} = Beanstix.reserve!(pid)
    assert :deleted = Beanstix.delete!(pid, job_id)
  end

  test "release", %{pid: pid} do
    assert {:ok, job_id} = Beanstix.put(pid, @data)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(pid)
    assert {:ok, :timed_out} = Beanstix.reserve(pid, 0)
    assert {:ok, :released} = Beanstix.release(pid, job_id)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(pid)
    assert :deleted = Beanstix.delete!(pid, job_id)
  end
end
