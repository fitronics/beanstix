defmodule BeanstixTest do
  use ExUnit.Case
  doctest Beanstix

  @moduletag :simple
  @data "simple"

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "put", %{pool_name: pool_name} do
    opts = [pool_name: pool_name]
    assert {:ok, job_id} = Beanstix.put(@data, opts)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(opts)
    assert {:ok, :deleted} = Beanstix.delete(job_id, opts)
  end

  test "put!", %{pool_name: pool_name} do
    opts = [pool_name: pool_name]
    job_id = Beanstix.put!(@data, opts)
    assert {^job_id, @data} = Beanstix.reserve!(opts)
    assert :deleted = Beanstix.delete!(job_id, opts)
  end

  test "put_in_tube", %{pool_name: pool_name} do
    opts = [pool_name: pool_name]
    tube = Atom.to_string(pool_name)
    assert {:ok, job_id} = Beanstix.put_in_tube(tube, @data, opts)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(opts)
    assert {:ok, :deleted} = Beanstix.delete(job_id, opts)
  end

  test "put_in_tube!", %{pool_name: pool_name} do
    opts = [pool_name: pool_name]
    tube = Atom.to_string(pool_name)
    job_id = Beanstix.put_in_tube!(tube, @data, opts)
    assert {^job_id, @data} = Beanstix.reserve!(opts)
    assert :deleted = Beanstix.delete!(job_id, opts)
  end

  test "release", %{pool_name: pool_name} do
    opts = [pool_name: pool_name]
    assert {:ok, job_id} = Beanstix.put(@data, opts)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(opts)
    assert {:ok, :timed_out} = Beanstix.reserve(opts ++ [timeout: 0])
    assert {:ok, :released} = Beanstix.release(job_id, opts)
    assert {:ok, {^job_id, @data}} = Beanstix.reserve(opts)
    assert :deleted = Beanstix.delete!(job_id, opts)
  end

end
