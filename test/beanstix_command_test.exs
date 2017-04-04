defmodule BeanstixCommandTest do
  use ExUnit.Case

  @moduletag :pipeline

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "command/2", %{pool_name: pool_name} do
    assert {:error, %Beanstix.Error{message: "UNKNOWN_COMMAND"}} = Beanstix.command(pool_name, :peak_ready)
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_ready)
  end

  test "put", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "put priority", %{pool_name: pool_name} do
    data1 = "1"
    data2 = "2"
    assert {:ok, job_id1} = Beanstix.command(pool_name, {:put, data1, priority: 9999})
    assert {:ok, job_id2} = Beanstix.command(pool_name, {:put, data2, priority: 999})
    assert {:ok, {^job_id2, ^data2}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, {^job_id1, ^data1}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id1})
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id2})
  end

  test "put timeout", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data, timeout: 1})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_ready)
    :timer.sleep(:timer.seconds(1))
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :peek_ready)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  @tag :basic
  test "use", %{pool_name: pool_name} do
    tube = "Beanstix_test_tube"
    assert {:ok, ^tube} = Beanstix.command(pool_name, {:use, tube})
  end

  test "reserve-with-timeout", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, {:reserve_with_timeout, 1})
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "release", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_ready)
    assert {:ok, :released} = Beanstix.command(pool_name, {:release, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "bury and kick", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :buried} = Beanstix.command(pool_name, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pool_name, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_ready)
    assert {:ok, 1} = Beanstix.command(pool_name, :kick)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "touch", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, {:reserve})
    assert {:ok, :touched} = Beanstix.command(pool_name, {:touch, job_id})
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
    assert {:ok, :not_found} = Beanstix.command(pool_name, {:touch, job_id})
  end

  test "watch and ignore", %{pool_name: pool_name, tube: tube} do
    new_tube = tube <> "_new"
    assert {:ok, 1} = Beanstix.command(pool_name, {:watch, tube})
    assert {:ok, 2} = Beanstix.command(pool_name, {:watch, new_tube})
    assert {:ok, 1} = Beanstix.command(pool_name, {:ignore, new_tube})
    assert {:ok, :not_ignored} = Beanstix.command(pool_name, {:ignore, tube})
  end

  test "peak, peek-ready, peek-delayed and peek-buried", %{pool_name: pool_name} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pool_name, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, {:peek, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :peek_ready)
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_delayed)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :released} = Beanstix.command(pool_name, {:release, job_id, delay: 1})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :peek_delayed)
    :timer.sleep(:timer.seconds(1))
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_delayed)
    assert {:ok, :not_found} = Beanstix.command(pool_name, :peek_buried)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :reserve)
    assert {:ok, :buried} = Beanstix.command(pool_name, {:bury, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pool_name, :peek_buried)
    assert {:ok, :deleted} = Beanstix.command(pool_name, {:delete, job_id})
  end

  test "list-tubes", %{pool_name: pool_name} do
    assert {:ok, tubes} = Beanstix.command(pool_name, :list_tubes)
    assert "default" in tubes
  end

  test "list-tube-used", %{pool_name: pool_name, tube: tube} do
    assert {:ok, ^tube} = Beanstix.command(pool_name, :list_tube_used)
  end

  test "list-tubes-watched", %{pool_name: pool_name, tube: tube} do
    assert {:ok, tubes} = Beanstix.command(pool_name, :list_tubes_watched)
    assert tube in tubes
  end

  test "pause-tube", %{pool_name: pool_name, tube: tube} do
    assert {:ok, :paused} = Beanstix.command(pool_name, {:pause_tube, tube})
    assert {:ok, :paused} = Beanstix.command(pool_name, {:pause_tube, tube, delay: 1})
  end

end
