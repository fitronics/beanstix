defmodule BeanstixCommandTest do
  use ExUnit.Case

  @moduletag :command

  setup context do
    Beanstix.TestHelpers.setup_connection(context)
  end

  test "command/2", %{pid: pid} do
    assert {:error, %Beanstix.Error{message: "UNKNOWN_COMMAND"}} = Beanstix.command(pid, :peak_ready)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
  end

  test "put", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "put priority", %{pid: pid} do
    data1 = "1"
    data2 = "2"
    assert {:ok, job_id1} = Beanstix.command(pid, {:put, data1, priority: 9999})
    assert {:ok, job_id2} = Beanstix.command(pid, {:put, data2, priority: 999})
    assert {:ok, {^job_id2, ^data2}} = Beanstix.command(pid, :reserve)
    assert {:ok, {^job_id1, ^data1}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id1})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id2})
  end

  test "put timeout", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data, timeout: 1})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
    :timer.sleep(:timer.seconds(1))
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :peek_ready)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  @tag :basic
  test "use", %{pid: pid} do
    tube = "Beanstix_test_tube"
    assert {:ok, ^tube} = Beanstix.command(pid, {:use, tube})
  end

  test "reserve-with-timeout", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, {:reserve_with_timeout, 1})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "release", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
    assert {:ok, :released} = Beanstix.command(pid, {:release, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "bury and kick", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :buried} = Beanstix.command(pid, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pid, {:bury, job_id})
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_ready)
    assert {:ok, 1} = Beanstix.command(pid, :kick)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "touch", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, {:reserve})
    assert {:ok, :touched} = Beanstix.command(pid, {:touch, job_id})
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
    assert {:ok, :not_found} = Beanstix.command(pid, {:touch, job_id})
  end

  test "watch and ignore", %{pid: pid, tube: tube} do
    new_tube = tube <> "_new"
    assert {:ok, 1} = Beanstix.command(pid, {:watch, tube})
    assert {:ok, 2} = Beanstix.command(pid, {:watch, new_tube})
    assert {:ok, 1} = Beanstix.command(pid, {:ignore, new_tube})
    assert {:ok, :not_ignored} = Beanstix.command(pid, {:ignore, tube})
  end

  test "peak, peek-ready, peek-delayed and peek-buried", %{pid: pid} do
    data = "1"
    assert {:ok, job_id} = Beanstix.command(pid, {:put, data})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, {:peek, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :peek_ready)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_delayed)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :released} = Beanstix.command(pid, {:release, job_id, delay: 1})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :peek_delayed)
    :timer.sleep(:timer.seconds(1))
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_delayed)
    assert {:ok, :not_found} = Beanstix.command(pid, :peek_buried)
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :reserve)
    assert {:ok, :buried} = Beanstix.command(pid, {:bury, job_id})
    assert {:ok, {^job_id, ^data}} = Beanstix.command(pid, :peek_buried)
    assert {:ok, :deleted} = Beanstix.command(pid, {:delete, job_id})
  end

  test "list-tubes", %{pid: pid} do
    assert {:ok, tubes} = Beanstix.command(pid, :list_tubes)
    assert "default" in tubes
  end

  test "list-tube-used", %{pid: pid, tube: tube} do
    assert {:ok, ^tube} = Beanstix.command(pid, :list_tube_used)
  end

  test "list-tubes-watched", %{pid: pid, tube: tube} do
    assert {:ok, tubes} = Beanstix.command(pid, :list_tubes_watched)
    assert tube in tubes
  end

  test "pause-tube", %{pid: pid, tube: tube} do
    assert {:ok, :paused} = Beanstix.command(pid, {:pause_tube, tube})
    assert {:ok, :paused} = Beanstix.command(pid, {:pause_tube, tube, delay: 1})
  end

end
