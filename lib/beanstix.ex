defmodule Beanstix do
  alias Beanstix.Connection

  @moduledoc """
  Beanstix - A beanstalkd client coding with Elixir

  Forked from ElixirTalk
  Copyright 2014-2016 by jsvisa(delweng@gmail.com)
  """
  @type job_id :: non_neg_integer
  @type connection_error :: :timeout | :closed | :inet.posix()
  @type put_result :: {:ok, job_id} | {:error, Beanstix.Error.t() | connection_error | binary}
  @type put_options :: [{:priority, integer}, {:delay, integer}, {:ttr, integer}]

  @vsn 1.0

  @doc """
  Connect to the beanstalkd server.
  """

  @spec connect(list) :: {:ok, pid} | {:error, term}
  def connect(opts) when is_list(opts) do
    Connection.start_link(opts)
  end

  @spec connect(:inet.ip_address() | :inet.hostname(), integer, timeout) ::
          {:ok, pid} | {:error, term}
  def connect(host \\ ~c"127.0.0.1", port \\ 11300, timeout \\ :infinity) do
    connect(host: host, port: port, recv_timeout: timeout, connect_timeout: 5_000)
  end

  @doc """
  Close the connection to server.
  """
  @spec quit(pid) :: :ok
  def quit(pid) do
    Connection.quit(pid)
  end

  def pipeline(pid, commands, timeout \\ 5000)
  def pipeline(pid, commands, timeout) when length(commands) > 0, do: Connection.call(pid, commands, timeout)
  def pipeline(_, _, _), do: []

  @spec command(pid, atom | tuple, integer | :infinity) :: {:ok, term} | {:error, term}
  def command(pid, command, timeout \\ 5000) do
    case pipeline(pid, [command], timeout) do
      result when is_list(result) -> hd(result)
      error -> error
    end
  end

  @doc """
  Put a job to the current tube.

  The opts can be any combination of

    * `:priority` - an integer < 2**32. Jobs with smaller priority values will be
      scheduled before jobs with larger priorities. The most urgent priority is 0;
      the least urgent priority is 4,294,967,295.

    * `:delay` - an integer number of seconds to wait before putting the job in
      the ready queue. The job will be in the "delayed" state during this time.

    * `:ttr` - time to run -- is an integer number of seconds to allow a worker
      to run this job. This time is counted from the moment a worker reserves
      this job. If the worker does not delete, release, or bury the job within
      `:ttr` seconds, the job will time out and the server will release the job.
      The minimum ttr is 1. If the client sends 0, the server will silently
      increase the ttr to 1.
  """
  @spec put(pid, String.t()) :: put_result
  @spec put(pid, String.t(), put_options) :: put_result
  def put(pid, data, opts \\ []) do
    command(pid, {:put, data, opts})
  end

  @spec put!(pid, String.t()) :: job_id
  @spec put!(pid, String.t(), put_options) :: job_id
  def put!(pid, data, opts \\ []) do
    case put(pid, data, opts) do
      {:ok, job_id} -> job_id
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Put a job in the specified tube.
  The opts are the same as `put`
  """
  @spec put_in_tube(pid, String.t(), String.t()) :: put_result
  @spec put_in_tube(pid, String.t(), String.t(), put_options) :: put_result
  def put_in_tube(pid, tube, data, opts \\ []) do
    case pipeline(pid, [{:use, tube}, {:put, data, opts}]) do
      [{:ok, ^tube}, result] -> result
      error -> {:error, "#{inspect(error)}"}
    end
  end

  @spec put_in_tube!(pid, String.t(), String.t()) :: job_id
  @spec put_in_tube!(pid, String.t(), String.t(), put_options) :: job_id
  def put_in_tube!(pid, tube, data, opts \\ []) do
    case put_in_tube(pid, tube, data, opts) do
      {:ok, job_id} -> job_id
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Use a tube to `put` jobs.
  """
  @spec use(pid, String.t()) :: {:ok, String.t()} | {:error, connection_error}
  def use(pid, tube) do
    command(pid, {:use, tube})
  end

  @doc """
  Add the named tube to the watch list for the current connection.
  A reserve command will take a job from any of the tubes in the
  watch list.
  """
  @spec watch(pid, String.t()) :: {:ok, non_neg_integer} | {:error, connection_error}
  def watch(pid, tube) do
    command(pid, {:watch, tube})
  end

  @doc """
  Remove the named tube from the watch list for the current connection.
  """
  @spec ignore(pid, String.t()) :: {:ok, non_neg_integer | :not_ignored} | {:error, connection_error}
  def ignore(pid, tube) do
    command(pid, {:ignore, tube})
  end

  @doc """
  Remove a job from the server entirely. It is normally used
  by the client when the job has successfully run to completion. A client can
  delete jobs that it has reserved, ready jobs, delayed jobs, and jobs that are
  buried.
  """

  @spec delete(pid, job_id) :: {:ok, :deleted | :not_found} | {:error, connection_error}
  def delete(pid, id) do
    command(pid, {:delete, id})
  end

  @spec delete!(pid, job_id) :: :deleted
  def delete!(pid, id) do
    case delete(pid, id) do
      {:ok, :deleted} -> :deleted
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Allow a worker to request more time to work on a job.
  This is useful for jobs that potentially take a long time, but you still want
  the benefits of a TTR pulling a job away from an unresponsive worker.  A worker
  may periodically tell the server that it's still alive and processing a job
  (e.g. it may do this on DEADLINE_SOON). The command postpones the auto
  release of a reserved job until TTR seconds from when the command is issued.
  """
  @spec touch(pid, job_id) :: {:ok, :touched | :not_found} | {:error, connection_error}
  def touch(pid, id) do
    command(pid, {:touch, id})
  end

  @doc """
  Let the client inspect a job in the system. Peeking the given job id
  """

  @spec peek(pid, job_id) :: {:ok, job_id | :not_found} | {:error, connection_error}
  def peek(pid, id) do
    command(pid, {:peek, id})
  end

  @doc """
  Peeking the next ready job.
  """

  @spec peek_ready(pid) :: {:ok, job_id | :not_found} | {:error, connection_error}
  def peek_ready(pid) do
    command(pid, :peek_ready)
  end

  @doc """
  Peeking the delayed job with the shortest delay left.
  """

  @spec peek_delayed(pid) :: {:ok, job_id | :not_found} | {:error, connection_error}
  def peek_delayed(pid) do
    command(pid, :peek_delayed)
  end

  @doc """
  Peeking the next job in the list of buried jobs.
  """

  @spec peek_buried(pid) :: {:ok, job_id | :not_found} | {:error, connection_error}
  def peek_buried(pid) do
    command(pid, :peek_buried)
  end

  @doc """
  Move jobs into the ready queue. If there are any buried jobs, it will only kick buried jobs.
  Otherwise it will kick delayed jobs.

  Apply only to the currently used tube.
  """

  @spec kick(pid, non_neg_integer) :: {:ok, non_neg_integer} | {:error, connection_error}
  def kick(pid, bound \\ 1) do
    command(pid, {:kick, [bound: bound]})
  end

  @doc """
  Similar to `kick(bound)`, if the given job id exists and is in a buried or
  delayed state, it will be moved to the ready queue of the the same tube where it
  currently belongs.
  """

  @spec kick_job(pid, job_id) :: {:ok, :not_found | :kicked} | {:error, connection_error}
  def kick_job(pid, id) do
    command(pid, {:kick_job, id})
  end

  @doc """
  Give statistical information about the system as a whole.
  """

  @spec stats(pid) :: {:ok, Map.t()} | {:error, connection_error}
  def stats(pid) do
    command(pid, :stats)
  end

  @doc """
  Similar to `stats/0`, gives statistical information about the specified job if
  it exists.
  """

  @spec stats_job(pid, non_neg_integer) :: {:ok, Map.t() | :not_found} | {:error, connection_error}
  def stats_job(pid, id) do
    command(pid, {:stats_job, id})
  end

  @doc """
  Similar to `stats/0`, gives statistical information about the specified tube
  if it exists.
  """

  @spec stats_tube(pid, String.t()) :: {:ok, Map.t() | :not_found} | {:error, connection_error}
  def stats_tube(pid, tube) do
    command(pid, {:stats_tube, tube})
  end

  @spec stats_tube!(pid, String.t()) :: {:ok, Map.t() | :not_found} | {:error, connection_error}
  def stats_tube!(pid, tube) do
    case stats_tube(pid, tube) do
      {:ok, stats} -> stats
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Return a list of all existing tubes in the server.
  """

  @spec list_tubes(pid) :: {:ok, list} | {:error, connection_error}
  def list_tubes(pid) do
    command(pid, :list_tubes)
  end

  @spec list_tubes!(pid) :: list
  def list_tubes!(pid) do
    case list_tubes(pid) do
      {:ok, tubes} -> tubes
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Return the tube currently being used by the client.
  """

  @spec list_tube_used(pid) :: {:ok, String.t()} | {:error, connection_error}
  def list_tube_used(pid) do
    command(pid, :list_tube_used)
  end

  @doc """
  Return the tubes currently being watched by the client.
  """

  @spec list_tubes_watched(pid) :: {:ok, list} | {:error, connection_error}
  def list_tubes_watched(pid) do
    command(pid, :list_tubes_watched)
  end

  @doc """
  Get a job from the currently watched tubes.
  """

  @spec reserve(pid) :: {:ok, {job_id, String.t()}} | {:error, connection_error}
  def reserve(pid) do
    command(pid, :reserve, :infinity)
  end

  @spec reserve!(pid) :: {job_id, String.t()}
  def reserve!(pid) do
    case reserve(pid) do
      {:ok, {job_id, data}} -> {job_id, data}
      {:error, message} -> raise Beanstix.Error, message: message
    end
  end

  @doc """
  Get a job from the currently watched tubes with timeout of seconds.
  """

  @spec reserve(pid, non_neg_integer) ::
          {:ok, {job_id, String.t()} | :deadline_soon | :timed_out} | {:error, connection_error}
  def reserve(pid, timeout) do
    command(pid, {:reserve_with_timeout, timeout}, :infinity)
  end

  @doc """
  Put a job into the "buried" state. Buried jobs are put into a
  FIFO linked list and will not be touched by the server again until a client
  kicks them with the `kick` command.
  """

  @spec bury(pid, non_neg_integer) :: {:ok, :buried | :not_found} | {:error, connection_error}
  @spec bury(pid, non_neg_integer, [{:priority, integer}]) :: {:ok, :buried | :not_found} | {:error, connection_error}
  def bury(pid, id, opts \\ []) do
    command(pid, {:bury, id, opts})
  end

  @doc """
  Delay any new job being reserved for a given time.
  """

  @spec pause_tube(pid, String.t(), [{:delay, integer}]) :: {:ok, :paused | :not_found} | {:error, connection_error}
  def pause_tube(pid, tube, opts \\ []) do
    command(pid, {:pause_tube, tube, opts})
  end

  @doc """
  Put a reserved job back into the ready queue (and marks its state as "ready")
  to be run by any client. It is normally used when the job fails because of a transitory error.

  The opts can any combination of

  * `:priority` - a new priority to assign to the job;

  * `:delay` - an integer number of seconds to wait before putting the job back in the ready queue.
    The job will be in the "delayed" state during this time.
  """

  @spec release(pid, non_neg_integer) :: {:ok, :released | :buried | :not_found} | {:error, connection_error}
  @spec release(pid, non_neg_integer, [{:priority, integer}, {:delay, integer}]) ::
          {:ok, :released | :buried | :not_found} | {:error, connection_error}
  def release(pid, id, opts \\ []) do
    command(pid, {:release, id, opts})
  end

  @doc """
  Delete all jobs in a given tube
  """
  def purge_tube(pid, tube) do
    {:ok, ^tube} = command(pid, {:use, tube})
    delete_jobs(pid, :peek_ready)
    delete_jobs(pid, :peek_delayed)
    delete_jobs(pid, :peek_buried)
  end

  defp delete_jobs(pid, peek_cmd) do
    case Beanstix.command(pid, peek_cmd) do
      {:ok, {job_id, _}} ->
        Beanstix.delete(pid, job_id)
        delete_jobs(pid, peek_cmd)

      _ ->
        :ok
    end
  end
end
