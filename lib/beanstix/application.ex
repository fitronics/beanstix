defmodule Beanstix.Application do
  @moduledoc """
  Beanstix application
  """

  @default_ip '127.0.0.1'
  @default_port 11_300
  @default_reconnect true
  @default_pool_size 1
  @default_pool_strategy :round_robin
  @default_backlog_size 1
  @default_socket_options [
    :binary,
    {:buffer, 65_535},
    {:nodelay, true},
    {:packet, :raw},
    {:send_timeout, 500},
    {:send_timeout_close, true}
  ]

  def start(_type, opts) do
    pool_name = opts[:pool_name] || Beanstix.pool_name()
    :shackle_pool.start(pool_name, Beanstix.Client, client_options(opts), pool_options(opts))
    {:ok, self()}
  end

  def stop(pool_name) do
    :shackle_pool.stop(pool_name)
  end

  def client_options(opts) do
    [
      ip: opts[:ip] || @default_ip,
      port: opts[:port] || @default_port,
      reconnect: @default_reconnect,
      socket_options: @default_socket_options,
    ]
  end

  def pool_options(opts) do
    [
      pool_size: opts[:pool_size] || @default_pool_size,
      pool_strategy: opts[:pool_strategy] || @default_pool_strategy,
      backlog_size: opts[:backlog_size] || @default_backlog_size,
    ]
  end
end
