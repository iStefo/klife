defmodule Klife.Consumer.ShareGroup do
  import Klife.ProcessRegistry, only: [via_tuple: 1]

  require Logger

  alias Klife.Connection.Broker
  alias Klife.Connection.Controller, as: ConnController
  alias Klife.Consumer.ShareGroup.Session
  alias Klife.Consumer.ShareGroup.Worker
  alias Klife.Helpers
  alias Klife.MetadataCache

  alias KlifeProtocol.Messages, as: M

  @share_group_opts [
    client: [
      type: :atom,
      required: true,
      doc:
        "The name of the klife client to be used by the share group. May be set on " <>
          "`use` (binding the module at compile time) or passed to `start_link/1` " <>
          "(binding it on first start). Works exactly like in `Klife.Consumer.ConsumerGroup`."
    ],
    group_name: [
      type: :string,
      required: true,
      doc: "Name (id) of the share group"
    ],
    topics: [
      type: {:list, {:keyword_list, [name: [type: :string, required: true, doc: "Topic name"]]}},
      required: true,
      doc:
        "List of subscribed topics. Each entry is a keyword list (`[name: \"my_topic\"]`) so " <>
          "per-topic options can be added in the future."
    ],
    mode: [
      type: {:in, [nil, :auto, :manual]},
      default: nil,
      doc:
        "How records are consumed. In `:auto` mode worker processes call the " <>
          "`c:handle_record_batch/2` callback and acknowledge according to its return value. " <>
          "In `:manual` mode records are fetched with `pull/2` and acknowledged with " <>
          "`acknowledge/2`. When not set it defaults to `:auto` if the module implements " <>
          "`c:handle_record_batch/2` and to `:manual` otherwise. Setting `:auto` explicitly on " <>
          "a module without the callback raises."
    ],
    handler_concurrency: [
      type: :pos_integer,
      default: 1,
      doc: "`:auto` mode only. Number of worker processes calling the callback concurrently."
    ],
    handler_max_batch_size: [
      type: :pos_integer,
      default: 100,
      doc:
        "`:auto` mode only. Maximum number of records handed to a single " <>
          "`c:handle_record_batch/2` call."
    ],
    handler_idle_ms: [
      type: :non_neg_integer,
      default: 100,
      doc: "`:auto` mode only. Time a worker waits after a pull that returned no records."
    ],
    share_auto_offset_reset: [
      type: {:in, [nil, :earliest, :latest]},
      default: nil,
      doc:
        "When set, the group config `share.auto.offset.reset` is set to this value before " <>
          "joining (it defines where a share group starts consuming when it has no state yet). " <>
          "`nil` leaves the broker/group configuration untouched, which defaults to `latest`."
    ],
    max_wait_ms: [
      type: :non_neg_integer,
      default: 500,
      doc: "`max_wait_ms` of the ShareFetch requests"
    ],
    min_bytes: [
      type: :non_neg_integer,
      default: 1,
      doc: "`min_bytes` of the ShareFetch requests"
    ],
    max_bytes: [
      type: :pos_integer,
      default: 52_428_800,
      doc: "`max_bytes` of the ShareFetch requests"
    ],
    max_records: [
      type: :pos_integer,
      default: 500,
      doc: "`max_records` and `batch_size` of the ShareFetch requests"
    ],
    max_buffered_records: [
      type: :pos_integer,
      default: 2_000,
      doc:
        "Per broker session. While the number of acquired but not yet pulled records is " <>
          "greater or equal to this value no more records are fetched (only acknowledgements are sent)."
    ],
    request_timeout_ms: [
      type: :pos_integer,
      doc:
        "Timeout of share requests (not including the fetch max wait). Defaults to the client's."
    ]
  ]

  @moduledoc """
  Defines a share group consumer (Kafka share groups, KIP-932).

  Unlike a consumer group, a share group does not give partitions exclusive to a
  single member. Every member may receive records of every partition: the broker
  hands out records by *acquiring* them (with a lock) for a member, and the member
  acknowledges each record individually. Partition assignment is only a hint used
  to spread fetch load between members, record locks are what guarantee that a
  record is held by one member at a time.

  Share groups require Kafka 4.1 or newer (and the `share.version` feature
  enabled on the cluster).

  This first version only supports the `:manual` mode: your code pulls records
  with `pull/2` and acknowledges them with `acknowledge/2` or `acknowledge/3`.

  ## Defining a share group

  ```elixir
  defmodule MyApp.MyShareGroup do
    use Klife.Consumer.ShareGroup,
      client: MyApp.MyClient,
      group_name: "my_share_group",
      topics: [[name: "my_topic"]],
      share_auto_offset_reset: :earliest
  end

  # supervision tree
  children = [{MyApp.MyShareGroup, []}]

  # later
  {:ok, records} = MyApp.MyShareGroup.pull("my_share_group", max_records: 100)
  :ok = MyApp.MyShareGroup.acknowledge("my_share_group", records, :accept)
  ```

  Options given to `use` are merged with the ones given to `start_link/1`, which
  take precedence, and `:client` binding works exactly as described in "Consumer
  group module bindings" of `Klife.Consumer.ConsumerGroup`.

  ## Configuration options

  #{NimbleOptions.docs(@share_group_opts)}

  ## Delivery semantics

  Share groups provide at-least-once delivery with no ordering guarantee, neither
  inside a partition nor across partitions. Every record pulled must be
  acknowledged with one of:

  - `:accept`: the record was processed, it is never delivered again.
  - `:release`: the record could not be processed now, it is made available again
    for any member and its `delivery_count` is incremented on the next delivery.
  - `:reject`: the record is unprocessable, it is archived and never delivered again.

  After `share.delivery.count.limit` (broker config, 5 by default) deliveries a
  record is archived by the broker even if it was never accepted.

  Records are `Klife.Record` structs with the `:delivery_count` field set (starting
  at 1); `:consumer_attempts` is `delivery_count - 1`.

  Acknowledgements are asynchronous (`acknowledge/3` returns `:ok` as soon as they are
  queued) and are sent piggybacked on the next fetch or on their own. An acknowledgement
  the broker rejects (for example because the lock already expired) is logged and the
  record will be redelivered.

  ## Lock timeout

  Acquired records are locked for `share.record.lock.duration.ms` (30 seconds by default).
  Records not acknowledged in this window are released by the broker and delivered
  again with a higher delivery count, possibly to another member. Records that sit
  in the member's buffer count against their lock as well, keep `:max_buffered_records`
  small enough for what you can process within the lock duration.

  ## Ownership

  The process that calls `pull/2` (or the one given in the `:owner` option) is the
  owner of the returned records and is monitored. If the owner dies, all its
  records that were not acknowledged are released immediately instead of waiting
  for the lock to expire. Gracefully stopping the share group releases every record
  that was pulled and not acknowledged too.

  ## Architecture

  The share group process takes care of membership (`ShareGroupHeartbeat`) and
  starts one `Klife.Consumer.ShareGroup.Session` process for each broker leading
  at least one assigned partition. Sessions own the share session with their broker,
  buffer the acquired records and send the acknowledgements. `pull/2` and
  `acknowledge/3` talk directly to the sessions and do not go through the group
  process.
  """

  @type ack_type :: :accept | :release | :reject

  @doc """
  Handles a batch of records in `:auto` mode.

  Called by the worker processes (see the `:handler_concurrency` option) with the
  records they pulled, at most `:handler_max_batch_size` of them. A batch may mix
  topics and partitions and carries no ordering guarantees.

  Return either a single ack type, applied to the whole batch, or a list of
  `{ack_type, record}` to acknowledge records individually. Records missing from the
  list are released (a warning is logged once per batch). An invalid return value is
  treated like a raising callback.

  If the callback raises, throws or exits, the error is logged, the whole batch is
  released and the worker keeps running. Released records are redelivered with a
  higher `delivery_count`, and archived by the broker after
  `share.delivery.count.limit` deliveries.
  """
  @callback handle_record_batch(group_name :: String.t(), list(Klife.Record.t())) ::
              ack_type | list({ack_type, Klife.Record.t()})

  @optional_callbacks handle_record_batch: 2

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      # Gives the group time to close its sessions before the supervisor kills it.
      use GenServer, shutdown: 10_000
      Klife.Consumer.ShareGroup

      if opts[:client] do
        def klife_client(), do: unquote(opts[:client])
      else
        def klife_client(),
          do: :persistent_term.get({Klife.Consumer.ShareGroup, __MODULE__}, nil)
      end

      @doc """
      Pulls records acquired for this member.

      See `Klife.Consumer.ShareGroup.pull/4`. The client and share group module
      are filled in automatically.
      """
      def pull(group_name, opts \\ []) do
        Klife.Consumer.ShareGroup.pull(klife_client(), __MODULE__, group_name, opts)
      end

      @doc """
      Acknowledges records with individual ack types.

      See `Klife.Consumer.ShareGroup.acknowledge/4`.
      """
      def acknowledge(group_name, typed_records) do
        Klife.Consumer.ShareGroup.acknowledge(
          klife_client(),
          __MODULE__,
          group_name,
          typed_records
        )
      end

      @doc """
      Acknowledges records with the same ack type.

      See `Klife.Consumer.ShareGroup.acknowledge/5`.
      """
      def acknowledge(group_name, records, ack_type) do
        Klife.Consumer.ShareGroup.acknowledge(
          klife_client(),
          __MODULE__,
          group_name,
          records,
          ack_type
        )
      end

      @doc """
      Returns the `{topic, partition}` tuples currently assigned to this member.

      See `Klife.Consumer.ShareGroup.assigned_partitions/3`.
      """
      def assigned_partitions(group_name) do
        Klife.Consumer.ShareGroup.assigned_partitions(klife_client(), __MODULE__, group_name)
      end

      def start_link(args) do
        sg_opts = unquote(opts)
        final_args = sg_opts |> Map.new() |> Map.merge(Map.new(args)) |> Map.to_list()

        Klife.Consumer.ShareGroup.start_link(__MODULE__, final_args)
      end

      def init(args), do: Klife.Consumer.ShareGroup.init(__MODULE__, args)

      def handle_info(:heartbeat, state), do: Klife.Consumer.ShareGroup.handle_heartbeat(state)

      def handle_info(:reroute, state), do: Klife.Consumer.ShareGroup.handle_reroute(state)

      def handle_info(:recompute, state), do: Klife.Consumer.ShareGroup.handle_recompute(state)

      def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
        Klife.Consumer.ShareGroup.handle_session_down(state, ref, reason)
      end

      def handle_info({:EXIT, _from, reason}, _state), do: exit(reason)

      def terminate(reason, state), do: Klife.Consumer.ShareGroup.handle_terminate(state, reason)
    end
  end

  defstruct Keyword.keys(@share_group_opts) ++
              [
                :mod,
                :client_name,
                :coordinator_id,
                :member_id,
                :epoch,
                :heartbeat_interval_ms,
                :assignment,
                :session_supervisor,
                :table,
                :recompute_ref,
                :worker_supervisor,
                sessions: %{},
                session_monitors: %{}
              ]

  @doc false
  def get_opts(), do: @share_group_opts

  @doc false
  def start_link(sg_mod, args) do
    validated = args |> NimbleOptions.validate!(@share_group_opts) |> resolve_mode!(sg_mod)
    client = Keyword.fetch!(validated, :client)

    :ok = bind_client!(sg_mod, client)

    if ConnController.disabled_feature?(client, :share_group) do
      raise "Share Group was started but share_group feature is disabled for client #{inspect(client)}. Check logs for details."
    end

    state_map =
      %__MODULE__{}
      |> Map.from_struct()
      |> Map.merge(Map.new(validated))
      |> Map.put(:mod, sg_mod)
      |> Map.put(:member_id, UUID.uuid4())

    GenServer.start_link(sg_mod, state_map,
      name: get_process_name(client, sg_mod, state_map.group_name)
    )
  end

  defp resolve_mode!(validated, sg_mod) do
    exported? =
      Code.ensure_loaded?(sg_mod) and function_exported?(sg_mod, :handle_record_batch, 2)

    case {Keyword.fetch!(validated, :mode), exported?} do
      {nil, true} ->
        Keyword.put(validated, :mode, :auto)

      {nil, false} ->
        Keyword.put(validated, :mode, :manual)

      {:auto, false} ->
        raise ArgumentError,
              "#{inspect(sg_mod)} does not implement handle_record_batch/2, which is " <>
                "required for mode: :auto. Implement the callback or set mode: :manual."

      {mode, _} ->
        Keyword.put(validated, :mode, mode)
    end
  end

  # Same permanent client binding semantics as Klife.Consumer.ConsumerGroup.
  defp bind_client!(sg_mod, client) do
    case sg_mod.klife_client() do
      nil ->
        :persistent_term.put({__MODULE__, sg_mod}, client)

      ^client ->
        :ok

      other_client ->
        raise ArgumentError,
              "#{inspect(sg_mod)} is already bound to the client " <>
                "#{inspect(other_client)} and cannot be started with client: " <>
                "#{inspect(client)}. A share group module is permanently bound " <>
                "to a single client; define another share group module to " <>
                "consume through a different client."
    end
  end

  defp get_process_name(client, sg_mod, group_name),
    do: via_tuple({__MODULE__, client, sg_mod, group_name})

  defp get_table(client, sg_mod, group_name),
    do: :"share_group.#{client}.#{sg_mod}.#{group_name}"

  ## Public API

  @doc """
  Pulls records acquired for this member from the broker sessions.

  Returns `{:ok, records}` with at most `:max_records` records or `{:ok, :empty}`
  when nothing is buffered at the moment. It never blocks waiting for records,
  the sessions fetch continuously in the background, and it does not go through
  the share group process.

  Every pulled record must be acknowledged, see the moduledoc.

  ## Options

  - `:max_records` - maximum number of records returned. Defaults to `500`.
  - `:owner` - pid that owns the pulled records, see "Ownership" in the moduledoc.
    Defaults to the caller.
  """
  @spec pull(atom(), module(), String.t(), [{:max_records, pos_integer()} | {:owner, pid()}]) ::
          {:ok, [Klife.Record.t()]} | {:ok, :empty}
  def pull(client, sg_mod, group_name, opts \\ []) do
    max_records = Keyword.get(opts, :max_records, 500)
    owner = Keyword.get(opts, :owner, self())
    table = get_table(client, sg_mod, group_name)

    case table_session_pids(table) do
      [] ->
        {:ok, :empty}

      pids ->
        n = length(pids)
        start = rem(:ets.update_counter(table, :pull_counter, 1, {:pull_counter, 0}), n)
        {tail, head} = Enum.split(pids, start)

        Enum.find_value(tail ++ head, {:ok, :empty}, fn pid ->
          try do
            case Session.pull(pid, max_records, owner) do
              [] -> nil
              recs -> {:ok, recs}
            end
          catch
            # The session is stopping, the group will replace it.
            :exit, _reason -> nil
          end
        end)
    end
  end

  @doc """
  Acknowledges records, each one with its own ack type (`:accept`, `:release` or `:reject`).

  Asynchronous: returns `:ok` once the acknowledgements are queued on the sessions.
  Acknowledgements that cannot be matched to an outstanding record (e.g. the
  session was reset or the record was already redelivered) are dropped.
  """
  @spec acknowledge(atom(), module(), String.t(), [{ack_type(), Klife.Record.t()}]) :: :ok
  def acknowledge(client, sg_mod, group_name, typed_records) do
    table = get_table(client, sg_mod, group_name)

    # Validate first so a bad ack type always raises in the caller.
    coded =
      Enum.map(typed_records, fn {type, %Klife.Record{} = rec} -> {ack_code!(type), rec} end)

    if :ets.whereis(table) == :undefined do
      Logger.debug("Share group #{group_name} is not running, dropping acknowledgements",
        client: client,
        group: group_name
      )
    else
      coded
      |> Enum.flat_map(fn {code, rec} ->
        case topic_id(client, rec) do
          nil ->
            []

          tid ->
            [
              {route(table, tid, rec.partition),
               {code, tid, rec.partition, rec.offset, rec.delivery_count}}
            ]
        end
      end)
      |> Enum.group_by(fn {pid, _entry} -> pid end, fn {_pid, entry} -> entry end)
      |> Enum.each(fn
        {nil, entries} ->
          # No route known (partition no longer assigned): any session that still
          # holds the acquisition applies it, the others drop it.
          table |> table_session_pids() |> Enum.each(&Session.ack(&1, entries))

        {pid, entries} ->
          Session.ack(pid, entries)
      end)
    end

    :ok
  end

  @doc """
  Same as `acknowledge/4` but applies the same ack type to all records.
  """
  @spec acknowledge(atom(), module(), String.t(), [Klife.Record.t()], ack_type()) :: :ok
  def acknowledge(client, sg_mod, group_name, records, ack_type) do
    acknowledge(client, sg_mod, group_name, Enum.map(records, &{ack_type, &1}))
  end

  @doc """
  Returns the `{topic_name, partition}` tuples currently assigned to this member.
  """
  @spec assigned_partitions(atom(), module(), String.t()) :: [{String.t(), integer()}]
  def assigned_partitions(client, sg_mod, group_name) do
    table = get_table(client, sg_mod, group_name)

    case :ets.whereis(table) do
      :undefined ->
        []

      tid ->
        tid
        |> :ets.select([{{{:assigned, :"$1", :"$2"}}, [], [{{:"$1", :"$2"}}]}])
        |> Enum.map(fn {topic_id, partition} ->
          {MetadataCache.get_topic_name_by_id!(client, topic_id), partition}
        end)
    end
  end

  defp ack_code!(:accept), do: 1
  defp ack_code!(:release), do: 2
  defp ack_code!(:reject), do: 3

  defp ack_code!(other),
    do:
      raise(
        ArgumentError,
        "invalid ack type #{inspect(other)}, expected :accept, :release or :reject"
      )

  defp topic_id(client, %Klife.Record{topic: topic, partition: partition}) do
    MetadataCache.get_metadata_attribute(client, topic, partition, :topic_id)
  rescue
    ArgumentError -> nil
  end

  defp route(table, tid, partition) do
    with [{_, broker_id}] <- :ets.lookup(table, {:route, tid, partition}),
         [{_, pid}] <- :ets.lookup(table, {:session, broker_id}) do
      pid
    else
      _ -> nil
    end
  end

  defp table_session_pids(table) do
    if :ets.whereis(table) == :undefined, do: [], else: do_session_pids(table)
  end

  defp do_session_pids(table) do
    table
    |> :ets.select([{{{:session, :_}, :"$1"}, [], [:"$1"]}])
    |> Enum.sort()
  end

  ## Group process

  @doc false
  def init(mod, args_map) do
    Process.flag(:trap_exit, true)

    table = get_table(args_map.client, mod, args_map.group_name)

    {:ok, session_sup} = DynamicSupervisor.start_link(strategy: :one_for_one)

    :ets.new(table, [:set, :public, :named_table, read_concurrency: true])

    state =
      %__MODULE__{
        mod: mod,
        client_name: args_map.client,
        group_name: args_map.group_name,
        topics: args_map.topics,
        mode: args_map.mode,
        handler_concurrency: args_map.handler_concurrency,
        handler_max_batch_size: args_map.handler_max_batch_size,
        handler_idle_ms: args_map.handler_idle_ms,
        share_auto_offset_reset: args_map.share_auto_offset_reset,
        max_wait_ms: args_map.max_wait_ms,
        min_bytes: args_map.min_bytes,
        max_bytes: args_map.max_bytes,
        max_records: args_map.max_records,
        max_buffered_records: args_map.max_buffered_records,
        request_timeout_ms:
          Map.get(args_map, :request_timeout_ms) ||
            args_map.client.get_default_request_timeout_ms(),
        member_id: args_map.member_id,
        epoch: 0,
        heartbeat_interval_ms: 1_000,
        assignment: MapSet.new(),
        session_supervisor: session_sup,
        table: table
      }

    :ok = maybe_set_offset_reset(state)

    state = get_coordinator!(state)

    state = maybe_start_workers(state)

    send(self(), :heartbeat)

    Logger.info(
      "Starting share group #{state.group_name} with module #{state.mod}",
      client: state.client_name,
      group: state.group_name
    )

    {:ok, state}
  end

  defp maybe_start_workers(%__MODULE__{mode: :manual} = state), do: state

  defp maybe_start_workers(%__MODULE__{mode: :auto} = state) do
    {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)

    for _ <- 1..state.handler_concurrency do
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          sup,
          {Worker,
           client: state.client_name,
           mod: state.mod,
           group_name: state.group_name,
           max_batch_size: state.handler_max_batch_size,
           idle_ms: state.handler_idle_ms}
        )
    end

    %{state | worker_supervisor: sup}
  end

  defp stop_workers(%__MODULE__{worker_supervisor: nil}), do: :ok

  defp stop_workers(%__MODULE__{worker_supervisor: sup}) do
    # Each worker finishes (and acknowledges) its current batch, bounded by the
    # worker shutdown timeout of 4s. Children are terminated in parallel.
    try do
      DynamicSupervisor.stop(sup, :shutdown, 5_000)
    catch
      :exit, _ -> :ok
    end
  end

  defp maybe_set_offset_reset(%__MODULE__{share_auto_offset_reset: nil}), do: :ok

  defp maybe_set_offset_reset(%__MODULE__{} = state) do
    content = %{
      resources: [
        %{
          resource_type: 32,
          resource_name: state.group_name,
          configs: [
            %{
              name: "share.auto.offset.reset",
              config_operation: 0,
              value: Atom.to_string(state.share_auto_offset_reset)
            }
          ]
        }
      ],
      validate_only: false
    }

    Helpers.with_timeout!(
      fn ->
        case Broker.send_message(M.IncrementalAlterConfigs, state.client_name, :any, content) do
          {:ok, %{content: %{responses: [%{error_code: 0}]}}} ->
            :ok

          {:ok, %{content: %{responses: [%{error_code: ec, error_message: em}]}}} ->
            Logger.error(
              "Setting share.auto.offset.reset failed for group #{state.group_name} with error code #{ec}: #{em}",
              client: state.client_name,
              group: state.group_name
            )

            {:retry, {:error, ec}}

          {:error, reason} ->
            {:retry, {:error, reason}}
        end
      end,
      state.client_name.get_default_request_timeout_ms() * 2
    )
  end

  defp get_coordinator!(%__MODULE__{} = state) do
    content = %{key_type: 0, coordinator_keys: [state.group_name]}

    fun = fn ->
      case Broker.send_message(M.FindCoordinator, state.client_name, :any, content) do
        {:ok, %{content: %{coordinators: [%{error_code: 0, node_id: broker_id}]}}} ->
          if state.coordinator_id != nil and state.coordinator_id != broker_id do
            Logger.info(
              "Coordinator changed from #{state.coordinator_id} to #{broker_id} for share group #{state.group_name} running module #{state.mod}",
              client: state.client_name,
              group: state.group_name
            )
          end

          %{state | coordinator_id: broker_id}

        {:ok, %{content: %{coordinators: [%{error_code: ec}]}}} ->
          Logger.error(
            "#{inspect(M.FindCoordinator)} failed with error code #{ec} for share group #{state.group_name} running module #{state.mod}",
            client: state.client_name,
            group: state.group_name
          )

          {:retry, {:error, ec}}

        {:error, reason} ->
          Logger.error(
            "#{inspect(M.FindCoordinator)} failed unexpectedly with reason #{inspect(reason)} for share group #{state.group_name} running module #{state.mod}",
            client: state.client_name,
            group: state.group_name
          )

          {:retry, {:error, reason}}
      end
    end

    Helpers.with_timeout!(fun, state.client_name.get_default_request_timeout_ms() * 2)
  end

  @doc false
  def handle_heartbeat(%__MODULE__{} = state) do
    case do_heartbeat(state) do
      {:ok, new_state, next_ms} ->
        new_state = sync_sessions(new_state)
        Process.send_after(self(), :heartbeat, next_ms)
        {:noreply, new_state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  # A session noticed that a partition cannot be served (leader moved, unknown
  # topic id...). Metadata refresh was already triggered, give it a moment.
  @doc false
  def handle_reroute(%__MODULE__{recompute_ref: nil} = state) do
    {:noreply, %{state | recompute_ref: Process.send_after(self(), :recompute, 300)}}
  end

  def handle_reroute(%__MODULE__{} = state), do: {:noreply, state}

  @doc false
  def handle_recompute(%__MODULE__{} = state) do
    {:noreply, sync_sessions(%{state | recompute_ref: nil})}
  end

  defp do_heartbeat(%__MODULE__{} = state) do
    content = %{
      group_id: state.group_name,
      member_id: state.member_id,
      member_epoch: state.epoch,
      rack_id: nil,
      subscribed_topic_names: Enum.map(state.topics, & &1[:name])
    }

    case Broker.send_message(
           M.ShareGroupHeartbeat,
           state.client_name,
           state.coordinator_id,
           content
         ) do
      {:error, error} ->
        Logger.error(
          "Heartbeat error for share group #{state.group_name} running module #{state.mod}: #{inspect(error)}",
          client: state.client_name,
          group: state.group_name,
          reason: inspect(error)
        )

        {:ok, get_coordinator!(state), state.heartbeat_interval_ms}

      {:ok, %{content: %{error_code: 0} = resp}} ->
        if resp.member_epoch != state.epoch do
          Logger.info(
            "Epoch bumped from #{state.epoch} to #{resp.member_epoch} for share group #{state.group_name} running module #{state.mod}",
            client: state.client_name,
            group: state.group_name
          )
        end

        assignment =
          case resp.assignment do
            nil ->
              state.assignment

            %{topic_partitions: tps} ->
              MapSet.new(for %{topic_id: tid, partitions: ps} <- tps, p <- ps, do: {tid, p})
          end

        {:ok,
         %{
           state
           | epoch: resp.member_epoch,
             heartbeat_interval_ms: resp.heartbeat_interval_ms,
             assignment: assignment
         }, resp.heartbeat_interval_ms}

      {:ok, %{content: %{error_code: ec} = resp}} ->
        Logger.error(
          "Heartbeat error for share group #{state.group_name} running module #{state.mod}: error_code=#{ec} error_message=#{resp[:error_message]}",
          client: state.client_name,
          group: state.group_name,
          error_code: ec
        )

        cond do
          ec in [14, 15, 16] ->
            {:ok, get_coordinator!(state), state.heartbeat_interval_ms}

          # Unknown member / fenced epoch: rejoin with the same member id, the
          # assignment is empty until the next one arrives.
          ec in [25, 110] ->
            {:ok, %{state | epoch: 0, assignment: MapSet.new()}, 100}

          true ->
            {:error, ec}
        end
    end
  end

  # Computes {topic_id, partition} => leader for the current assignment, starts
  # sessions for brokers that need one and pushes the desired partitions to all
  # sessions, then publishes routing/assignment on the ETS table.
  defp sync_sessions(%__MODULE__{} = state) do
    routes =
      state.assignment
      |> Enum.flat_map(fn {tid, p} ->
        tname = MetadataCache.get_topic_name_by_id(state.client_name, tid)

        if tname != nil and MetadataCache.metadata_exists?(state.client_name, tname, p) do
          case MetadataCache.get_metadata_attribute(state.client_name, tname, p, :leader_id) do
            leader when leader >= 0 ->
              [{{tid, p}, leader}]

            _ ->
              []
          end
        else
          Logger.warning(
            "Unkown metadata for #{tname}:#{p} assigned for share group #{state.group_name} running module #{state.mod}",
            client: state.client_name,
            group: state.group_name
          )

          []
        end
      end)

    desired_by_broker =
      routes
      |> Enum.group_by(fn {_tp, broker} -> broker end, fn {tp, _broker} -> tp end)
      |> Map.new(fn {broker, tps} -> {broker, MapSet.new(tps)} end)

    state =
      Enum.reduce(desired_by_broker, state, fn {broker, desired}, acc ->
        case Map.fetch(acc.sessions, broker) do
          {:ok, pid} ->
            Session.set_partitions(pid, desired)
            acc

          :error ->
            start_session(acc, broker, desired)
        end
      end)

    Enum.each(state.sessions, fn {broker, pid} ->
      if not Map.has_key?(desired_by_broker, broker),
        do: Session.set_partitions(pid, MapSet.new())
    end)

    publish_routes(state, routes)

    state
  end

  defp publish_routes(%__MODULE__{table: table} = state, routes) do
    new_routes = Map.new(routes)

    stale =
      table
      |> :ets.select([{{{:route, :"$1", :"$2"}, :_}, [], [{{:"$1", :"$2"}}]}])
      |> Enum.reject(&Map.has_key?(new_routes, &1))

    Enum.each(stale, fn {tid, p} ->
      :ets.delete(table, {:route, tid, p})
      :ets.delete(table, {:assigned, tid, p})
    end)

    entries =
      Enum.flat_map(new_routes, fn {{tid, p}, broker} ->
        [{{:route, tid, p}, broker}, {{:assigned, tid, p}}]
      end)

    # Sessions must be visible before the routes pointing to them.
    Enum.each(state.sessions, fn {broker, pid} ->
      :ets.insert(table, {{:session, broker}, pid})
    end)

    :ets.insert(table, entries)
  end

  defp start_session(%__MODULE__{} = state, broker, desired) do
    args = %{
      client: state.client_name,
      mod: state.mod,
      group_name: state.group_name,
      broker_id: broker,
      member_id: state.member_id,
      group_pid: self(),
      desired: desired,
      max_wait_ms: state.max_wait_ms,
      min_bytes: state.min_bytes,
      max_bytes: state.max_bytes,
      max_records: state.max_records,
      max_buffered_records: state.max_buffered_records,
      request_timeout_ms: state.request_timeout_ms
    }

    spec = %{
      id: {Session, broker},
      start: {Session, :start_link, [args]},
      restart: :temporary,
      type: :worker
    }

    case DynamicSupervisor.start_child(state.session_supervisor, spec) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info(
          "Started share session with broker #{broker} for share group #{state.group_name} running module #{state.mod}",
          client: state.client_name,
          group: state.group_name
        )

        %{
          state
          | sessions: Map.put(state.sessions, broker, pid),
            session_monitors: Map.put(state.session_monitors, ref, broker)
        }

      {:error, {:already_started, pid}} ->
        # Previous incarnation still shutting down, retry on the next tick.
        Logger.debug("Share session for broker #{broker} still running (#{inspect(pid)})",
          client: state.client_name,
          group: state.group_name
        )

        state

      {:error, reason} ->
        Logger.error(
          "Could not start share session with broker #{broker} for share group #{state.group_name}: #{inspect(reason)}",
          client: state.client_name,
          group: state.group_name
        )

        state
    end
  end

  @doc false
  def handle_session_down(%__MODULE__{} = state, ref, reason) do
    case Map.pop(state.session_monitors, ref) do
      {nil, _} ->
        Logger.error(
          "Unexpected DOWN message received for share group #{state.group_name} running module #{state.mod}",
          client: state.client_name,
          group: state.group_name,
          reason: inspect(reason)
        )

        {:stop, reason, state}

      {broker, monitors} ->
        pid = Map.get(state.sessions, broker)

        if reason not in [:normal, :shutdown] do
          Logger.error(
            "Share session with broker #{broker} for share group #{state.group_name} died: #{inspect(reason)}",
            client: state.client_name,
            group: state.group_name
          )
        end

        :ets.delete_object(state.table, {{:session, broker}, pid})

        # Re-evaluate right away so a session that stopped because it raced with
        # a new assignment is replaced without waiting for the next heartbeat.
        new_state =
          %{
            state
            | session_monitors: monitors,
              sessions: Map.delete(state.sessions, broker)
          }

        {:noreply, new_state |> sync_sessions()}
    end
  end

  @doc false
  def handle_terminate(%__MODULE__{} = state, reason) do
    Logger.info(
      "Terminating share group #{state.group_name} running module #{state.mod}: #{inspect(reason)}",
      client: state.client_name,
      group: state.group_name
    )

    # Workers go first so their last batch is acknowledged before the sessions close.
    stop_workers(state)

    # Stop serving pulls and acks, then close every session so unacknowledged
    # records are released right away.
    :ets.delete_all_objects(state.table)

    state.sessions
    |> Map.values()
    |> Task.async_stream(
      fn pid ->
        try do
          Session.close(pid, 3_000)
        catch
          :exit, _reason -> :ok
        end
      end,
      timeout: 3_500,
      on_timeout: :kill_task
    )
    |> Enum.to_list()

    case do_heartbeat(%{state | epoch: -1}) do
      {:ok, exit_state, _} ->
        exit_state

      {:error, reason} ->
        Logger.error(
          "Error while terminating share group #{state.group_name} running module #{state.mod}: #{inspect(reason)}",
          client: state.client_name,
          group: state.group_name
        )

        state
    end
  end
end
