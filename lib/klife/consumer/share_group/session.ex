defmodule Klife.Consumer.ShareGroup.Session do
  @moduledoc false

  # One share session (KIP-932) between a share group member and a single broker.
  #
  # The process owns the session epoch, the buffer of acquired records, the set
  # of outstanding (acquired but not yet acknowledged) records and the pending
  # acknowledgements. It keeps at most one request in flight (a share session
  # allows only one) and decides what to send next every time something
  # changes, see `maybe_send/1`.
  #
  # ## Acknowledgements for partitions that left the session
  #
  # Verified empirically against Kafka 4.2 (see
  # test/consumer/share_forgotten_acks_test.exs):
  #
  #   * a ShareAcknowledge carrying acks for a partition that was forgotten
  #     (or is otherwise not part of the session) succeeds and the acks are
  #     applied by the broker;
  #   * listing such a partition under `topics` of a ShareFetch, even if it only
  #     carries acks, RE-ADDS it to the session and acquires new records for it.
  #
  # Therefore acks for partitions that are not in `desired` anymore are always
  # sent with ShareAcknowledge (before the ShareFetch that forgets the
  # partition) and never piggybacked on a ShareFetch. No fallback of keeping the
  # partition in the session while it has outstanding records is needed.

  use GenServer

  import Klife.ProcessRegistry, only: [via_tuple: 1]

  require Logger

  alias Klife.Connection.Broker
  alias Klife.MetadataCache
  alias Klife.Record

  alias KlifeProtocol.Messages, as: M

  @max_epoch 2_147_483_647

  # Wire ack types
  @gap 0
  @release 2

  @metadata_error_codes [3, 6, 100]
  @session_error_codes [122, 123]

  @reset_backoff_ms 100
  @error_backoff_ms 1_000

  defstruct [
    :client_name,
    :mod,
    :group_name,
    :broker_id,
    :member_id,
    :group_pid,
    :max_wait_ms,
    :min_bytes,
    :max_bytes,
    :max_records,
    :max_buffered_records,
    :request_timeout_ms,
    :lock_timeout_ms,
    :in_flight,
    :backoff_ref,
    :close_from,
    epoch: 0,
    open?: false,
    closing?: false,
    desired: MapSet.new(),
    in_session: MapSet.new(),
    buffer: :queue.new(),
    buffered_count: 0,
    outstanding: %{},
    pending_acks: %{},
    owners: %{}
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: get_process_name(args.client, args.mod, args.group_name, args.broker_id)
    )
  end

  def get_process_name(client, mod, group_name, broker_id),
    do: via_tuple({__MODULE__, client, mod, group_name, broker_id})

  def pull(pid, max_records, owner), do: GenServer.call(pid, {:pull, max_records, owner})

  def ack(pid, entries), do: GenServer.cast(pid, {:ack, entries})

  def set_partitions(pid, %MapSet{} = partitions),
    do: GenServer.cast(pid, {:set_partitions, partitions})

  def close(pid, timeout), do: GenServer.call(pid, :close, timeout)

  @impl true
  def init(args) do
    state = %__MODULE__{
      client_name: args.client,
      mod: args.mod,
      group_name: args.group_name,
      broker_id: args.broker_id,
      member_id: args.member_id,
      group_pid: args.group_pid,
      desired: args.desired,
      max_wait_ms: args.max_wait_ms,
      min_bytes: args.min_bytes,
      max_bytes: args.max_bytes,
      max_records: args.max_records,
      max_buffered_records: args.max_buffered_records,
      request_timeout_ms: args.request_timeout_ms
    }

    {:ok, state, {:continue, :maybe_send}}
  end

  @impl true
  def handle_continue(:maybe_send, state), do: continue(state)

  @impl true
  def handle_call({:pull, max_records, owner}, _from, %__MODULE__{} = state) do
    {taken, queue} = take_from_queue(state.buffer, max_records, [])
    count = length(taken)

    {outstanding, owners} =
      Enum.reduce(taken, {state.outstanding, state.owners}, fn {key, rec}, {out, owners} ->
        {tid, p} = key
        {dc, _} = Map.fetch!(out, {tid, p, rec.offset})
        {Map.put(out, {tid, p, rec.offset}, {dc, owner}), owners}
      end)

    owners =
      if count > 0 and not Map.has_key?(owners, owner),
        do: Map.put(owners, owner, Process.monitor(owner)),
        else: owners

    state = %{
      state
      | buffer: queue,
        buffered_count: state.buffered_count - count,
        outstanding: outstanding,
        owners: owners
    }

    recs = Enum.map(taken, fn {_key, rec} -> rec end)
    reply_continue(recs, state)
  end

  def handle_call(:close, from, %__MODULE__{} = state) do
    continue(%{state | closing?: true, close_from: from})
  end

  @impl true
  def handle_cast({:ack, entries}, %__MODULE__{} = state) do
    continue(Enum.reduce(entries, state, &apply_ack/2))
  end

  def handle_cast({:set_partitions, new_desired}, %__MODULE__{} = state) do
    if MapSet.equal?(new_desired, state.desired) do
      {:noreply, state}
    else
      removed = MapSet.difference(state.desired, new_desired)
      state = %{state | desired: new_desired}
      state = Enum.reduce(removed, state, &release_buffered_partition/2)
      continue(state)
    end
  end

  @impl true
  def handle_info(
        {:async_broker_response, ref, binary_resp, msg_mod, msg_version},
        %__MODULE__{in_flight: %{ref: ref} = in_flight} = state
      ) do
    state = %{state | in_flight: nil}

    parsed =
      try do
        msg_mod.deserialize_response(binary_resp, msg_version)
      rescue
        error -> {:deserialize_error, error}
      end

    case parsed do
      {:ok, %{content: %{error_code: 0} = content}} ->
        case handle_success(state, in_flight, content) do
          {:stop, _state} = stop -> continue_or_stop(stop)
          new_state -> continue(new_state)
        end

      {:ok, %{content: %{error_code: ec} = content}} ->
        handle_top_level_error(state, in_flight, ec, content[:error_message])

      other ->
        log_error(state, "unexpected #{inspect(msg_mod)} response: #{inspect(other)}")
        state |> reset(@error_backoff_ms) |> continue()
    end
  end

  def handle_info(
        {:async_broker_response, ref, :timeout},
        %__MODULE__{in_flight: %{ref: ref}} = state
      ) do
    log_error(state, "request timed out, resetting share session")
    state |> Map.put(:in_flight, nil) |> reset(@error_backoff_ms) |> continue()
  end

  # Late responses/timeouts of requests we already gave up on.
  def handle_info({:async_broker_response, _ref, _}, state), do: {:noreply, state}
  def handle_info({:async_broker_response, _ref, _, _, _}, state), do: {:noreply, state}

  def handle_info(:backoff_done, %__MODULE__{} = state),
    do: continue(%{state | backoff_ref: nil})

  def handle_info({:DOWN, ref, :process, pid, _reason}, %__MODULE__{} = state) do
    case Map.get(state.owners, pid) do
      ^ref ->
        {released, outstanding} =
          Enum.split_with(state.outstanding, fn {_k, {_dc, owner}} -> owner == pid end)

        pending =
          Enum.reduce(released, state.pending_acks, fn {{tid, p, offset}, _}, acc ->
            put_ack(acc, tid, p, offset, @release)
          end)

        if released != [] do
          Logger.debug(
            "Owner #{inspect(pid)} died with #{length(released)} unacknowledged records, releasing them (client=#{inspect(state.client_name)}, group=#{state.group_name})",
            client: state.client_name,
            group: state.group_name
          )
        end

        continue(%{
          state
          | owners: Map.delete(state.owners, pid),
            outstanding: Map.new(outstanding),
            pending_acks: pending
        })

      _ ->
        {:noreply, state}
    end
  end

  ## Decision loop

  defp continue(state), do: state |> maybe_send() |> continue_or_stop()

  defp continue_or_stop({:stop, %__MODULE__{} = state}) do
    if state.close_from, do: GenServer.reply(state.close_from, :ok)
    {:stop, :normal, state}
  end

  defp continue_or_stop(%__MODULE__{} = state), do: {:noreply, state}

  defp reply_continue(reply, state) do
    case maybe_send(state) do
      {:stop, state} ->
        if state.close_from, do: GenServer.reply(state.close_from, :ok)
        {:stop, :normal, reply, %{state | close_from: nil}}

      state ->
        {:reply, reply, state}
    end
  end

  # Re-evaluated whenever nothing is in flight and something changed (response,
  # pull, ack, owner DOWN, set_partitions, backoff end).
  defp maybe_send(%__MODULE__{in_flight: in_flight} = state) when in_flight != nil, do: state

  defp maybe_send(%__MODULE__{closing?: true, open?: false} = state), do: {:stop, state}

  defp maybe_send(%__MODULE__{backoff_ref: ref} = state) when ref != nil, do: state

  defp maybe_send(%__MODULE__{closing?: true} = state), do: send_close(state)

  defp maybe_send(%__MODULE__{open?: false} = state) do
    if MapSet.size(state.desired) == 0, do: {:stop, state}, else: send_fetch(state)
  end

  defp maybe_send(%__MODULE__{} = state) do
    desired? = MapSet.size(state.desired) > 0

    cond do
      stale_acks?(state) ->
        send_ack(state, :stale)

      desired? and state.buffered_count < state.max_buffered_records ->
        send_fetch(state)

      state.pending_acks != %{} ->
        send_ack(state, :all)

      not desired? and state.outstanding == %{} ->
        maybe_send(%{state | closing?: true})

      true ->
        state
    end
  end

  defp stale_acks?(%__MODULE__{} = state),
    do: Enum.any?(state.pending_acks, fn {key, _} -> not MapSet.member?(state.desired, key) end)

  ## Requests

  defp send_fetch(%__MODULE__{open?: false} = state) do
    # Opening a session: epoch 0 cannot carry acks, they belonged to a dead session.
    topics = build_topics(state.desired, %{})

    state = %{state | pending_acks: %{}}
    do_send_fetch(state, topics, [], %{kind: :fetch, add: state.desired, forget: MapSet.new()})
  end

  defp send_fetch(%__MODULE__{} = state) do
    add = MapSet.difference(state.desired, state.in_session)
    forget = MapSet.difference(state.in_session, state.desired)

    {acks, rest} =
      Map.split_with(state.pending_acks, fn {key, _} -> MapSet.member?(state.desired, key) end)

    listed = MapSet.union(add, MapSet.new(Map.keys(acks)))

    forgotten =
      forget
      |> Enum.group_by(fn {tid, _p} -> tid end, fn {_tid, p} -> p end)
      |> Enum.map(fn {tid, ps} -> %{topic_id: tid, partitions: ps} end)

    do_send_fetch(
      %{state | pending_acks: rest},
      build_topics(listed, acks),
      forgotten,
      %{kind: :fetch, add: add, forget: forget}
    )
  end

  defp do_send_fetch(%__MODULE__{} = state, topics, forgotten, in_flight_data) do
    content = %{
      group_id: state.group_name,
      member_id: state.member_id,
      share_session_epoch: state.epoch,
      max_wait_ms: state.max_wait_ms,
      min_bytes: state.min_bytes,
      max_bytes: state.max_bytes,
      max_records: state.max_records,
      batch_size: state.max_records,
      topics: topics,
      forgotten_topics_data: forgotten
    }

    dispatch(
      state,
      M.ShareFetch,
      content,
      in_flight_data,
      state.request_timeout_ms + state.max_wait_ms
    )
  end

  defp send_ack(%__MODULE__{} = state, which) do
    {acks, rest} =
      case which do
        :all ->
          {state.pending_acks, %{}}

        :stale ->
          Map.split_with(state.pending_acks, fn {key, _} ->
            not MapSet.member?(state.desired, key)
          end)
      end

    content = ack_content(state, state.epoch, acks)

    dispatch(
      %{state | pending_acks: rest},
      M.ShareAcknowledge,
      content,
      %{kind: :ack},
      state.request_timeout_ms
    )
  end

  defp send_close(%__MODULE__{} = state) do
    content = ack_content(state, -1, state.pending_acks)

    dispatch(
      %{state | pending_acks: %{}},
      M.ShareAcknowledge,
      content,
      %{kind: :close},
      state.request_timeout_ms
    )
  end

  defp ack_content(%__MODULE__{} = state, epoch, acks) do
    %{
      group_id: state.group_name,
      member_id: state.member_id,
      share_session_epoch: epoch,
      topics: build_topics(Map.keys(acks), acks)
    }
  end

  defp dispatch(%__MODULE__{} = state, msg_mod, content, in_flight_data, timeout_ms) do
    ref = make_ref()

    result =
      try do
        Broker.send_message(msg_mod, state.client_name, state.broker_id, content, %{},
          async: true,
          callback_pid: self(),
          callback_ref: ref,
          timeout_ms: timeout_ms
        )
      rescue
        error -> {:error, error}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end

    case result do
      :ok ->
        %{state | in_flight: Map.put(in_flight_data, :ref, ref)}

      {:error, reason} ->
        log_error(state, "could not send #{inspect(msg_mod)}: #{inspect(reason)}")

        case reset(state, @error_backoff_ms) do
          %__MODULE__{closing?: true} = state -> {:stop, state}
          state -> state
        end
    end
  end

  defp build_topics(partition_keys, ack_map) do
    partition_keys
    |> Enum.group_by(fn {tid, _p} -> tid end, fn {_tid, p} = key -> {p, key} end)
    |> Enum.map(fn {tid, ps} ->
      %{
        topic_id: tid,
        partitions:
          Enum.map(ps, fn {p, key} ->
            %{
              partition_index: p,
              acknowledgement_batches: ack_batches(Map.get(ack_map, key, %{}))
            }
          end)
      }
    end)
  end

  # Coalesces contiguous offsets with the same ack type into one batch.
  defp ack_batches(offset_map) do
    offset_map
    |> Enum.sort()
    |> Enum.reduce([], fn
      {offset, type}, [{first, last, type} | rest] when offset == last + 1 ->
        [{first, offset, type} | rest]

      {offset, type}, acc ->
        [{offset, offset, type} | acc]
    end)
    |> Enum.reverse()
    |> Enum.map(fn {first, last, type} ->
      %{first_offset: first, last_offset: last, acknowledge_types: [type]}
    end)
  end

  ## Responses

  defp handle_top_level_error(state, in_flight, ec, em) do
    log_error(
      state,
      "#{in_flight.kind} request failed with error_code=#{ec} error_message=#{em}, resetting share session"
    )

    backoff = if ec in @session_error_codes, do: @reset_backoff_ms, else: @error_backoff_ms
    state |> reset(backoff) |> continue()
  end

  defp handle_success(%__MODULE__{} = state, %{kind: :close}, content) do
    log_partition_errors(state, content)
    {:stop, %{state | open?: false, in_flight: nil}}
  end

  defp handle_success(%__MODULE__{} = state, in_flight, content) do
    state = %{
      state
      | epoch: next_epoch(state.epoch),
        open?: true,
        lock_timeout_ms: Map.get(content, :acquisition_lock_timeout_ms, state.lock_timeout_ms)
    }

    state =
      if in_flight.kind == :fetch do
        in_session =
          state.in_session
          |> MapSet.union(in_flight.add)
          |> MapSet.difference(in_flight.forget)

        %{state | in_session: in_session}
      else
        state
      end

    content.responses
    |> Enum.flat_map(fn %{topic_id: tid, partitions: parts} ->
      Enum.map(parts, &{tid, &1})
    end)
    |> Enum.reduce(state, &handle_partition_response(&1, &2, in_flight.kind))
  end

  defp handle_partition_response({tid, pdata}, %__MODULE__{} = state, kind) do
    p = pdata.partition_index
    ack_ec = Map.get(pdata, :acknowledge_error_code, 0)

    if ack_ec != 0 do
      # 121 (INVALID_RECORD_STATE) means the lock is gone, the others also mean
      # the acks were lost. In both cases the broker will redeliver the records.
      Logger.warning(
        "Acknowledgement for #{partition_name(state, tid, p)} failed with error_code=#{ack_ec}, records will be redelivered (client=#{inspect(state.client_name)}, group=#{state.group_name})",
        client: state.client_name,
        group: state.group_name,
        error_code: ack_ec
      )
    end

    cond do
      pdata.error_code == 0 and kind == :fetch ->
        process_acquired(state, tid, p, pdata)

      # ShareAcknowledge v1 responses carry no records.
      pdata.error_code == 0 ->
        state

      pdata.error_code in @metadata_error_codes ->
        Logger.warning(
          "Partition #{partition_name(state, tid, p)} error_code=#{pdata.error_code}, re-routing (client=#{inspect(state.client_name)}, group=#{state.group_name})",
          client: state.client_name,
          group: state.group_name,
          error_code: pdata.error_code
        )

        MetadataCache.trigger_check_metadata(state.client_name)
        send(state.group_pid, :reroute)
        drop_partition(state, tid, p)

      pdata.error_code == 121 ->
        Logger.debug(
          "Ack for #{partition_name(state, tid, p)} rejected with INVALID_RECORD_STATE, dropping (client=#{inspect(state.client_name)}, group=#{state.group_name})",
          client: state.client_name,
          group: state.group_name
        )

        state

      true ->
        log_error(
          state,
          "partition #{partition_name(state, tid, p)} failed with error_code=#{pdata.error_code} error_message=#{pdata[:error_message]}"
        )

        state
    end
  end

  defp log_partition_errors(state, content) do
    for %{topic_id: tid, partitions: parts} <- content.responses,
        %{error_code: ec} = pdata <- parts,
        ec != 0 do
      Logger.warning(
        "Close request for #{partition_name(state, tid, pdata.partition_index)} returned error_code=#{ec} (client=#{inspect(state.client_name)}, group=#{state.group_name})",
        client: state.client_name,
        group: state.group_name,
        error_code: ec
      )
    end

    :ok
  end

  defp process_acquired(%__MODULE__{} = state, _tid, _p, %{acquired_records: []}), do: state

  defp process_acquired(%__MODULE__{} = state, tid, p, pdata) do
    key = {tid, p}

    cond do
      # The partition was forgotten meanwhile, hand everything back.
      not MapSet.member?(state.desired, key) ->
        pending =
          Enum.reduce(pdata.acquired_records, state.pending_acks, fn range, acc ->
            Enum.reduce(range.first_offset..range.last_offset, acc, fn offset, acc2 ->
              put_ack(acc2, tid, p, offset, @release)
            end)
          end)

        %{state | pending_acks: pending}

      true ->
        case MetadataCache.get_topic_name_by_id(state.client_name, tid) do
          nil ->
            MetadataCache.trigger_check_metadata(state.client_name)
            send(state.group_pid, :reroute)
            drop_partition(state, tid, p)

          topic ->
            do_process_acquired(state, topic, tid, p, pdata)
        end
    end
  end

  defp do_process_acquired(state, topic, tid, p, pdata) do
    # Record batches may contain offsets we did not acquire and control records,
    # both must be dropped. Mirrors the Java client (ShareCompletedFetch).
    records_by_offset =
      pdata.records
      |> Enum.flat_map(&Record.parse_from_protocol(topic, p, &1))
      |> Enum.reject(fn rec -> rec.batch_attributes.is_control_batch end)
      |> Map.new(fn rec -> {rec.offset, rec} end)

    Enum.reduce(pdata.acquired_records, state, fn range, acc ->
      dc = range.delivery_count

      Enum.reduce(range.first_offset..range.last_offset, acc, fn offset, acc2 ->
        case Map.fetch(records_by_offset, offset) do
          {:ok, rec} ->
            rec = %{rec | delivery_count: dc, consumer_attempts: dc - 1}

            %{
              acc2
              | buffer: :queue.in({{tid, p}, rec}, acc2.buffer),
                buffered_count: acc2.buffered_count + 1,
                outstanding: Map.put(acc2.outstanding, {tid, p, offset}, {dc, nil})
            }

          :error ->
            # Acquired but there is no (non control) record: acknowledge as GAP.
            %{acc2 | pending_acks: put_ack(acc2.pending_acks, tid, p, offset, @gap)}
        end
      end)
    end)
  end

  ## Acks

  defp apply_ack({type, tid, p, offset, dc}, %__MODULE__{} = state) do
    case Map.get(state.outstanding, {tid, p, offset}) do
      {^dc, _owner} ->
        %{
          state
          | outstanding: Map.delete(state.outstanding, {tid, p, offset}),
            pending_acks: put_ack(state.pending_acks, tid, p, offset, type)
        }

      _ ->
        Logger.debug(
          "Dropping stale ack for #{tid}:#{p}:#{offset} delivery_count=#{dc} (client=#{inspect(state.client_name)}, group=#{state.group_name})",
          client: state.client_name,
          group: state.group_name
        )

        state
    end
  end

  defp put_ack(pending, tid, p, offset, type),
    do: Map.update(pending, {tid, p}, %{offset => type}, &Map.put(&1, offset, type))

  ## State helpers

  # Partitions leaving `desired`: drop what was not pulled yet and release it.
  # Already pulled records stay outstanding and can still be acknowledged, see
  # the moduledoc comment about acks for forgotten partitions.
  defp release_buffered_partition({tid, p}, %__MODULE__{} = state) do
    {dropped, kept} =
      state.buffer
      |> :queue.to_list()
      |> Enum.split_with(fn {key, _rec} -> key == {tid, p} end)

    pending =
      Enum.reduce(dropped, state.pending_acks, fn {_key, rec}, acc ->
        put_ack(acc, tid, p, rec.offset, @release)
      end)

    outstanding =
      Enum.reduce(dropped, state.outstanding, fn {_key, rec}, acc ->
        Map.delete(acc, {tid, p, rec.offset})
      end)

    %{
      state
      | buffer: :queue.from_list(kept),
        buffered_count: length(kept),
        pending_acks: pending,
        outstanding: outstanding
    }
  end

  # The partition cannot be served by this session anymore (leader change etc).
  defp drop_partition(%__MODULE__{} = state, tid, p) do
    kept =
      state.buffer
      |> :queue.to_list()
      |> Enum.reject(fn {key, _rec} -> key == {tid, p} end)

    %{
      state
      | desired: MapSet.delete(state.desired, {tid, p}),
        buffer: :queue.from_list(kept),
        buffered_count: length(kept),
        outstanding:
          Map.reject(state.outstanding, fn {{t, pp, _o}, _} -> {t, pp} == {tid, p} end),
        pending_acks: Map.delete(state.pending_acks, {tid, p})
    }
  end

  # We cannot know whether the broker advanced the epoch (or still has the
  # session) so everything tied to the old session is dropped.
  defp reset(%__MODULE__{} = state, backoff_ms) do
    ref = Process.send_after(self(), :backoff_done, backoff_ms)

    if state.backoff_ref, do: Process.cancel_timer(state.backoff_ref)

    %{
      state
      | epoch: 0,
        open?: false,
        in_session: MapSet.new(),
        buffer: :queue.new(),
        buffered_count: 0,
        outstanding: %{},
        pending_acks: %{},
        in_flight: nil,
        backoff_ref: ref
    }
  end

  defp take_from_queue(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp take_from_queue(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, item}, rest} -> take_from_queue(rest, n - 1, [item | acc])
      {:empty, queue} -> {Enum.reverse(acc), queue}
    end
  end

  defp next_epoch(@max_epoch), do: 1
  defp next_epoch(epoch), do: epoch + 1

  defp partition_name(state, tid, p) do
    "#{MetadataCache.get_topic_name_by_id(state.client_name, tid) || tid}:#{p}"
  end

  defp log_error(%__MODULE__{} = state, msg) do
    Logger.error(
      "Share session with broker #{state.broker_id} for group #{state.group_name} running module #{state.mod}: #{msg} (client=#{inspect(state.client_name)}, group=#{state.group_name})",
      client: state.client_name,
      group: state.group_name,
      broker_id: state.broker_id
    )
  end
end
