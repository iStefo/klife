defmodule Klife.Consumer.ShareGroup.Worker do
  @moduledoc false
  # Worker of a share group running in `mode: :auto`. Loops
  # `pull(owner: self())` -> `handle_record_batch/2` -> `acknowledge`.
  #
  # Being the owner of the pulled records means that if the worker is killed
  # mid-batch the session releases its outstanding records. The worker traps
  # exits so a supervisor shutdown is only handled between batches, which lets
  # the current batch finish and be acknowledged.

  use GenServer, restart: :permanent, shutdown: 4_000

  require Logger

  alias Klife.Consumer.ShareGroup

  defstruct [:client, :mod, :group_name, :max_batch_size, :idle_ms]

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)
    state = struct!(__MODULE__, args)
    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, %__MODULE__{} = state) do
    case ShareGroup.pull(state.client, state.mod, state.group_name,
           max_records: state.max_batch_size,
           owner: self()
         ) do
      {:ok, :empty} ->
        Process.send_after(self(), :poll, state.idle_ms)

      {:ok, records} ->
        process_batch(state, records)
        send(self(), :poll)
    end

    {:noreply, state}
  end

  def handle_info({:EXIT, _from, reason}, state), do: {:stop, reason, state}

  def handle_info(_msg, state), do: {:noreply, state}

  defp process_batch(state, records) do
    # Invalid return values (e.g. unknown ack types) are handled like a raising
    # callback: logged and the batch released, the worker keeps running.
    typed =
      try do
        state.mod.handle_record_batch(state.group_name, records)
        |> normalize(state, records)
      catch
        kind, reason ->
          Logger.error(
            "Share group #{state.group_name} running module #{state.mod} failed to handle a batch of #{length(records)} records, releasing it: " <>
              Exception.format(kind, reason, __STACKTRACE__),
            client: state.client,
            group: state.group_name
          )

          Enum.map(records, &{:release, &1})
      end

    ShareGroup.acknowledge(state.client, state.mod, state.group_name, typed)
  end

  defp normalize(type, _state, records) when is_atom(type),
    do: Enum.map(records, &{validate!(type), &1})

  defp normalize(list, state, records) when is_list(list) do
    typed = Enum.map(list, fn {type, %Klife.Record{} = rec} -> {validate!(type), rec} end)

    acked = MapSet.new(typed, fn {_t, r} -> {r.topic, r.partition, r.offset} end)

    missing =
      Enum.reject(records, fn r -> MapSet.member?(acked, {r.topic, r.partition, r.offset}) end)

    if missing != [] do
      Logger.warning(
        "Share group #{state.group_name} running module #{state.mod}: handle_record_batch/2 did not return an ack type for #{length(missing)} records, releasing them",
        client: state.client,
        group: state.group_name
      )
    end

    typed ++ Enum.map(missing, &{:release, &1})
  end

  defp normalize(other, _state, _records),
    do:
      raise(
        ArgumentError,
        "handle_record_batch/2 must return an ack type or a list of {ack_type, record}, got: #{inspect(other)}"
      )

  defp validate!(type) when type in [:accept, :release, :reject], do: type

  defp validate!(other),
    do:
      raise(
        ArgumentError,
        "invalid ack type #{inspect(other)}, expected :accept, :release or :reject"
      )
end
