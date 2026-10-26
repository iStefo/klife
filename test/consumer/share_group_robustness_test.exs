defmodule Klife.Consumer.ShareGroupRobustnessTest do
  use ExUnit.Case, async: false

  alias Klife.Connection.Broker
  alias Klife.Record

  alias KlifeProtocol.Messages, as: M

  @topic "test_share_topic_1"
  @partitions [0, 1, 2, 3]

  defmacrop define_share_group(mod_name) do
    quote do
      defmodule unquote(mod_name) do
        use Klife.Consumer.ShareGroup, client: MyClient
      end
    end
  end

  defp new_mod(), do: :"ShareRobustness#{:rand.bytes(8) |> Base.encode16()}"

  defp new_ids() do
    id = "#{System.unique_integer([:positive])}_#{System.os_time(:millisecond)}"
    {"share_rob_#{id}", "sgr#{id}"}
  end

  defp start_group(mod, group, opts \\ []) do
    {topic, opts} = Keyword.pop(opts, :topic, @topic)

    args =
      [group_name: group, topics: [[name: topic]], share_auto_offset_reset: :earliest]
      |> Keyword.merge(opts)

    start_supervised!({mod, args}, id: {mod, group}, restart: :temporary)
  end

  defp produce(prefix, count, opts \\ []) do
    topic = Keyword.get(opts, :topic, @topic)
    partitions = Keyword.get(opts, :partitions, @partitions)
    from = Keyword.get(opts, :from, 1)

    recs =
      for i <- from..(from + count - 1) do
        %Record{
          topic: topic,
          partition: Enum.at(partitions, rem(i, length(partitions))),
          value: "#{prefix}_#{i}"
        }
      end

    {:ok, produced} = recs |> MyClient.produce_batch() |> Record.verify_batch()
    produced
  end

  defp set_group_config(group, name, value) do
    {:ok, %{content: %{responses: [%{error_code: 0}]}}} =
      Broker.send_message(M.IncrementalAlterConfigs, MyClient, :any, %{
        resources: [
          %{
            resource_type: 32,
            resource_name: group,
            configs: [%{name: name, config_operation: 0, value: value}]
          }
        ],
        validate_only: false
      })

    :ok
  end

  defp own?(%Record{value: value}, prefix), do: String.starts_with?(value, prefix <> "_")

  defp tp(%Record{topic: t, partition: p, offset: o}), do: {t, p, o}

  defp now(), do: System.monotonic_time(:millisecond)

  # Pulls until `count` distinct own offsets were seen (or the timeout hits).
  # Accepts everything not owned by the test. Own records are acknowledged
  # with the type returned by `ack_fun.(record)` (nil: not acknowledged).
  # Returns all own records pulled, in arrival order.
  defp pull_until(mod, group, prefix, count, opts \\ []) do
    deadline = now() + Keyword.get(opts, :timeout, 30_000)
    ack_fun = Keyword.get(opts, :ack_fun, fn _ -> :accept end)
    do_pull_until(mod, group, prefix, count, opts, ack_fun, deadline, [])
  end

  defp do_pull_until(mod, group, prefix, count, opts, ack_fun, deadline, acc) do
    distinct = acc |> Enum.map(&tp/1) |> Enum.uniq() |> length()

    cond do
      distinct >= count ->
        acc

      now() > deadline ->
        flunk("timed out: pulled #{distinct} of #{count} expected distinct own records")

      true ->
        case mod.pull(group, max_records: Keyword.get(opts, :max_records, 100)) do
          {:ok, :empty} ->
            Process.sleep(50)
            do_pull_until(mod, group, prefix, count, opts, ack_fun, deadline, acc)

          {:ok, recs} ->
            {own, others} = Enum.split_with(recs, &own?(&1, prefix))
            :ok = mod.acknowledge(group, others, :accept)

            typed = for r <- own, t = ack_fun.(r), do: {t, r}
            :ok = mod.acknowledge(group, typed)

            do_pull_until(mod, group, prefix, count, opts, ack_fun, deadline, acc ++ own)
        end
    end
  end

  defp pull_for(mod, group, prefix, duration) do
    deadline = now() + duration
    do_pull_for(mod, group, prefix, deadline, [])
  end

  defp do_pull_for(mod, group, prefix, deadline, acc) do
    if now() > deadline do
      acc
    else
      case mod.pull(group, max_records: 100) do
        {:ok, :empty} ->
          Process.sleep(50)
          do_pull_for(mod, group, prefix, deadline, acc)

        {:ok, recs} ->
          :ok = mod.acknowledge(group, recs, :accept)
          do_pull_for(mod, group, prefix, deadline, acc ++ Enum.filter(recs, &own?(&1, prefix)))
      end
    end
  end

  test "join latency: time from start to the first pulled record" do
    latencies =
      for _ <- 1..3 do
        mod = new_mod()
        define_share_group(mod)
        {group, prefix} = new_ids()
        produce(prefix, 8)

        started = now()
        pid = start_group(mod, group)
        pull_until(mod, group, prefix, 1)
        latency = now() - started

        :ok = stop_supervised({mod, group})
        refute Process.alive?(pid)
        latency
      end

    IO.puts("JOIN LATENCIES ms: #{inspect(latencies)}")
    assert Enum.max(latencies) < 3_000
  end
end
