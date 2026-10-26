defmodule Klife.Consumer.ShareGroupTest do
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

  defp new_mod(), do: :"ShareGroupTest#{:rand.bytes(8) |> Base.encode16()}"

  defp new_ids() do
    id = "#{System.unique_integer([:positive])}_#{System.os_time(:millisecond)}"
    {"share_test_#{id}", "sgt#{id}"}
  end

  defp start_group(mod, group, opts \\ []) do
    args =
      [group_name: group, topics: [[name: @topic]], share_auto_offset_reset: :earliest]
      |> Keyword.merge(opts)

    start_supervised!({mod, args}, id: {mod, group}, restart: :temporary)
  end

  defp produce(prefix, count) do
    recs =
      for i <- 1..count do
        %Record{
          topic: @topic,
          partition: Enum.at(@partitions, rem(i, length(@partitions))),
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

  # Pulls (accepting every record that does not belong to the test) until
  # `count` own records were received. Own records are acknowledged with
  # `:ack` (nil holds them). Returns the own records in arrival order.
  defp pull_until(mod, group, prefix, count, opts \\ []) do
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 30_000)
    do_pull_until(mod, group, prefix, count, opts, deadline, [])
  end

  defp do_pull_until(mod, group, prefix, count, opts, deadline, acc) do
    if length(acc) >= count do
      acc
    else
      case mod.pull(group, max_records: Keyword.get(opts, :max_records, 100)) do
        {:ok, :empty} ->
          if System.monotonic_time(:millisecond) > deadline do
            flunk("timed out: pulled #{length(acc)} of #{count} expected own records")
          end

          Process.sleep(50)
          do_pull_until(mod, group, prefix, count, opts, deadline, acc)

        {:ok, recs} ->
          {own, others} = Enum.split_with(recs, &own?(&1, prefix))
          :ok = mod.acknowledge(group, others, :accept)

          case Keyword.get(opts, :ack, :accept) do
            nil -> :ok
            type -> :ok = mod.acknowledge(group, own, type)
          end

          do_pull_until(mod, group, prefix, count, opts, deadline, acc ++ own)
      end
    end
  end

  # Pulls for `duration` ms accepting everything, returns the own records seen.
  defp pull_for(mod, group, prefix, duration) do
    deadline = System.monotonic_time(:millisecond) + duration
    do_pull_for(mod, group, prefix, deadline, [])
  end

  defp do_pull_for(mod, group, prefix, deadline, acc) do
    if System.monotonic_time(:millisecond) > deadline do
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

  defp wait_for_assignment(mod, group, count, timeout \\ 30_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_assignment(mod, group, count, deadline)
  end

  defp do_wait_for_assignment(mod, group, count, deadline) do
    assigned = mod.assigned_partitions(group)

    cond do
      length(assigned) >= count ->
        assigned

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(50)
        do_wait_for_assignment(mod, group, count, deadline)

      true ->
        flunk("timed out waiting for #{count} assigned partitions, got #{inspect(assigned)}")
    end
  end

  defp tp(%Record{topic: t, partition: p, offset: o}), do: {t, p, o}

  test "basic consume: pull, accept, delivery count and record fields" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produced = produce(prefix, 50)

    start_group(mod, group)

    pulled = pull_until(mod, group, prefix, 50)

    assert length(pulled) == 50
    assert pulled |> Enum.map(&tp/1) |> Enum.uniq() |> length() == 50

    expected = Map.new(produced, fn r -> {{r.topic, r.partition, r.offset}, r.value} end)

    Enum.each(pulled, fn rec ->
      assert rec.delivery_count == 1
      assert rec.consumer_attempts == 0
      assert expected[tp(rec)] == rec.value
      assert is_integer(rec.timestamp)
    end)

    assigned = wait_for_assignment(mod, group, 4)
    assert Enum.sort(assigned) == Enum.sort(for p <- @partitions, do: {@topic, p})
  end

  test "accepted records are not redelivered" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod, group)

    pulled = pull_until(mod, group, prefix, 20)

    # Acknowledging again (stale ack) must be harmless and must not resurrect anything.
    :ok = mod.acknowledge(group, pulled, :release)

    assert [] = pull_for(mod, group, prefix, 4_000)
  end

  test "released records come back with delivery_count 2" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod, group)

    first = pull_until(mod, group, prefix, 20, ack: nil)
    assert Enum.all?(first, &(&1.delivery_count == 1))

    :ok = mod.acknowledge(group, first, :release)

    second = pull_until(mod, group, prefix, 20)
    assert Enum.all?(second, &(&1.delivery_count == 2 and &1.consumer_attempts == 1))
    assert Enum.sort(Enum.map(second, &tp/1)) == Enum.sort(Enum.map(first, &tp/1))

    assert [] = pull_for(mod, group, prefix, 2_000)
  end

  test "rejected records are never redelivered" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod, group)

    pull_until(mod, group, prefix, 20, ack: :reject)

    assert [] = pull_for(mod, group, prefix, 4_000)
  end

  test "typed acknowledge/2 handles mixed ack types" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod, group)

    first = pull_until(mod, group, prefix, 20, ack: nil)
    {to_release, to_accept} = Enum.split(first, 10)

    :ok =
      mod.acknowledge(
        group,
        Enum.map(to_release, &{:release, &1}) ++ Enum.map(to_accept, &{:accept, &1})
      )

    second = pull_until(mod, group, prefix, 10)
    assert Enum.sort(Enum.map(second, &tp/1)) == Enum.sort(Enum.map(to_release, &tp/1))
    assert Enum.all?(second, &(&1.delivery_count == 2))

    assert_raise ArgumentError, fn -> mod.acknowledge(group, first, :bogus) end
  end

  test "records of a dead owner are released before the lock expires" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod, group)

    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        pulled = pull_until(mod, group, prefix, 20, ack: nil)
        send(parent, {:pulled, pulled})
      end)

    assert_receive {:pulled, first}, 30_000
    assert Enum.all?(first, &(&1.delivery_count == 1))
    assert_receive {:DOWN, ^ref, :process, ^pid, _}

    started = System.monotonic_time(:millisecond)
    second = pull_until(mod, group, prefix, 20, timeout: 20_000)
    elapsed = System.monotonic_time(:millisecond) - started

    assert Enum.all?(second, &(&1.delivery_count == 2))
    # default lock duration is 30s
    assert elapsed < 15_000
  end

  test "unacknowledged records are redelivered after the lock timeout" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    :ok = set_group_config(group, "share.record.lock.duration.ms", "2000")

    produce(prefix, 10)
    start_group(mod, group)

    first = pull_until(mod, group, prefix, 10, ack: nil)
    assert Enum.all?(first, &(&1.delivery_count == 1))
    held_at = System.monotonic_time(:millisecond)

    second = pull_until(mod, group, prefix, 10, ack: nil, timeout: 20_000)
    elapsed = System.monotonic_time(:millisecond) - held_at

    assert Enum.all?(second, &(&1.delivery_count == 2))
    assert Enum.sort(Enum.map(second, &tp/1)) == Enum.sort(Enum.map(first, &tp/1))
    assert elapsed >= 1_000
  end

  test "two members split the partitions and every record is consumed once" do
    mod1 = new_mod()
    mod2 = new_mod()
    define_share_group(mod1)
    define_share_group(mod2)
    {group, prefix} = new_ids()

    start_group(mod1, group)
    start_group(mod2, group)

    deadline = System.monotonic_time(:millisecond) + 30_000
    {a1, a2} = wait_for_split(mod1, mod2, group, deadline)

    assert [_ | _] = a1
    assert [_ | _] = a2
    assert MapSet.disjoint?(MapSet.new(a1), MapSet.new(a2))
    assert Enum.sort(a1 ++ a2) == Enum.sort(for p <- @partitions, do: {@topic, p})

    produce(prefix, 100)

    tasks =
      for m <- [mod1, mod2] do
        Task.async(fn -> pull_for(m, group, prefix, 8_000) end)
      end

    all = tasks |> Task.await_many(30_000) |> List.flatten()

    assert length(all) == 100
    assert all |> Enum.map(& &1.value) |> Enum.uniq() |> length() == 100
    assert Enum.all?(all, &(&1.delivery_count == 1))
  end

  defp wait_for_split(mod1, mod2, group, deadline) do
    a1 = mod1.assigned_partitions(group)
    a2 = mod2.assigned_partitions(group)

    cond do
      a1 != [] and a2 != [] and length(a1) + length(a2) == 4 ->
        {a1, a2}

      System.monotonic_time(:millisecond) > deadline ->
        flunk("no partition split between members: #{inspect({a1, a2})}")

      true ->
        Process.sleep(100)
        wait_for_split(mod1, mod2, group, deadline)
    end
  end

  test "acks for partitions that were reassigned to another member are still applied" do
    mod1 = new_mod()
    mod2 = new_mod()
    define_share_group(mod1)
    define_share_group(mod2)
    {group, prefix} = new_ids()

    # Long enough to survive the heartbeat interval needed to observe the rebalance.
    :ok = set_group_config(group, "share.record.lock.duration.ms", "15000")

    produce(prefix, 20)
    start_group(mod1, group)

    first = pull_until(mod1, group, prefix, 20, ack: nil)
    assert length(wait_for_assignment(mod1, group, 4)) == 4

    start_group(mod2, group)
    deadline = System.monotonic_time(:millisecond) + 30_000
    {a1, _a2} = wait_for_split(mod1, mod2, group, deadline)
    assert length(a1) < 4

    # Member 1 lost some partitions but still holds records of them.
    :ok = mod1.acknowledge(group, first, :accept)

    # If the acks of the forgotten partitions were lost the locks would expire
    # (15s) and the records would be delivered again.
    tasks =
      for m <- [mod1, mod2] do
        Task.async(fn -> pull_for(m, group, prefix, 17_000) end)
      end

    assert [] = tasks |> Task.await_many(30_000) |> List.flatten()
  end

  test "stopping a member releases its unacknowledged records to other members" do
    mod1 = new_mod()
    mod2 = new_mod()
    define_share_group(mod1)
    define_share_group(mod2)
    {group, prefix} = new_ids()

    produce(prefix, 20)
    start_group(mod1, group)

    first = pull_until(mod1, group, prefix, 20, ack: nil)
    assert Enum.all?(first, &(&1.delivery_count == 1))

    started = System.monotonic_time(:millisecond)
    :ok = stop_supervised({mod1, group})

    start_group(mod2, group)
    second = pull_until(mod2, group, prefix, 20, timeout: 25_000)

    assert Enum.all?(second, &(&1.delivery_count == 2))
    assert System.monotonic_time(:millisecond) - started < 25_000
  end

  test "ack-only mode: consumption continues while the buffer is full" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    produce(prefix, 60)

    start_group(mod, group, max_buffered_records: 10, max_records: 5)

    pulled = pull_until(mod, group, prefix, 60, max_records: 5, timeout: 60_000)

    assert pulled |> Enum.map(&tp/1) |> Enum.uniq() |> length() == 60
    assert Enum.all?(pulled, &(&1.delivery_count == 1))
    assert [] = pull_for(mod, group, prefix, 2_000)
  end

  test "pull on a group that is not running returns empty" do
    mod = new_mod()
    define_share_group(mod)
    {group, _prefix} = new_ids()

    mod.klife_client()
    assert {:ok, :empty} = Klife.Consumer.ShareGroup.pull(MyClient, mod, group, [])
    assert :ok = Klife.Consumer.ShareGroup.acknowledge(MyClient, mod, group, [], :accept)
    assert [] = Klife.Consumer.ShareGroup.assigned_partitions(MyClient, mod, group)
  end
end
