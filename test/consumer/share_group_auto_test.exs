defmodule Klife.Consumer.ShareGroupAutoTest do
  use ExUnit.Case, async: false

  alias Klife.Record

  @topic "test_share_topic_1"
  @partitions [0, 1, 2, 3]

  # The callback of every test module delegates to a function registered per
  # group, so each test defines its own behaviour inline.
  defmodule AutoGroup do
    defmacro __using__(_opts) do
      quote do
        use Klife.Consumer.ShareGroup, client: MyClient

        @impl true
        def handle_record_batch(group_name, records) do
          fun = :persistent_term.get({Klife.Consumer.ShareGroupAutoTest, group_name})
          fun.(records)
        end
      end
    end
  end

  defp new_mod(), do: :"ShareGroupAutoTest#{:rand.bytes(8) |> Base.encode16()}"

  defp new_ids() do
    id = "#{System.unique_integer([:positive])}_#{System.os_time(:millisecond)}"
    {"share_auto_test_#{id}", "sga#{id}"}
  end

  defp define_auto_mod() do
    mod = new_mod()

    Module.create(
      mod,
      quote do
        use Klife.Consumer.ShareGroupAutoTest.AutoGroup
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp define_manual_mod() do
    mod = new_mod()

    Module.create(
      mod,
      quote do
        use Klife.Consumer.ShareGroup, client: MyClient
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp set_handler(group, fun) do
    :persistent_term.put({__MODULE__, group}, fun)
    on_exit(fn -> :persistent_term.erase({__MODULE__, group}) end)
  end

  defp start_group(mod, group, opts \\ []) do
    args =
      [group_name: group, topics: [[name: @topic]], share_auto_offset_reset: :earliest]
      |> Keyword.merge(opts)

    start_supervised!({mod, args}, id: {mod, group}, restart: :temporary)
  end

  defp produce(prefix, count, partitions \\ @partitions) do
    recs =
      for i <- 1..count do
        %Record{
          topic: @topic,
          partition: Enum.at(partitions, rem(i, length(partitions))),
          value: "#{prefix}_#{i}"
        }
      end

    {:ok, produced} = recs |> MyClient.produce_batch() |> Record.verify_batch()
    produced
  end

  defp own?(%Record{value: value}, prefix), do: String.starts_with?(value, prefix <> "_")

  # Handler which reports every own record to the test process (with the pid
  # of the calling worker) and acks them with `ack_fun.(record)`.
  defp reporting_handler(prefix, ack_fun) do
    test_pid = self()

    fn records ->
      Enum.map(records, fn rec ->
        if own?(rec, prefix) do
          send(test_pid, {:handled, self(), rec})
          {ack_fun.(rec), rec}
        else
          {:accept, rec}
        end
      end)
    end
  end

  defp collect(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_collect(deadline, [])
  end

  defp do_collect(deadline, acc) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:handled, pid, rec} -> do_collect(deadline, [{pid, rec} | acc])
    after
      remaining -> Enum.reverse(acc)
    end
  end

  defp collect_until(count, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_collect_until(count, deadline, [])
  end

  defp do_collect_until(count, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    cond do
      length(acc) >= count ->
        Enum.reverse(acc)

      remaining <= 0 ->
        flunk("timeout: got #{length(acc)} of #{count} expected handled records")

      true ->
        receive do
          {:handled, pid, rec} -> do_collect_until(count, deadline, [{pid, rec} | acc])
        after
          remaining -> do_collect_until(count, deadline, acc)
        end
    end
  end

  test "auto mode consumes all records with :accept and never redelivers them" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produced = produce(prefix, 40)
    set_handler(group, reporting_handler(prefix, fn _ -> :accept end))

    start_group(mod, group)

    handled = collect_until(40, 30_000)
    assert [{pid, _} | _] = handled
    assert is_pid(pid)

    # nothing comes back
    handled = handled ++ collect(3_000)
    recs = Enum.map(handled, fn {_, r} -> r end)

    assert length(recs) == 40
    assert Enum.all?(recs, &(&1.delivery_count == 1))

    assert Enum.sort(Enum.map(recs, &{&1.partition, &1.offset, &1.value})) ==
             Enum.sort(Enum.map(produced, &{&1.partition, &1.offset, &1.value}))

    assert Enum.sort(mod.assigned_partitions(group)) == Enum.map(@partitions, &{@topic, &1})
  end

  test "handler_concurrency runs the callback in several workers" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produce(prefix, 60)
    test_pid = self()

    set_handler(group, fn records ->
      # only own records are slow, the topic holds lots of history of other tests
      if Enum.any?(records, &own?(&1, prefix)), do: Process.sleep(100)

      Enum.map(records, fn rec ->
        if own?(rec, prefix), do: send(test_pid, {:handled, self(), rec})
        {:accept, rec}
      end)
    end)

    start_group(mod, group, handler_concurrency: 4, handler_max_batch_size: 10)

    handled = collect_until(60, 60_000)
    handled = handled ++ collect(2_000)

    assert length(handled) == 60

    assert handled
           |> Enum.map(fn {_, r} -> {r.partition, r.offset} end)
           |> Enum.uniq()
           |> length() == 60

    assert handled |> Enum.map(fn {pid, _} -> pid end) |> Enum.uniq() |> length() > 1
  end

  test "per-record results: released records come back, accepted ones do not" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produce(prefix, 30)

    release_first? = fn rec ->
      rem(index(rec, prefix), 3) == 0 and rec.delivery_count == 1
    end

    set_handler(
      group,
      reporting_handler(prefix, fn rec -> if release_first?.(rec), do: :release, else: :accept end)
    )

    start_group(mod, group)

    # 30 first deliveries + 10 redeliveries
    handled = collect_until(40, 30_000)
    handled = handled ++ collect(3_000)
    recs = Enum.map(handled, fn {_, r} -> r end)

    assert length(recs) == 40

    by_index = Enum.group_by(recs, &index(&1, prefix), & &1.delivery_count)

    for i <- 1..30 do
      counts = by_index |> Map.fetch!(i) |> Enum.sort()
      if rem(i, 3) == 0, do: assert(counts == [1, 2]), else: assert(counts == [1])
    end
  end

  test "records are missing from a list result are released" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produce(prefix, 10, [0])
    test_pid = self()

    set_handler(group, fn records ->
      own = Enum.filter(records, &own?(&1, prefix))
      Enum.each(own, &send(test_pid, {:handled, self(), &1}))

      # only ack the first own record on first delivery, the others are omitted
      case own do
        [first | _] when first.delivery_count == 1 ->
          [{:accept, first}] ++ Enum.map(records -- own, &{:accept, &1})

        _ ->
          Enum.map(records, &{:accept, &1})
      end
    end)

    start_group(mod, group)

    handled = collect_until(10, 30_000)
    # the 9 omitted records are redelivered
    handled = handled ++ collect(4_000)
    counts = handled |> Enum.map(fn {_, r} -> r.delivery_count end) |> Enum.frequencies()
    assert counts[1] == 10
    assert counts[2] == 9
  end

  test "a raising callback releases the batch, the worker survives" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produce(prefix, 20)
    test_pid = self()

    set_handler(group, fn records ->
      own = Enum.filter(records, &own?(&1, prefix))
      Enum.each(own, &send(test_pid, {:handled, self(), &1}))

      if Enum.any?(own, fn r -> r.delivery_count == 1 and rem(index(r, prefix), 5) == 0 end) do
        raise "boom"
      end

      Enum.map(records, &{:accept, &1})
    end)

    ExUnit.CaptureLog.capture_log(fn ->
      start_group(mod, group)

      # every record is eventually accepted after at least one redelivery of the
      # failing ones
      handled = collect(10_000)

      failed = for {_, r} <- handled, r.delivery_count == 2, do: index(r, prefix)
      assert Enum.all?([5, 10, 15, 20], &(&1 in failed))

      # no record gets past delivery count 2 in the end
      assert Enum.all?(handled, fn {_, r} -> r.delivery_count <= 2 end)

      # the worker survived the raise and handled the redelivery
      pids = handled |> Enum.map(fn {pid, _} -> pid end) |> Enum.uniq()
      assert Enum.all?(pids, &Process.alive?/1)
      assert length(pids) == 1
    end)
  end

  test "auto mode without handle_record_batch/2 raises" do
    {group, _prefix} = new_ids()
    mod = define_manual_mod()

    assert_raise ArgumentError, ~r/handle_record_batch\/2/, fn ->
      mod.start_link(
        group_name: group,
        topics: [[name: @topic]],
        mode: :auto
      )
    end

    # not setting the mode falls back to :manual
    pid = start_group(mod, group)
    assert is_pid(pid)
  end

  test "stopping the group lets the running batch finish and be acknowledged" do
    {group, prefix} = new_ids()
    mod = define_auto_mod()
    produce(prefix, 5, [0])
    test_pid = self()

    set_handler(group, fn records ->
      own = Enum.filter(records, &own?(&1, prefix))

      if own != [] do
        send(test_pid, {:started, self(), length(own)})
        Process.sleep(2_000)
        Enum.each(own, &send(test_pid, {:handled, self(), &1}))
      end

      Enum.map(records, &{:accept, &1})
    end)

    start_group(mod, group)

    assert_receive {:started, _pid, _n}, 30_000

    :ok = stop_supervised({mod, group})

    handled = collect(0)
    assert handled != []

    # a second member of the same group does not get the handled records
    second = define_manual_mod()
    start_group(second, group)
    seen = pull_all(second, group, 5_000)
    handled_values = MapSet.new(handled, fn {_, r} -> r.value end)
    refute Enum.any?(seen, &MapSet.member?(handled_values, &1.value))
  end

  defp pull_all(mod, group, ms) do
    deadline = System.monotonic_time(:millisecond) + ms
    do_pull_all(mod, group, deadline, [])
  end

  defp do_pull_all(mod, group, deadline, acc) do
    if System.monotonic_time(:millisecond) >= deadline do
      acc
    else
      case mod.pull(group, max_records: 100) do
        {:ok, :empty} ->
          Process.sleep(50)
          do_pull_all(mod, group, deadline, acc)

        {:ok, recs} ->
          mod.acknowledge(group, recs, :accept)
          do_pull_all(mod, group, deadline, acc ++ recs)
      end
    end
  end

  defp index(%Record{value: value}, prefix) do
    value |> String.replace_prefix(prefix <> "_", "") |> String.to_integer()
  end
end
