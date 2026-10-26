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

  defp session_states(mod, group) do
    ms = [{{{Klife.Consumer.ShareGroup.Session, :_, mod, group, :_}, :"$1", :_}, [], [:"$1"]}]

    Klife.ProcessRegistry
    |> Registry.select(ms)
    |> Enum.map(&:sys.get_state/1)
    |> tap(fn states -> assert states != [], "no session processes found" end)
  end

  # With a small buffer the session stops fetching once it is full, so the
  # buffered records are NOT re-acquired and really age past the lock.
  @tag :buffer_variant
  test "stale records in a full buffer are dropped, not handed out" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    :ok = set_group_config(group, "share.record.lock.duration.ms", "2000")

    produce(prefix, 40)
    start_group(mod, group, max_buffered_records: 10, max_records: 10)

    Process.sleep(4_500)

    # The group starts at the earliest offset, so every session holds a full
    # buffer of (mostly foreign) records acquired >4s ago. The broker released
    # them after 2s, a pull must not hand out any of them.
    assert {:ok, [_ | _] = first} = mod.pull(group, max_records: 100)
    pulled_at = now()

    states = session_states(mod, group)

    for rec <- first do
      acquired_at =
        Enum.find_value(states, fn st ->
          Enum.find_value(st.outstanding, fn {{_tid, p, o}, {dc, _owner, at}} ->
            if p == rec.partition and o == rec.offset and dc == rec.delivery_count, do: at
          end)
        end)

      assert acquired_at, "pulled record has no outstanding acquisition"
      assert pulled_at - acquired_at < 2_000, "handed out a record whose lock expired"
    end

    :ok = mod.acknowledge(group, first, :accept)
    own_first = Enum.filter(first, &own?(&1, prefix))

    rest =
      pull_until(mod, group, prefix, 40 - length(own_first), timeout: 30_000, max_records: 10)

    pulled = own_first ++ rest

    keys = Enum.map(pulled, &tp/1)
    assert length(keys) == length(Enum.uniq(keys)), "same offset handed out twice"
    assert length(keys) == 40

    # Everything was accepted in time: nothing may come back.
    assert [] = pull_for(mod, group, prefix, 4_500)
  end

  test "records buffered longer than the acquisition lock are never handed out" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()

    :ok = set_group_config(group, "share.record.lock.duration.ms", "2000")

    produce(prefix, 40)
    start_group(mod, group)

    # Let the session buffer everything and keep the buffered records waiting
    # for longer than the lock. The broker releases and redelivers them meanwhile
    # (the fetch loop continues), so offsets get acquired again with a higher
    # delivery count while an older copy may still sit in the buffer.
    Process.sleep(4_500)

    # Never acknowledge a record that was not pulled in the last instant: an
    # expired acquisition would fail its ack and come back later.
    pulled = pull_until(mod, group, prefix, 40, timeout: 30_000)

    offsets = Enum.map(pulled, &tp/1)
    assert length(offsets) == length(Enum.uniq(offsets)), "same offset handed out twice"

    # Accepted in time, so nothing may come back (waits longer than one lock).
    assert [] = pull_for(mod, group, prefix, 4_500)

    for state <- session_states(mod, group) do
      assert :queue.len(state.buffer) == state.buffered_count
      assert map_size(state.outstanding) == state.buffered_count
    end
  end

  test "buffered and pulled-but-never-acked acquisitions are pruned from the session state" do
    mod = new_mod()
    define_share_group(mod)
    {group, prefix} = new_ids()
    topic = create_topic(1)

    :ok = set_group_config(group, "share.record.lock.duration.ms", "2000")

    produce(prefix, 20, topic: topic, partitions: [0])

    # Small buffer: the session stops fetching while it is full, so pulled
    # acquisitions are not replaced by redeliveries and only pruning removes them.
    start_group(mod, group, topic: topic, max_buffered_records: 5, max_records: 5)

    first =
      pull_until(mod, group, prefix, 20,
        ack_fun: fn _ -> nil end,
        max_records: 5,
        timeout: 40_000
      )

    assert first |> Enum.map(&tp/1) |> Enum.uniq() |> length() == 20

    # Without pruning the pulled acquisitions (and the owner monitor) would
    # stay in `outstanding` forever, their lock is long gone after 2s.
    Process.sleep(5_000)

    for state <- session_states(mod, group) do
      assert state.owners == %{}
      assert Enum.all?(state.outstanding, fn {_k, {_dc, owner, _at}} -> owner == nil end)
      assert map_size(state.outstanding) == state.buffered_count
    end
  end

  ## Fault paths

  describe "fault paths" do
    test "leader change: consumption continues and every record is consumed" do
      mod = new_mod()
      define_share_group(mod)
      {group, prefix} = new_ids()
      topic = create_topic(2)

      :ok = set_group_config(group, "share.record.lock.duration.ms", "5000")

      produce(prefix, 40, topic: topic, partitions: [0, 1])
      start_group(mod, group, topic: topic)

      first = pull_until(mod, group, prefix, 20)
      leaders_before = topic_leaders(topic)

      rotate_leaders(topic)
      leaders_after = topic_leaders(topic)
      refute leaders_before == leaders_after

      produce(prefix, 40, topic: topic, partitions: [0, 1], from: 41)

      # Acks sent to the old leader may be lost (the broker redelivers after the
      # lock), so duplicates are possible but only with a higher delivery count.
      rest = pull_until(mod, group, prefix, 80 - 20, timeout: 60_000)
      assert_distinct_acquisitions(first ++ rest)

      values = (first ++ rest) |> Enum.map(& &1.value) |> Enum.uniq()
      assert length(values) == 80
    end

    test "session reset: a replaced share session is detected and recovered from" do
      mod = new_mod()
      define_share_group(mod)
      {group, prefix} = new_ids()
      topic = create_topic(2)

      :ok = set_group_config(group, "share.record.lock.duration.ms", "3000")

      produce(prefix, 30, topic: topic, partitions: [0, 1])
      start_group(mod, group, topic: topic)

      # Held (not acknowledged) records of the session that is going to die.
      held = pull_until(mod, group, prefix, 10, ack_fun: fn _ -> nil end)

      sessions_before = session_pids(mod, group)
      [%{member_id: member_id} | _] = Enum.map(sessions_before, &:sys.get_state/1)

      log =
        capture_errors(fn ->
          # Opening a new session for the same member replaces the old one on
          # the broker, so the next request of ours carries a stale epoch.
          for pid <- sessions_before do
            %{broker_id: broker_id} = :sys.get_state(pid)
            replace_share_session(broker_id, group, member_id)
          end

          # The stale epoch is only noticed with the next request of the session.
          produce(prefix, 30, topic: topic, partitions: [0, 1], from: 31)
          Process.sleep(1_500)
        end)

      assert log =~ "resetting share session"

      # Acks for records of the dead session are dropped as stale, the broker
      # redelivers those after the lock.
      :ok = mod.acknowledge(group, held, :accept)

      pulled = pull_until(mod, group, prefix, 60, timeout: 60_000)
      assert_distinct_acquisitions(held ++ pulled)
      assert (held ++ pulled) |> Enum.map(& &1.value) |> Enum.uniq() |> length() == 60

      # The same session processes recovered, no crash and restart involved.
      assert Enum.sort(session_pids(mod, group)) == Enum.sort(sessions_before)
    end

    test "transactions: control records are never delivered and the partition keeps progressing" do
      mod_uc = new_mod()
      mod_c = new_mod()
      define_share_group(mod_uc)
      define_share_group(mod_c)
      {group_uc, prefix} = new_ids()
      group_c = group_uc <> "_c"
      topic = create_topic(1)

      :ok = set_group_config(group_uc, "share.record.lock.duration.ms", "3000")
      :ok = set_group_config(group_c, "share.record.lock.duration.ms", "3000")
      :ok = set_group_config(group_c, "share.isolation.level", "read_committed")

      txn = fn label, abort? ->
        recs =
          for i <- 1..3, do: %Record{topic: topic, partition: 0, value: "#{prefix}_#{label}_#{i}"}

        result =
          MyClient.transaction(
            fn ->
              assert [_, _, _] = MyClient.produce_batch(recs)
              if abort?, do: raise("abort #{label}"), else: {:ok, :done}
            end,
            pool_name: :my_test_pool_1
          )

        if abort?,
          do: assert({:error, %RuntimeError{}} = result),
          else: assert({:ok, :done} = result)
      end

      txn.("c1", false)
      txn.("a", true)
      txn.("c2", false)

      start_group(mod_uc, group_uc, topic: topic)
      start_group(mod_c, group_c, topic: topic)

      read_committed = pull_until(mod_c, group_c, prefix, 6)
      values_c = Enum.map(read_committed, & &1.value)

      assert Enum.sort(values_c) ==
               Enum.sort(for l <- ["c1", "c2"], i <- 1..3, do: "#{prefix}_#{l}_#{i}")

      # The default isolation level (read_uncommitted) also hands out the
      # records of the aborted transaction, but never a control record.
      read_uncommitted = pull_until(mod_uc, group_uc, prefix, 9)

      assert read_uncommitted |> Enum.map(& &1.value) |> Enum.sort() ==
               Enum.sort(for l <- ["c1", "a", "c2"], i <- 1..3, do: "#{prefix}_#{l}_#{i}")

      for rec <- read_committed ++ read_uncommitted do
        assert rec.batch_attributes.is_control_batch == false
        assert rec.delivery_count == 1
      end

      # Control records (commit/abort markers) were acknowledged as GAP: the
      # partition progresses and nothing is redelivered after the lock.
      produce(prefix, 3, topic: topic, partitions: [0], from: 100)
      assert [_, _, _] = pull_until(mod_c, group_c, prefix, 3, timeout: 20_000)
      assert [_, _, _] = pull_until(mod_uc, group_uc, prefix, 3, timeout: 20_000)

      assert [] = pull_for(mod_c, group_c, prefix, 4_500)
      assert [] = pull_for(mod_uc, group_uc, prefix, 100)
    end

    test "member removed on the coordinator: the group rejoins and keeps consuming" do
      mod = new_mod()
      define_share_group(mod)
      {group, prefix} = new_ids()
      topic = create_topic(2)

      produce(prefix, 10, topic: topic, partitions: [0, 1])
      pid = start_group(mod, group, topic: topic)

      assert [_ | _] = pull_until(mod, group, prefix, 10)

      %{member_id: member_id, coordinator_id: coordinator_id} = :sys.get_state(pid)

      # Leaving behind the back of the group process: its next heartbeat gets
      # UNKNOWN_MEMBER_ID (or FENCED_MEMBER_EPOCH) and must rejoin.
      assert {:ok, %{content: %{error_code: 0}}} =
               Broker.send_message(M.ShareGroupHeartbeat, MyClient, coordinator_id, %{
                 group_id: group,
                 member_id: member_id,
                 member_epoch: -1,
                 rack_id: nil,
                 subscribed_topic_names: nil
               })

      # The broker heartbeat interval is ~5s, the group notices the removal then.
      log = capture_errors(fn -> Process.sleep(7_000) end)

      produce(prefix, 10, topic: topic, partitions: [0, 1], from: 11)
      assert [_ | _] = pull_until(mod, group, prefix, 10, timeout: 60_000)

      assert log =~ "error_code=25" or log =~ "error_code=110"
      assert Process.alive?(pid)
      assert length(wait_for_assignment(mod, group, 2)) == 2
    end
  end

  defp wait_for_assignment(mod, group, count) do
    deadline = now() + 30_000

    Stream.repeatedly(fn ->
      Process.sleep(50)
      mod.assigned_partitions(group)
    end)
    |> Enum.find(fn assigned ->
      length(assigned) >= count or (now() > deadline and flunk("no assignment"))
    end)
  end

  # The test env disables logging globally, enable it just to inspect errors.
  defp capture_errors(fun) do
    previous = Logger.level()
    Logger.configure(level: :error)

    try do
      ExUnit.CaptureLog.capture_log(fun)
    after
      Logger.configure(level: previous)
    end
  end

  # No two pulled records may share the same acquisition (offset + delivery count).
  defp assert_distinct_acquisitions(records) do
    keys = Enum.map(records, &{tp(&1), &1.delivery_count})
    assert length(keys) == length(Enum.uniq(keys)), "an acquisition was handed out twice"
  end

  defp session_pids(mod, group) do
    ms = [{{{Klife.Consumer.ShareGroup.Session, :_, mod, group, :_}, :"$1", :_}, [], [:"$1"]}]
    Registry.select(Klife.ProcessRegistry, ms)
  end

  defp replace_share_session(broker_id, group, member_id, attempts \\ 10) do
    result =
      Broker.send_message(M.ShareFetch, MyClient, broker_id, %{
        group_id: group,
        member_id: member_id,
        share_session_epoch: 0,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1_000_000,
        max_records: 1,
        batch_size: 1,
        topics: [],
        forgotten_topics_data: []
      })

    case result do
      {:ok, %{content: %{error_code: 0}}} ->
        :ok

      other when attempts > 0 ->
        Process.sleep(200)
        _ = other
        replace_share_session(broker_id, group, member_id, attempts - 1)

      other ->
        flunk("could not replace the share session: #{inspect(other)}")
    end
  end

  ## Dedicated topics and admin helpers (non disruptive)

  @kafka_container "compose_files-kafka1-1"
  @kafka_bootstrap "kafka1:19091"

  defp create_topic(partitions) do
    name = "share_rob_topic_#{System.unique_integer([:positive])}_#{System.os_time(:millisecond)}"

    {:ok, %{content: %{topics: [%{error_code: 0}]}}} =
      Broker.send_message(M.CreateTopics, MyClient, :controller, %{
        topics: [
          %{
            name: name,
            num_partitions: partitions,
            replication_factor: 3,
            assignments: [],
            configs: []
          }
        ],
        timeout_ms: 15_000,
        validate_only: false
      })

    wait_until(30_000, "metadata for #{name}", fn ->
      Klife.MetadataCache.trigger_check_metadata(MyClient)
      Enum.all?(0..(partitions - 1), &Klife.MetadataCache.metadata_exists?(MyClient, name, &1))
    end)

    name
  end

  defp wait_until(timeout, what, fun) do
    deadline = now() + timeout

    Stream.repeatedly(fn ->
      Process.sleep(200)
      fun.()
    end)
    |> Enum.find(fn ok? ->
      ok? or (now() > deadline and flunk("timed out waiting for #{what}"))
    end)
  end

  defp kafka_cmd(script, args) do
    {out, status} =
      System.cmd(
        "docker",
        [
          "exec",
          @kafka_container,
          "/opt/kafka/bin/#{script}",
          "--bootstrap-server",
          @kafka_bootstrap
        ] ++
          args,
        stderr_to_stdout: true
      )

    {status, out}
  end

  # %{partition => {leader, replicas}}
  defp topic_leaders(topic) do
    {0, out} = kafka_cmd("kafka-topics.sh", ["--describe", "--topic", topic])

    ~r/Partition: (\d+)\s+Leader: (\d+)\s+Replicas: ([\d,]+)/
    |> Regex.scan(out)
    |> Map.new(fn [_, p, l, r] ->
      {String.to_integer(p),
       {String.to_integer(l), r |> String.split(",") |> Enum.map(&String.to_integer/1)}}
    end)
  end

  # Moves leadership of every partition to the next replica: reorders the
  # replica list (same replica set, so no data moves) and runs a preferred
  # leader election, both on the dedicated test topic only.
  defp rotate_leaders(topic) do
    leaders = topic_leaders(topic)

    entries =
      Enum.map_join(leaders, ",", fn {p, {_leader, [first | rest]}} ->
        replicas = rest ++ [first]
        any = Enum.map_join(replicas, ",", fn _ -> ~s("any") end)

        ~s({"topic":"#{topic}","partition":#{p},"replicas":[#{Enum.join(replicas, ",")}],"log_dirs":[#{any}]})
      end)

    json = ~s({"version":1,"partitions":[#{entries}]})
    path = "/tmp/#{topic}.json"

    {_, 0} =
      System.cmd("docker", ["exec", @kafka_container, "sh", "-c", "echo '#{json}' > #{path}"])

    {0, _} =
      kafka_cmd("kafka-reassign-partitions.sh", ["--execute", "--reassignment-json-file", path])

    wait_until(60_000, "reassignment to complete", fn ->
      {_, out} =
        kafka_cmd("kafka-reassign-partitions.sh", ["--verify", "--reassignment-json-file", path])

      not String.contains?(out, "is still in progress")
    end)

    for p <- Map.keys(leaders) do
      kafka_cmd("kafka-leader-election.sh", [
        "--election-type",
        "preferred",
        "--topic",
        topic,
        "--partition",
        "#{p}"
      ])
    end

    wait_until(30_000, "leaders to move", fn ->
      new = topic_leaders(topic)
      Enum.all?(leaders, fn {p, {old_leader, _}} -> elem(new[p], 0) != old_leader end)
    end)
  end
end
