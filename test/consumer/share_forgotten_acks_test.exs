defmodule Klife.Consumer.ShareForgottenAcksTest do
  # Empirical check (Kafka 4.2) of how the broker treats acknowledgements for
  # partitions that were forgotten from a share session. The result drives the
  # design of Klife.Consumer.ShareGroup.Session, see the comment there.
  #
  # Findings:
  #
  #  * ShareAcknowledge carrying acks for a partition that is no longer part of
  #    the session (it was removed with forgotten_topics_data) succeeds
  #    (error_code 0) and the acks ARE applied (a RELEASE ack makes the records
  #    come back with delivery_count 2 once the partition is re-added).
  #  * Listing a partition that is not in the session under `topics` of a
  #    ShareFetch, even when it only carries acks, re-adds it to the session
  #    and fetches (acquires) new records for it.
  #
  # So acks for forgotten partitions must be sent with ShareAcknowledge and
  # never piggybacked in a ShareFetch.
  use ExUnit.Case, async: false

  alias Klife.Connection.Broker
  alias Klife.MetadataCache
  alias KlifeProtocol.Messages, as: M

  @topic "test_share_topic_1"

  test "acks for forgotten partitions are accepted by ShareAcknowledge and do not re-add" do
    group = "share_forgotten_#{System.unique_integer([:positive])}"
    set_earliest(group)

    tid = MetadataCache.get_metadata_attribute(MyClient, @topic, 0, :topic_id)

    {leader, [pa, pb | _]} =
      0..3
      |> Enum.group_by(&MetadataCache.get_metadata_attribute(MyClient, @topic, &1, :leader_id))
      |> Enum.find(fn {_leader, ps} -> length(ps) >= 2 end)

    for p <- [pa, pb] do
      [{:ok, _}] =
        MyClient.produce_batch([%Klife.Record{value: "forgotten", topic: @topic, partition: p}])
    end

    {:ok, %{content: %{coordinators: [%{node_id: coord}]}}} =
      Broker.send_message(M.FindCoordinator, MyClient, :any, %{
        key_type: 0,
        coordinator_keys: [group]
      })

    member = UUID.uuid4()
    join(coord, group, member, 0, 30)

    parts = for p <- [pa, pb], do: %{partition_index: p, acknowledgement_batches: []}
    r0 = fetch(leader, group, member, 0, [%{topic_id: tid, partitions: parts}], [])
    assert r0.error_code == 0

    # Collect acquired ranges for both partitions.
    {acquired, epoch} = collect_acquired(leader, group, member, 1, r0, %{}, [pa, pb])
    assert [_ | _] = acquired[pa]
    assert [_ | _] = acquired[pb]

    # Forget pb, then RELEASE its records through ShareAcknowledge.
    r = fetch(leader, group, member, epoch, [], [%{topic_id: tid, partitions: [pb]}])
    assert r.error_code == 0

    {:ok, %{content: ack_resp}} =
      Broker.send_message(M.ShareAcknowledge, MyClient, leader, %{
        group_id: group,
        member_id: member,
        share_session_epoch: epoch + 1,
        topics: [
          %{
            topic_id: tid,
            partitions: [%{partition_index: pb, acknowledgement_batches: release(acquired[pb])}]
          }
        ]
      })

    assert ack_resp.error_code == 0
    assert [%{partitions: [%{partition_index: ^pb, error_code: 0}]}] = ack_resp.responses

    # pb is still out of the session: the release must not have re-added it.
    r = fetch(leader, group, member, epoch + 2, [], [])
    assert Enum.all?(partition_responses(r), &(&1.partition_index != pb))

    # Re-add pb: the released records come back with delivery_count 2, which
    # proves the ack for the forgotten partition was applied.
    r =
      fetch(
        leader,
        group,
        member,
        epoch + 3,
        [%{topic_id: tid, partitions: [%{partition_index: pb, acknowledgement_batches: []}]}],
        []
      )

    assert r.error_code == 0

    {redelivered, _} = collect_acquired(leader, group, member, epoch + 4, r, %{}, [pb])
    assert Enum.all?(redelivered[pb], &(&1.delivery_count == 2))

    # Close the session, releasing everything still held.
    fetch(leader, group, member, -1, [], [])
  end

  defp collect_acquired(leader, group, member, epoch, resp, acc, wanted) do
    acc =
      for pd <- partition_responses(resp), reduce: acc do
        acc ->
          Map.update(acc, pd.partition_index, pd.acquired_records, &(&1 ++ pd.acquired_records))
      end

    if Enum.all?(wanted, &(acc[&1] not in [nil, []])) do
      {acc, epoch}
    else
      Process.sleep(100)

      collect_acquired(
        leader,
        group,
        member,
        epoch + 1,
        fetch(leader, group, member, epoch, [], []),
        acc,
        wanted
      )
    end
  end

  defp partition_responses(resp), do: for(t <- resp.responses, p <- t.partitions, do: p)

  defp release(ranges),
    do:
      for(
        ar <- ranges,
        do: %{first_offset: ar.first_offset, last_offset: ar.last_offset, acknowledge_types: [2]}
      )

  defp fetch(leader, group, member, epoch, topics, forgotten) do
    {:ok, %{content: r}} =
      Broker.send_message(M.ShareFetch, MyClient, leader, %{
        group_id: group,
        member_id: member,
        share_session_epoch: epoch,
        max_wait_ms: 1000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        max_records: 500,
        batch_size: 500,
        topics: topics,
        forgotten_topics_data: forgotten
      })

    r
  end

  defp set_earliest(group) do
    {:ok, %{content: %{responses: [%{error_code: 0}]}}} =
      Broker.send_message(M.IncrementalAlterConfigs, MyClient, :any, %{
        resources: [
          %{
            resource_type: 32,
            resource_name: group,
            configs: [%{name: "share.auto.offset.reset", config_operation: 0, value: "earliest"}]
          }
        ],
        validate_only: false
      })
  end

  defp join(_coord, _group, _member, _epoch, 0), do: flunk("no share assignment")

  defp join(coord, group, member, epoch, tries) do
    {:ok, %{content: resp}} =
      Broker.send_message(M.ShareGroupHeartbeat, MyClient, coord, %{
        group_id: group,
        member_id: member,
        member_epoch: epoch,
        rack_id: nil,
        subscribed_topic_names: [@topic]
      })

    assert resp.error_code == 0, inspect(resp)

    case resp.assignment do
      %{topic_partitions: [_ | _]} = a -> a
      _ -> Process.sleep(500) && join(coord, group, member, resp.member_epoch, tries - 1)
    end
  end
end
