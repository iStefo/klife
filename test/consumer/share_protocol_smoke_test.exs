defmodule Klife.Consumer.ShareProtocolSmokeTest do
  # Raw wire-level check that the KIP-932 APIs negotiated by the client work
  # against the test cluster. Higher level share group tests live elsewhere.
  use ExUnit.Case, async: false

  alias Klife.Connection.Broker
  alias Klife.MetadataCache
  alias KlifeProtocol.Messages, as: M

  @topic "test_share_topic_2"

  test "join share group, fetch and acknowledge records" do
    group = "share_smoke_#{System.unique_integer([:positive])}"

    {:ok, %{content: %{responses: [%{error_code: 0}]}}} =
      Broker.send_message(M.IncrementalAlterConfigs, MyClient, :any, %{
        resources: [
          %{
            resource_type: 32,
            resource_name: group,
            configs: [
              %{name: "share.auto.offset.reset", config_operation: 0, value: "earliest"}
            ]
          }
        ],
        validate_only: false
      })

    values = for i <- 1..5, do: "smoke_#{group}_#{i}"
    recs = for v <- values, do: %Klife.Record{value: v, topic: @topic, partition: 0}
    {:ok, _} = recs |> MyClient.produce_batch() |> Klife.Record.verify_batch()

    {:ok, %{content: %{coordinators: [%{error_code: 0, node_id: coord}]}}} =
      Broker.send_message(M.FindCoordinator, MyClient, :any, %{
        key_type: 0,
        coordinator_keys: [group]
      })

    member_id = UUID.uuid4()
    topic_id = MetadataCache.get_metadata_attribute(MyClient, @topic, 0, :topic_id)

    assignment = wait_assignment(coord, group, member_id, 0, 30)

    assert %{topic_id: ^topic_id, partitions: parts} =
             Enum.find(assignment, &(&1.topic_id == topic_id))

    assert 0 in parts

    leader = MetadataCache.get_metadata_attribute(MyClient, @topic, 0, :leader_id)

    {:ok, %{content: resp}} =
      Broker.send_message(M.ShareFetch, MyClient, leader, %{
        group_id: group,
        member_id: member_id,
        share_session_epoch: 0,
        max_wait_ms: 2_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        max_records: 500,
        batch_size: 500,
        topics: [
          %{topic_id: topic_id, partitions: [%{partition_index: 0, acknowledgement_batches: []}]}
        ],
        forgotten_topics_data: []
      })

    assert resp.error_code == 0
    [%{partitions: [pdata]}] = resp.responses
    assert pdata.error_code == 0
    assert [_ | _] = pdata.acquired_records

    fetched =
      pdata.records
      |> Enum.flat_map(&Klife.Record.parse_from_protocol(@topic, 0, &1))
      |> Enum.map(& &1.value)

    assert Enum.all?(values, &(&1 in fetched))

    acks =
      Enum.map(pdata.acquired_records, fn ar ->
        %{first_offset: ar.first_offset, last_offset: ar.last_offset, acknowledge_types: [1]}
      end)

    {:ok, %{content: ack_resp}} =
      Broker.send_message(M.ShareAcknowledge, MyClient, leader, %{
        group_id: group,
        member_id: member_id,
        share_session_epoch: -1,
        topics: [
          %{
            topic_id: topic_id,
            partitions: [%{partition_index: 0, acknowledgement_batches: acks}]
          }
        ]
      })

    assert ack_resp.error_code == 0
    assert [%{partitions: [%{error_code: 0}]}] = ack_resp.responses
  end

  defp wait_assignment(_coord, _group, _member, _epoch, 0), do: flunk("no share assignment")

  defp wait_assignment(coord, group, member_id, epoch, tries) do
    {:ok, %{content: resp}} =
      Broker.send_message(M.ShareGroupHeartbeat, MyClient, coord, %{
        group_id: group,
        member_id: member_id,
        member_epoch: epoch,
        rack_id: nil,
        subscribed_topic_names: [@topic]
      })

    assert resp.error_code == 0, inspect(resp)

    case resp.assignment do
      %{topic_partitions: [_ | _] = tps} ->
        tps

      _ ->
        Process.sleep(500)
        wait_assignment(coord, group, member_id, resp.member_epoch, tries - 1)
    end
  end
end
