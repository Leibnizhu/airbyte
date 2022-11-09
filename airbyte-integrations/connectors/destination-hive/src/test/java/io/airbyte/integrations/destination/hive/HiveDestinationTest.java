package io.airbyte.integrations.destination.hive;

import org.junit.jupiter.api.Test;

public class HiveDestinationTest {

    @Test
    public void singleStreamTest() throws Exception {
        /*HiveDestination destination = new HiveDestination();
        String USERS_STREAM_NAME = "users";
        ConfiguredAirbyteCatalog CATALOG = new ConfiguredAirbyteCatalog().withStreams(List.of(
            CatalogHelpers.createConfiguredAirbyteStream(USERS_STREAM_NAME,
                null,
                Field.of("id", JsonSchemaType.STRING),
                Field.of("name", JsonSchemaType.STRING)
            )));
        JsonNode config = Jsons.deserialize(
            "{\"host\" : \"localhost\",\"port\" : 10000,\"database\" : \"test_airbyte\"}");
        AirbyteMessageConsumer consumer = destination.getConsumer(config,
            CATALOG,
            Destination::defaultOutputRecordCollector);

        consumer.start();
        Instant NOW = Instant.now();
        consumer.accept(new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream(USERS_STREAM_NAME)
                .withData(Jsons.jsonNode(Map.of("name", "john","id", "10")))
                .withEmittedAt(NOW.toEpochMilli())));
        consumer.accept(new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream(USERS_STREAM_NAME)
                .withData(Jsons.jsonNode(Map.of("name", "susan","id", "30")))
                .withEmittedAt(NOW.toEpochMilli())));
        consumer.accept(new AirbyteMessage().withType(AirbyteMessage.Type.STATE)
            .withState(new AirbyteStateMessage().withData(Jsons.jsonNode(Map.of("checkpoint", "now!")))));
        consumer.close();*/
    }
}