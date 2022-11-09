package io.airbyte.integrations.destination.hive;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HiveSqlNameTransformerTest {

    private final HiveSqlNameTransformer transformer = new HiveSqlNameTransformer();

    @Test
    public void getTmpTableNameTest() {
        String tmpTableName = transformer.getTmpTableName("groups");
        assertTrue(tmpTableName.startsWith("airbyte_tmp_"));
        assertTrue(tmpTableName.endsWith("_groups"));
        assertTrue(tmpTableName.matches("airbyte_tmp_[a-z]{3}_groups"));
    }

    @Test
    public void getRawTableNameTest() {
        String rawTableName = transformer.getRawTableName("groups");
        assertEquals("airbyte_raw_groups", rawTableName);
    }

    @Test
    public void longNameTest() {
        String rawTableName = transformer.getRawTableName(
            "groupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroups");
        assertEquals(
            "airbyte_raw_groupsgroupsgroupsgroupsgroupsgroupsgroup__roupsgroupsgroupsgroupsgroupsgroupsgroupsgroupsgroups",
            rawTableName);
    }

}