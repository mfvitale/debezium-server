/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Test profile for JDBC sink integration tests.
 */
public class JdbcTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<>();

        // Use JSON format for testing
        config.put("debezium.format.key", "json");
        config.put("debezium.format.value", "json");

        return config;
    }

    @Override
    public String getConfigProfile() {
        return "jdbc";
    }
}
