/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.Objects;

import org.apache.kafka.connect.data.Struct;

public class InvalidationEvent {
    private final String engine;
    private final String database;
    private final String schema;
    private final String table;
    private final Struct source;
    private final Struct key;

    private InvalidationEvent(String engine,
                              String database,
                              String schema,
                              String table,
                              Struct source,
                              Struct key) {
        this.engine = engine;
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.source = source;
        this.key = key;
    }

    public static InvalidationEvent from(String engine, Struct key, Struct value) {
        return new InvalidationEvent(engine, getString(value, "db", "database"),
                getString(value, "schema"),
                coalesce(getString(value, "table"), getString(value, "collection")),
                value,
                key);
    }

    private static String getString(Struct struct, String... candidates) {
        for (String candidate : candidates) {
            try {
                if (struct.schema().field(candidate) != null) {
                    Object v = struct.get(candidate);
                    if (v != null) {
                        return v.toString();
                    }
                }
            }
            catch (Exception ignore) {
                // ignore
            }
        }
        return null;
    }

    private static String coalesce(String... vals) {
        for (String v : vals) {
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    public String database() {
        return database;
    }

    public String schema() {
        return schema;
    }

    public String table() {
        return table;
    }

    public String engine() {
        return engine;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (InvalidationEvent) obj;
        return Objects.equals(this.database, that.database) &&
                Objects.equals(this.schema, that.schema) &&
                Objects.equals(this.table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, schema, table);
    }

    @Override
    public String toString() {
        return "InvalidationEvent[" +
                "database=" + database + ", " +
                "schema=" + schema + ", " +
                "table=" + table + ']';
    }

}
