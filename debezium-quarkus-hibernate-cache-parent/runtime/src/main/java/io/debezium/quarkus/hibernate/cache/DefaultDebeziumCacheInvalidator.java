/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;

public class DefaultDebeziumCacheInvalidator implements DebeziumCacheInvalidator {

    private final DebeziumEvictionStrategy evictionStrategy;
    private final DebeziumFilterStrategy filterStrategy;

    public DefaultDebeziumCacheInvalidator(DebeziumEvictionStrategy evictionStrategy,
                                           DebeziumFilterStrategy filterStrategy) {
        this.evictionStrategy = evictionStrategy;
        this.filterStrategy = filterStrategy;
    }

    @Override
    public void evaluate(CapturingEvent<SourceRecord> event) {
        if (filterStrategy.filter(event)) {
            return;
        }

        evictionStrategy.evict(InvalidationEvent.from(
                event.engine(),
                (Struct) event.record().key(),
                ((Struct) event
                        .record()
                        .value())
                        .getStruct("source")));
    }

}
