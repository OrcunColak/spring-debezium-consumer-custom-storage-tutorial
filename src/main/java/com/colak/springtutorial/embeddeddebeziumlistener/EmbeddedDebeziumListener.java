package com.colak.springtutorial.embeddeddebeziumlistener;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Slf4j
@Component
public class EmbeddedDebeziumListener {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // Beginning with the 2.6.0 release, Debezium provides two implementations of the DebeziumEngine interface.
    // The older EmbeddedEngine implementation runs a single connector that uses only one task. The connector emits all records sequentially.
    // This the default implementation.

    // Beginning with the 2.6.0 release, a new AsyncEmbeddedEngine implementation is available.
    // This implementation also runs only a single connector, but it can process records in multiple threads, and run multiple tasks,
    // if the connector supports it (currently only the connectors for SQL Server and MongoDB support running multiple tasks within a single connector).
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;

    public EmbeddedDebeziumListener(Configuration employeeConfiguration) {

        Properties properties = employeeConfiguration.asProperties();
        // the output value is change event wrapping Kafka Connect’s SourceRecord
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(properties)
                .notifying(this::handleChangeEvent)
                .build();
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> recordChangeEvent) {
        SourceRecord sourceRecord = recordChangeEvent.record();

        // log.info("Key= {}, value= {}, topic= {}", sourceRecord.key(), sourceRecord.value(), sourceRecord.topic());

        if (isInternalMessage(sourceRecord)) {
            return;
        }

        processStruct(sourceRecord);
    }

    private boolean isInternalMessage(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            return true;
        }

        // e.g. for heart beat topic= __debezium-heartbeat.employee_topic1
        if (sourceRecord.topic().startsWith("__debezium")) {
            return true;
        }

        // Alternative
        Struct sourceRecordValue = (Struct) sourceRecord.value();
        String sourceSchemaName = sourceRecordValue.schema().name();
        return sourceSchemaName.startsWith("io.debezium");
    }

    private static void processStruct(SourceRecord sourceRecord) {

        Struct sourceRecordValue = (Struct) sourceRecord.value();

        if (sourceRecordValue != null) {
            String sourceSchemaName = sourceRecordValue.schema().name();
            log.info(" {}", sourceSchemaName);

            Envelope.Operation operation = Envelope.Operation.forCode((String) sourceRecordValue.get(Envelope.FieldName.OPERATION));

            if (operation == Envelope.Operation.READ) {
                processReadOperation(sourceRecord, operation);
            }
            // Handle Create/Update/Delete func
            else {
                processUpsertOperation(operation, sourceRecordValue);
            }
        }
    }

    private static void processUpsertOperation(Envelope.Operation operation, Struct sourceRecordValue) {
        // Handling Update & Insert operations.

        final Struct struct;
        if (operation == Envelope.Operation.DELETE) {
            struct = (Struct) sourceRecordValue.get(Envelope.FieldName.BEFORE);
        } else {
            struct = (Struct) sourceRecordValue.get(Envelope.FieldName.AFTER);
        }

        Map<String, Object> payload = struct.schema().fields().stream()
                .map(Field::name)
                .filter(fieldName -> struct.get(fieldName) != null)
                .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        log.info("Updated Data: {} with Operation: {}", payload, operation.name());
    }

    private static void processReadOperation(SourceRecord sourceRecord, Envelope.Operation operation) {
        // Extract the payload for the READ operation
        Struct value = (Struct) sourceRecord.value();

        // `AFTER` contains the full row for READ
        Struct after = value.getStruct(Envelope.FieldName.AFTER);

        if (after != null) {
            // Get the schema for the 'after' data
            Schema schema = after.schema();

            Map<String, Object> payload = after.schema().fields().stream()
                    .map(Field::name)
                    .filter(fieldName -> after.get(fieldName) != null)
                    .map(fieldName -> Pair.of(fieldName, after.get(fieldName)))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            log.info("Read Data: {} for Schema : {} with Operation: {}", payload, schema, operation.name());
        }
    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy
    private void stop() throws IOException {
        if (this.debeziumEngine != null) {
            // stop the engine safely and gracefully
            // The engine’s connector will stop reading information from the source system, forward all remaining change events to your handler function,
            // and flush the latest offets to offset storage. Only after all of this completes will the engine’s run() method return.
            this.debeziumEngine.close();
        }
        try {
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("Waiting another 5 seconds for the embedded engine to shut down");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
