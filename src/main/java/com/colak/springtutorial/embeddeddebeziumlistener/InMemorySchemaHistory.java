package com.colak.springtutorial.embeddeddebeziumlistener;

import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistoryException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class InMemorySchemaHistory extends AbstractSchemaHistory {

    // List to store the schema history records in memory
    private final List<HistoryRecord> historyRecords = new ArrayList<>();

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        // Add the history record to the in-memory list
        historyRecords.add(record);
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) throws SchemaHistoryException {
        // Pass each record in memory to the consumer
        for (HistoryRecord record : historyRecords) {
            records.accept(record);
        }
    }

    @Override
    public void initializeStorage() {
        // No initialization needed for in-memory storage
    }

    @Override
    public void start() {
        // No startup logic needed for in-memory storage
    }

    @Override
    public void stop() {
        // No cleanup needed for in-memory storage
    }

    @Override
    public boolean exists() {
        // The in-memory history always "exists" as long as the application is running
        return true;
    }

    @Override
    public boolean storageExists() {
        // The storage always "exists" in memory
        return true;
    }
}

