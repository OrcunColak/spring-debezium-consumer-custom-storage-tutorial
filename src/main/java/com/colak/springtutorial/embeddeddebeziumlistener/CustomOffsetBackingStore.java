package com.colak.springtutorial.embeddeddebeziumlistener;


import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.util.Map;
import java.util.Set;

public class CustomOffsetBackingStore extends MemoryOffsetBackingStore {


    @Override
    public Set<Map<String, Object>> connectorPartitions(String s) {
        return null;
    }
}

