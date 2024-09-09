package com.colak.springtutorial.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DebeziumConfig {

    @Bean
    public io.debezium.config.Configuration employeeConfiguration() {
        return io.debezium.config.Configuration.create()
                .with("name", "mysql-connector")
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")

                .with("topic.prefix", "employee_topic1")

                // when we run debezium for the first time, it reads the entire database and then calculate a resume token
                // to detect changes starting from that point.
                // This resume token will be saved into Kafka as we use KafkaOffsetBackingStore
                .with("offset.storage", "com.colak.springtutorial.embeddeddebeziumlistener.CustomOffsetBackingStore")
                // .with("offset.storage.topic", "debezium-connector-offset")
                // .with("offset.storage.partitions", "1")
                // .with("offset.storage.replication.factor", "1")
                .with("offset.flush.interval.ms", "60000")

                .with("database.server.name", "mysql-debezium")
                .with("database.server.id", "184054")
                .with("database.hostname", "localhost")
                .with("database.port", "3306")
                .with("database.user", "debezium")
                .with("database.password", "123456")
                .with("database.dbname", "debezium")
                .with("database.history.kafka.bootstrap.servers", "localhost:29092")
//                .with("database.history.kafka.topic", "dbz-history")

                .with("heartbeat.interval.ms", "1000")

                .with("schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory")
                // .with("schema.history.internal.kafka.bootstrap.servers", "localhost:29092")
                //
                // .with("bootstrap.servers", "localhost:29092")

                // .with("include.schema.changes", "true")


                .build();
    }

}
