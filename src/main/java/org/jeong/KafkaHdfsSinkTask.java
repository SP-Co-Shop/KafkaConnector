package org.jeong;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jeong.config.HdfsConfig;
import org.jeong.etc.JarMainFest;
import org.jeong.hdfs.HdfsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaHdfsSinkTask extends SinkTask {

    private JarMainFest mainFest = new JarMainFest(KafkaToHdfsSinkConnector.class.getProtectionDomain().getCodeSource().getLocation(), "kafka-custom-connect");

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());


    private HdfsClient hdfsClient = null;

    @Override
    public String version() {
        return mainFest.getVersion();
    }

    private HdfsConfig config;
    @Override
    public void start(Map<String, String> props) {
        logger.info("start kafka connector's task");
        this.config = new HdfsConfig(props);
        hdfsClient = new HdfsClient(config);

        try {
            hdfsClient.init();
        } catch (IOException e) {
            logger.error("hdfs writer initialize failed");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        hdfsClient.refreshOutputStream();

        records.stream()
                .collect(Collectors.groupingBy(r -> Pair.of(r.topic(), r.kafkaPartition())))
                .forEach((key, value) -> {
                    String topic = key.getLeft();
                    Integer partition = key.getRight();
                    try {
                        hdfsClient.write(value, topic, partition);
                    } catch (Exception e) {
                        logger.error("Error writing to HDFS : {}", e.getMessage(), e);
                    }
                });
    }

    @Override
    public void stop() {
        logger.info("Stop kafka connector task");
        try {
            hdfsClient.close();
        }catch (Exception e){
            logger.info("Error closing HDFS client: {}",e.getMessage(), e);
        }
    }
}