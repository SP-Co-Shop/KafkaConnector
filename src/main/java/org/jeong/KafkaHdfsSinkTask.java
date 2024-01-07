package org.jeong;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jeong.config.HdfsConfig;
import org.jeong.etc.JarMainFest;
import org.jeong.hdfs.HdfsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaHdfsSinkTask extends SinkTask {

    private JarMainFest mainFest = new JarMainFest(KafkaHdfsSinkConnector.class.getProtectionDomain().getCodeSource().getLocation(), "kafka-custom-connect");

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private HdfsConfig config;

    private HdfsClient hdfsClient;

    @Override
    public String version() {
        return mainFest.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("[ Run ] ---> kafka connector task");

        logger.info("[ Settings ] Task Configs");
        this.config = new HdfsConfig(props);

        logger.info("[ Settings ] hdfs Configs");
        hdfsClient = new HdfsClient(config);

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        hdfsClient.refreshOutputStream();
        records.stream()
                .collect(Collectors.groupingBy(r -> Pair.of(r.topic(), r.kafkaPartition())))
                .forEach((key, value) -> {
                    String topic = key.getLeft();
                    Integer partition = key.getRight();
                    hdfsClient.write(value, partition);
                });

    }

    @Override
    public void stop() {
        logger.info("[ Stop ] ---> kafka connector task");
        try {
            hdfsClient.close();
        }catch (Exception e){
            logger.info(e.getMessage(), e);
        }

    }
}
