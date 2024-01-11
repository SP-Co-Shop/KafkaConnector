package org.jeong;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.jeong.config.HdfsConfig;
import org.jeong.etc.JarMainFest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaToHdfsSinkConnector extends SinkConnector {

    private Map<String, String> props;
    private JarMainFest mainFest = new JarMainFest(this.getClass().getProtectionDomain().getCodeSource().getLocation(), "kafka-custom-connect");
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Override
    public void start(Map<String, String> props) {
        logger.info("Start kafka connector");
        this.props = props;

        try {
            HdfsConfig config = new HdfsConfig(props);
            logger.info("Connector configuration validated. Config: {}", config);
        } catch (ConfigException e) {
            logger.error("Configuration validation failed: {}", e.getMessage(), e);
            throw new ConnectException("Fail to Start Connection Error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaHdfsSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Setting task config, max tasks: {}", maxTasks);
        return IntStream.range(0, maxTasks)
                .mapToObj(taskId -> {
                    Map<String, String> taskProps = new HashMap<>();
                    taskProps.putAll(props);
                    taskProps.put("task.id", Integer.toString(taskId));
                    return taskProps;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        logger.info("Stop kafka connector");
    }

    @Override
    public ConfigDef config() {
        return HdfsConfig.CONFIG;
    }

    @Override
    public String version() {
        return mainFest.getVersion();
    }

}
