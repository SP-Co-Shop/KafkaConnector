package org.jeong;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.jeong.config.HdfsConfig;
import org.jeong.etc.JarMainFest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.List;
import java.util.Map;

public class KafkaHdfsSinkConnector extends SinkConnector {

    private Map<String, String> props;
    private JarMainFest mainFest = new JarMainFest(
            /* URL */
            this.getClass().getProtectionDomain().getCodeSource().getLocation(),
            /* Project */
            "kafka-custom-connect"
    );
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Override
    public void start(Map<String, String> props) {
        logger.info("[ Start ] ---> kafka connector");
        this.props = props;

        try{
            logger.info("[ Valid ] ---> Connector Config");
            logger.info("Connector props : {}", props.toString());

            new HdfsConfig(props);

        }catch (ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public void stop() {
        logger.info("[ Stop ] ---> kafka connector");
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
