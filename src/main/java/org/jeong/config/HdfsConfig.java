package org.jeong.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HdfsConfig extends AbstractConfig {

    private static final String HDFS_PATH = "hdfs.path";

    private static final String HDFS_PATH_DOC = "hdfs.path";

    public HdfsConfig(Map<String, String> props) { super(CONFIG, props);}

    public static final ConfigDef CONFIG = new ConfigDef()
            .define(HDFS_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HDFS_PATH_DOC);

    public String getHdfsPath() { return this.getString(HDFS_PATH);}
}
