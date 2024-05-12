package org.jeong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.config.HdfsConfig;
import org.jeong.entry.RecordEntry;
import org.jeong.process.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsClient {
    private final HdfsConfig config;
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private FileSystem fileSystem;
    private DateTimeFormatter formatter;

    private final Map<String, FSDataOutputStreamWrapper> outs = new HashMap<String, FSDataOutputStreamWrapper>();

    public HdfsClient(HdfsConfig config) {
        this.config = config;
    }

    public void init() throws IOException {
        this.fileSystem = FileSystem.newInstance(buildConfig());
    }

    public void write(List<SinkRecord> records, String topic, Integer partition){
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH");
        records.forEach(record ->{

            try {
                RecordEntry recordEntry = RecordParser.parse(record);
                String convertingTime = LocalDateTime.parse(
                        recordEntry.getTime(),
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME
                ).format(formatter);

                String hdfsPath = String.format("%s/%s/%s",
                        this.config.getHdfsPath(),
                        convertingTime,
                        String.format("log-%s.json", partition));

                writeToHDFS(hdfsPath, record.value().toString());
            } catch (IOException | ParseException e) {
                logger.error("Failed to write record to HDFS. Topic: {}, Partition: {}, Error: {}", topic, partition, e.getMessage(), e);
            }
        });
    }

    private void writeToHDFS(String path, String data) throws IOException {
        FSDataOutputStreamWrapper out = getOutputStream(path);
        if (out == null) {
            logger.error("OutputStream not found for path: {}", path);
            throw new IOException("OutputStream not found for path: " + path);
        }
        out.write(data);
        out.write("\n");
    }

    private FSDataOutputStreamWrapper getOutputStream(String path) throws IOException {
        FSDataOutputStreamWrapper outWrapper = outs.get(path);

        if (outWrapper == null) {
            Path hdfsPath = new Path(path);

            logger.info("hdfsPath : {}", hdfsPath.toString());
            try {
                FSDataOutputStream fsOut = null;
                if (!fileSystem.exists(hdfsPath)) {
                    logger.info("Creating new file at path: {}", path);
                    fsOut = fileSystem.create(hdfsPath, true);
                } else {
                    logger.info("Appending to existing file at path: {}", path);
                    fsOut = fileSystem.append(hdfsPath);
                }

                if (fsOut == null) {
                    throw new IOException("Failed to create or append to file at path: " + path);
                }

                outWrapper = new FSDataOutputStreamWrapper(fsOut);
                outs.put(path, outWrapper);
            } catch (IOException e) {
                logger.error("Error while accessing HDFS for path: {}, Error: {}", path, e.getMessage(), e);
                throw e;
            }
        }

        return outWrapper;
    }

    public Configuration buildConfig() {
        Configuration hadoopConfig = new Configuration();
        String classpath = System.getProperty("java.class.path");
        logger.info("========== {}", classpath);
        hadoopConfig.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("hadoop/core-site.xml"));
        hadoopConfig.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("hadoop/hdfs-site.xml"));
        hadoopConfig.setInt("dfs.replication", 3);
        hadoopConfig.setStrings("fs.hdfs.impl", new DistributedFileSystem().getClass().getName());

        String ip = hadoopConfig.get("fs.default.name");
        System.out.println(ip);

        return hadoopConfig;
    }

    public void close() throws InterruptedException {

        for (String path : outs.keySet()) {
            closeOutputStream(path);
        }
        outs.clear();
    }

    private void closeOutputStream(String path) throws InterruptedException {
        FSDataOutputStreamWrapper out = outs.get(path);

        int failOnRetry = 3;
        int attempt = 0;
        boolean result = false;

        while (attempt < failOnRetry) {
            try {
                out.close();
                result = true;
                break;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                attempt += 1;
                if (attempt != failOnRetry) {
                    Thread.sleep(1000);
                }
            }
        }
        if (!result) {
            logger.error("[ Fail ] ---> HDFS Output Stream close");
        }
    }

    public void refreshOutputStream() {
        long currentTime = new Date().getTime();
        for (Map.Entry<String, FSDataOutputStreamWrapper> entry : outs.entrySet()) {
            long streamOpenElapsed = currentTime - entry.getValue().getStreamOpenDate().getTime();
            if (streamOpenElapsed > 60000) { // 60초 이상 열려 있으면 닫음
                try {
                    closeOutputStream(entry.getKey());
                } catch (Exception e) {
                    logger.error("Error occurred when closing HDFS output stream for path: {}", entry.getKey(), e);
                }
            }
        }
    }
}
