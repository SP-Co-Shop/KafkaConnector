package org.jeong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.config.HdfsConfig;
import org.jeong.dto.Record;
import org.jeong.process.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsClient {
    private HdfsConfig config;

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private FileSystem fileSystem;

    private Parser parser;

    private Map<String, FSDataOutputStreamWrapper> outs = new HashMap<String, FSDataOutputStreamWrapper>();

    public HdfsClient(HdfsConfig config) {
        this.config = config;
    }

    public void init() throws IOException{
        this.fileSystem = FileSystem.newInstance(buildConfig());

    }


    public void write(List<SinkRecord> records, Integer partition){
        records.forEach(record ->{
            try {
                /**
                 * @param record Parsing
                 * */
                Record recordData = parser.parse(record);

                String hdfsPath = String.format("%s/%s",
                        this.config.getHdfsPath(),
                        String.format("data-%s.text",partition));

                FSDataOutputStreamWrapper out = getOutputStream(hdfsPath);


                out.write(record.toString());
                out.write("\n");

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private FSDataOutputStreamWrapper getOutputStream(String path) throws IOException {

        /* 해당 Path 키를 가지고 있지 않을 시 */
        if (!outs.containsKey(path)) {
            Path hdfsPath = new Path(path);

            /* 파일이 존재 하지 않으면 파일 생성 */
            if (!fileSystem.exists(hdfsPath)) {
                fileSystem.createNewFile(hdfsPath);
            }
            /* 해당 파일에 대해 hdfs output stream 생성 */
            outs.put(path, new FSDataOutputStreamWrapper(fileSystem.append(hdfsPath)));
        }
        return outs.get(path);
    }

    public Configuration buildConfig() {
        Configuration hadoopConfig = new Configuration();
        String classpath = System.getProperty("java.class.path");
        logger.info("==========" + classpath);
        hadoopConfig.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("hadoop/core-site.xml"));
        hadoopConfig.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("hadoop/hdfs-site.xml"));
        hadoopConfig.setInt("dfs.replication", 3);
        hadoopConfig.setStrings("fs.hdfs.impl", new DistributedFileSystem().getClass().getName());

        String ip = hadoopConfig.get("fs.default.name");
        System.out.println(ip);

        return hadoopConfig;
    }

    public void close() throws IOException, InterruptedException {

        for (String path: outs.keySet()){
            closeOutputStream(path);
        }
        outs.clear();
    }

    private void closeOutputStream(String path) throws InterruptedException {
        FSDataOutputStreamWrapper out = outs.get(path);

        int failOnRetry = 3;
        int attempt = 0;
        boolean result = false;

        while (attempt < failOnRetry){
            try {
                out.close();
                result = true;
                break;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                attempt += 1;
                if (attempt != failOnRetry){
                    Thread.sleep(1000);
                }
            }
        }
        if (!result){
            logger.error("[ Fail ] ---> HDFS Output Stream close");
        }
    }

    public void refreshOutputStream() {
        for (String path: outs.keySet()) {
            long streamOpenElapsed = new Date().getTime() - outs.get(path).getStreamOpenDate().getTime();
            if (streamOpenElapsed > 60000) {
                try {
                    closeOutputStream(path);
                    outs.remove(path);
                } catch (Exception e) {
                    logger.error("error occured when hdfs output stream close");
                }
            }
        }
    }
}
