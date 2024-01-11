package org.jeong.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class FSDataOutputStreamWrapper {

    private Date streamOpenDate;
    private FSDataOutputStream out;
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public FSDataOutputStreamWrapper(FSDataOutputStream out) {
        if (out == null) {
            throw new IllegalArgumentException("Error FSDataOutputStream cannot be null");
        }
        this.streamOpenDate = new Date();
        this.out = out;
    }

    public void write(String data) throws IOException {
        logger.info("Write data information : {}",data.getBytes(StandardCharsets.UTF_8));
        out.write(data.getBytes(StandardCharsets.UTF_8));
    }

    public void close() throws IOException {
        out.hflush();
        out.hsync();
        out.close();
    }

    public Date getStreamOpenDate() {
        return streamOpenDate;
    }

}
