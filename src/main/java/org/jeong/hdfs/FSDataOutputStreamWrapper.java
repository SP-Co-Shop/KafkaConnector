package org.jeong.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.Date;

public class FSDataOutputStreamWrapper {

    private Date streamOpenDate;
    private FSDataOutputStream out;

    public FSDataOutputStreamWrapper(FSDataOutputStream out) {
        this.streamOpenDate = new Date();
        this.out = out;
    }

    public void write(String data) throws IOException {
        out.write(data.getBytes());
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
