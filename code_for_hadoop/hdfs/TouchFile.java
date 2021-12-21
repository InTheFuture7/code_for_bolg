package my.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
Create a file in HDFS

$hadoop fs -ls -R /
 */
public class TouchFile {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        String hdfsPath = "hdfs://localhost:9000";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), configuration);

        // FileSystem hdfs = FileSystem.get(new URI("hdfs:localhost:9000"), new Configuration);

        String filePath = "/hdfstest/touchfile";
        FSDataOutputStream create = hdfs.create(new Path(filePath));
        System.out.println("Finish!");

    }
}