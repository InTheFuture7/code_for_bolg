package my.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LocateFile{
    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
        
        Path file = new Path("/hdfstest/sample_data");
        FileStatus fileStatus = hdfs.getFileStatus(file);

        BlockLocation[] location = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation block: location) {
            // block.getHosts()
            String[] hosts = block.getHosts();
            for (String host: hosts){
                System.out.println("block:" + block + "host:" + host);
            }
        }

    }
}