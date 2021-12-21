package my.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
将 HDFS 文件系统中的文件下载到本地 Linux 系统

1.在/data/hadoop/下创建目录 copytolocal。
$mkdir /data/hadoop/copytolocal

2.在 my.hdfs 包下，创建类 CopyToLocalFile.class，程序功能是将 HDFS 文件
系统上的文件/hdfstest/sample_data，下载到本地/data/hadoop/copytolocal 。

$ cd /data/hadoop/copytolocal
$ ls

*/
public class CopyToLocalFile{
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000";

        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        // if from_HDFS = "hdfstest/sample_data", it equals to "hdfs://localhost:9000/user/hadoop/hdfstest/sample_data"
        String from_HDFS = "/hdfstest/sample_data";
        // 若/data/hadoop/copytolocal权限不够，则 chomd -R 777 /data/hadoop/copytolocal
        String to_Local = "/data/hadoop/copytolocal";

        // 函数hdfs.CopyTOLocalFile()
        hdfs.copyToLocalFile(false, new Path(from_HDFS), new Path(to_Local));
        System.out.println("Finish!");

    }
}

