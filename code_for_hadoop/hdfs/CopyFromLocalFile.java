package my.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
将本地文件上传到 HDFS 文件系统

$hadoop fs -ls -R

1.在/data/hadoop 下使用 vim 创建 sample_data 文件，
$cd /data/hadoop
$vim sample_data

2.向 sample_data 文件中写入 hello world。(使用 vim 编辑时，需输入 a，开启
输入模式)
hello world

3.在 my.hdfs 包下，创建类 CopyFromLocalFile.java，
程序功能是将本地 Linux 操作系统上的文件 /data/hadoop/sample_data ，上传到 HDFS 文件系统的 /hdfstest 目录下。
*/
public class CopyFromLocalFile{
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);

        String from_Linux = "/data/hadoop/sample_data";
        String to_HDFS = "/hdfstest";

        // hdfs.copyFromLocalFile()
        hdfs.copyFromLocalFile(new Path(from_Linux), new Path(to_HDFS));
        System.out.println("Finish!");
    }
}
