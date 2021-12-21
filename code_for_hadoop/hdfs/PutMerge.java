package my.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
首先切换到/data/hadoop 目录下，将该目录下的所有文件删除（此时要求
/data/hadoop 中必须全是文件，不能有目录）。
$cd /data/hadoop
$rm -r /data/hadoop/*


然后在该目录下新建两文件，分别命名为 file1 ,file2。
$touch file1
$touch file2
向 file1 和 file2 中，分别输入内容如下
$echo "hello file1" > file1
$echo "hello file2" > file2
在 my.hdfs 包下，新建类 PutMerge，程序功能是将 Linux 本地文件夹
/data/hadoop/下的所有文件，上传到 HDFS 上并合并成一个文件/hdfstest/mergefile。


查看结果：
$hadoop fs -ls /hdfstest
*/

public class PutMerge{
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);

        FileSystem local = FileSystem.getLocal(conf);

        String from_LinuxDir = "/data/hadoop/";
        String to_HDFS = "/hdfstest/mergefile";

        FileStatus[] inputFiles = local.listStatus(new Path(from_LinuxDir));

        FSDataOutputStream out = hdfs.create(new Path(to_HDFS));

        for (FileStatus file: inputFiles) {
            FSDataInputStream in = local.open(file.getPath());
            byte[] buffer = new byte[256];
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);   
            }
            in.close();
        }
        System.out.println("Finish!");

    }
}