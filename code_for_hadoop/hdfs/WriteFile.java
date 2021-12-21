package my.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
在 my.hdfs 包 下 ， 新 建 类 WriteFile ， 程 序 功 能 是 在 HDFS 上 ， 创 建
/hdfstest/writefile 文件并在文件中写入内容“hello world hello data!


结果查看
$hadoop fs -ls -R /hdfstest
$hadoop fs -cat /hdfstest/writefile
*/
public class WriteFile{
    public static void main(String[] args) throws IOException, URISyntaxException {

        // FileSystem.get(Configuration conf) 使用配置文件来获取文件系统， 配置文件conf/core-site.xml，若没有指定则返回local file system. （原来是这样）
        // FileSystem.get(URI uri, Configuration conf) 根据uri和conf来确定文件系统
        // 若 hdfs系统的uri 写错，则会默认为local文件系统，从当前java的Project文件所在的位置
        FileSystem hdfs = FileSystem.get(new URI("hdfs//localhost:9000"), new Configuration());

        String filePath = "/hdfstest/writefile";

        // FSDataOutputStream
        FSDataOutputStream create = hdfs.create(new Path(filePath));
        System.out.println("Step 1 Finish!");

        String sayHi = "hello world hello data!";
        byte[] buff = sayHi.getBytes();
        create.write(buff, 0, buff.length);

        create.close();

        System.out.println("Step 2 Finish!");

    }
}