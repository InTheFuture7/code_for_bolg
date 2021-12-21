package my.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/*
discription: Create a directory in the HDFS

use this command: "$hadoop fs -ls -R /" in the terminal to inspect the result of script
 */
public class MakeDir {
    public static void main(String[] args) throws IOException, URISyntaxException {

        // 创建 Configuration 对象
        // 实例化一个Configuration，它会自动去加载本地的 core-site.xml 配置文件的fs.defaultFS属性。(该文件放在项目的resources目录即可。)
        Configuration conf = new Configuration();
        // FileSystem默认的端口号:9000
        String hdfsPath = "hdfs://localhost:9000";
        // 获取 FileSystem 对象
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);

        String newDir = "/hdfstest";
        boolean result = hdfs.mkdirs(new Path(newDir));
        if (result) {
            System.out.println("Success!");
        } else {
            System.out.println("Failed!");
        }
    }
}
