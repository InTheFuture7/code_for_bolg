package my.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/*
discription: Create a directory in the HDFS

use this command: "$hadoop fs -ls -R /" in the terminal to inspect the result of script
 */
public class OperateFile {
    public static void main(String[] args) throws IOException, URISyntaxException {

        // 创建 Configuration 对象
        Configuration conf = new Configuration();
        // FileSystem默认的端口号:9000
        String hdfsPath = "hdfs://localhost:9000";
        // 获取 FileSystem 对象
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);

        // 迭代 /hdfstest 文件夹下所有的文件文件
        String watchHDFS = "/hdfstest";
        FileStatus[] files = hdfs.listStatus(new Path(watchHDFS));
        for (FileStatus file: files){
            if (file.isFile()) {
                System.out.println("file:" + file.getPermission() + " " + file.getOwner() + " " + file.getGroup() + " " + file.getPath());
            }
        }

        System.out.println("----------Operate file----------");

        // create file。如果文件所在的文件夹不存在，那么会自动创建文件夹
        String filePath = "/hdfstest/src_data";
        FSDataOutputStream create = hdfs.create(new Path(filePath));
        System.out.println("create file successfully!" + create);


        // 重命名 hdfs.rename() src_data -> dst_data
        Path src = new Path("/hdfstest/src_data");
        Path dst = new Path("/hdfstest/dst_data");
        System.out.println("rename file successfully?" + hdfs.rename(src, dst));

        // 判断文件是否存在 hdfs.exist() /hdfstest/src_data
        System.out.println("after rename,src_data is still exist?" + hdfs.exists(src));


        // 删除文件 hdfs.delete() dst_data
        hdfs.delete(dst, true);
        // 判断文件是否存在 /hdfstest/dst_data
        System.out.println("after delete(), dst_data is still exist?" + hdfs.exists(dst));


        System.out.println("----------Operate file----------");


        // 迭代 /hdfstest 文件夹下所有的文件文件
        for (FileStatus file: files){
            if (file.isFile()) {
                System.out.println("file:" + file.getPermission() + " " + file.getOwner() + " " + file.getGroup() + " " + file.getPath());
            }
        }
    }
}
