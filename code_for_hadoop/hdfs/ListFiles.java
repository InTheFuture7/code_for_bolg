package my.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
/**
列出文件系统 /hdfstest 下的所有文件及其权限、用户组、所属用户
 */
public class ListFiles {
    public static void main(String[] args) throws URISyntaxException, IOException {
        // 获取 FileSystem 对象
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), new Configuration());

        String watchHDFS = "/hdfstest";

        System.out.println("迭代指定文件夹下的文件");
        FileStatus[] files = hdfs.listStatus(new Path(watchHDFS));
        for (FileStatus file: files){
            if (file.isDirectory()) {
                System.out.println("dir:" + file.getPermission() + " " + file.getOwner() + " " + file.getGroup() + " " + file.getPath());
            } else if(file.isFile()){
                System.out.println("file:" + file.getPermission() + " " + file.getOwner() + " " + file.getGroup() + " " + file.getPath());
            }
        }
    }
}
