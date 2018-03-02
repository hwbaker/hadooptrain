package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.SocketPermission;
import java.net.URI;

/**
 * Hadoop HDFS Java API操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://localhost:19000";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS目录.选中这个方法->右键->运行
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看文件
     * @throws Exception
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名文件
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传本地文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("/Users/hwbaker/SiteGit/testData/hello.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传本地文件到HDFS,带进度条
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
//        Path localPath = new Path("/opt/spark-2.2.0/spark-2.2.0-bin-2.6.0-cdh5.7.0.tgz");
//        Path hdfsPath = new Path("/hdfsapi/test");
//        fileSystem.copyFromLocalFile(localPath, hdfsPath);

        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("/opt/spark-2.2.0/spark-2.2.0-bin-2.6.0-cdh5.7.0.tgz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/spark-2.2.0-bin-2.6.0-cdh5.7.0.tgz"),
                new Progressable() {
                    public void progress() {
                        System.out.print("."); // 带进度提醒
                    }
                });
        IOUtils.copyBytes(in, output, 4096);
    }

    /**
     * 下载HDSF文件到本地
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        Path localPath = new Path("/Users/hwbaker/SiteGit/testData/");
        fileSystem.copyToLocalFile(hdfsPath, localPath);

    }

    /**
     * 查看某个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/"));
        for (FileStatus fileStatus: fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "目录" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);

        }

    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");

        configuration = new Configuration();
//        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hwbaker");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;

        System.out.println("HDFSApp.tearDown");
    }
}
