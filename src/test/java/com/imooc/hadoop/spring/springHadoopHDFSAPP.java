package com.imooc.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 使用 Spring Hadoop 访问HDFS文件系统
 */
public class springHadoopHDFSAPP {

    private ApplicationContext ctx;
    private FileSystem fileSystem;

    /**
     * 读取HDDFS文件内容
     * @throws Exception
     */
    @Test
    public void testText() throws Exception {
        FSDataInputStream inOut = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(inOut, System.out, 1024);
        inOut.close();
    }

    /**
     * 创建HDDFS文件夹
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception {
        fileSystem.mkdirs(new Path("/springhdfs/"));
    }

    @Before
    public void setUp(){
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem)ctx.getBean("fileSystem");
    }

    @After
    public void tearDown () throws Exception
    {
        ctx = null;
        fileSystem.close();
    }
}
