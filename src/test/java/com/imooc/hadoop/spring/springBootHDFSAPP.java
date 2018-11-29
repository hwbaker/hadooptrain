package com.imooc.hadoop.spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * 使用 Spring Boot 访问HDFS文件系统
 */

@SpringBootApplication
public class springBootHDFSAPP implements CommandLineRunner {
    @Autowired
    FsShell fsShell;

    public void run(String... strings) throws Exception {
        System.out.println("=========start===========");
        for(FileStatus fileStatus : fsShell.lsr("/springhdfs")) {
            System.out.println(">" + fileStatus.getPath());
        }
        System.out.println("=========end===========");
    }

//    public static void main(String[] args) {
//        SpringApplication.run(springBootHDFSAPP.class, args);
//    }
    
    public static void main(String[] args) {
        try {
            SpringApplication.run(springBootHDFSAPP.class, args);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }
}
