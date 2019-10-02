package com.smile.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author smi1e
 * Date 2019/9/25 19:40
 * Description
 */
public class HDFSTest {
    private static FileSystem hadoop;
    private static Configuration configuration;

    @BeforeAll
    public static void before() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("--------------begin----------------");
        configuration = new Configuration();
        hadoop = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "hadoop");
    }

    /**
     * 简单文件上传
     *
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        hadoop.copyFromLocalFile(new Path("D:\\工具\\hadoop-2.7.3.tar.gz"), new Path("/"));
    }

    /**
     * 数据流分块下载
     */
    @Test
    public void test2() throws IOException {
        FSDataInputStream fis = hadoop.open(new Path("/hadoop-2.7.3.tar.gz"));
        FileOutputStream part1 = new FileOutputStream("D:\\hadoop-2.7.3.tar.gz.part1");
        FileOutputStream part2 = new FileOutputStream("D:\\hadoop-2.7.3.tar.gz.part2");
        byte[] bytes = new byte[1024];
        int size = 1024 * 128;
        for (int i = 0; i < size; i++) {
            fis.read(bytes);
            part1.write(bytes);
        }
        //第一部分完成
        part1.close();
        fis.close();

        fis = hadoop.open(new Path("/hadoop-2.7.3.tar.gz"));
        fis.seek(1024 * 1024 * 128);
        IOUtils.copyBytes(fis, part2, configuration);
        //第二部分完成
        part2.close();
        fis.close();
    }

    @AfterAll
    public static void after() throws IOException {
        hadoop.close();
        System.out.println("----------over--------------------");
    }
}
