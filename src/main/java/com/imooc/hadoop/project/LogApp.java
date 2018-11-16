package com.imooc.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用MapReduce完成需求：统计浏览器访问次数
 */
public class LogApp {

    /**
     * Map:输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        // setup保证UserAgentParser只会初始化一遍
        private UserAgentParser userAgentParser;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); // 读取日志每行日志信息

            String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
            UserAgent agent = userAgentParser.parse(source);

            String browser = agent.getBrowser();
            context.write(new Text(browser), new LongWritable(1));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }

    }

    /**
     * Reduce:输出文件
     */
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 获取指定字符串中，指定字符标识出现的索引位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private static int getCharacterPosition(String value, String operator, int index)
    {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int miDx = 0;
        while (slashMatcher.find()) {
            miDx++;

            if (miDx == index) {
                break;
            }
        }

        return slashMatcher.start();
    }

    /**
     * 封装MapReduce作业的所有信息
     * @param args
     */
    public static void main(String[] args) throws Exception {

        //创建Configuration
        Configuration configuration = new Configuration();

        // 准备清理已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted...");
        }


        //创建Job
        Job job = Job.getInstance(configuration, "LogApp");

        //设置job的处理类
        job.setJarByClass(LogApp.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(LogApp.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(LogApp.MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 2);


    }
}
