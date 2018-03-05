package com.imooc.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * partitioner
 * mvn clean package -DskipTests
 * hadoop jar /opt/hadoop-2.6.0-cdh5.7.0/lib/hadoop-train-1.0.jar com.imooc.hadoop.mapreduce.PartitionerAPP hdfs://localhost:19000/input/partitioner hdfs://localhost:19000/output/
 */
public class PartitionerAPP {

    /**
     * Map:输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
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
     * 自定义分区
     */
    public static class MyPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {

            if (key.toString().equals("xiaomi")) {
                return 0;
            }

            if (key.toString().equals("huawei")) {
                return 1;
            }

            if (key.toString().equals("iphone7")) {
                return 2;
            }

            //nokia
            return 3;
        }
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
        //本地运行报错，服务器jar打包运行正常
        FileSystem fileSystem = FileSystem.get(configuration);
        //本地运行正常
//        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted...");
        }


        //创建Job
        Job job = Job.getInstance(configuration, "wordcount");

        //设置job的处理类
        job.setJarByClass(PartitionerAPP.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置job的partition
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个reducer,每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 2);


    }
}
