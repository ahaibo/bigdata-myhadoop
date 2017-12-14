package com.hai.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("input/"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        String[] args = new String[] { --name",
//                "SynonymTest3", "--driver-memory",
//                "4g", "--jar",
//                "hdfs:///apps/spark/jobs/tpc-spark-1.0.jar","--class",
//                "com.tpc.analytics.spark.SynonymJob3", "--arg",
//                "yarn-cluster"
//        };
//        Configuration config = new Configuration();
//        config.addResource("core-site.xml");
//        config.addResource("yarn-site.xml");
//        config.addResource("hdfs-site.xml");
//        config.addResource("mapred-site.xml");
//        config.set("mapred.remote.os", "Linux");
//        config.set("os.name", "Linux");
//        config.setInt("MAX_APP_ATTEMPTS", 3);
//        System.setProperty("SPARK_YARN_MODE", "true");
//        System.setProperty("SPARK_USER", "root");
//        System.setProperty("MAX_APP_ATTEMPTS", "1");
//
//        SparkConf sparkConf = new SparkConf();
//        System.setProperty("hadoop.home.dir","/usr/local/hadoop");
//        System.setProperty("os.name", "Linux");
//        System.setProperty("spark.yarn.jar","hdfs:///apps/spark/jobs/spark-yarn_2.11-1.6.0.jar");
//        System.setProperty("yarn.resourcemanager.am.max-attempts","1");
//        sparkConf.setAppName("SynonymTest3").setMaster("yarn").set("spark.local.dir", "/data/hadoop/spark-tmp")
//                .set("spark.driver.maxResultSize", "3g");
//.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName())
//                .set("spark.kryoserializer.buffer.max","1024")
//                .set("mapreduce.app-submission.cross-platform", "true");
//        ClientArguments cArgs = new ClientArguments(args, sparkConf);
//        Client client = new Client(cArgs, config, sparkConf);
//        client.run();
    }
}