package com.hai.avro.mr;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by as on 2017/3/26.
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {

    public static final Schema SCHEMA = new Schema.Parser().parse(
            "{" +
                    " \"type\": \"record\"," +
                    " \"name\": \"WeatherRecord\"," +
                    " \"doc\": \"A weather reading.\"," +
                    " \"fields\": [" +
                    " {\"name\": \"year\", \"type\": \"int\"}," +
                    " {\"name\": \"temperature\", \"type\": \"int\"}," +
                    " {\"name\": \"stationId\", \"type\": \"string\"}" +
                    " ]" +
                    "}"
    );

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance();

        job.setJobName("Max temperature");
        job.setJarByClass(getClass());
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SCHEMA);
        AvroJob.setOutputKeySchema(job, SCHEMA);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        // set the output format
//        job.setOutputFormatClass(AvroParquetOutputFormat.class);
//        AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
//        AvroParquetOutputFormat.setSchema(job, ReadAlignment.SCHEMA$);
//        AvroParquetOutputFormat.setCompression(job, CompressionUtils.getParquetCodec(codecName));
//        AvroParquetOutputFormat.setCompressOutput(job, true);
//
//        // set a large block size to ensure a single row group
//        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}
