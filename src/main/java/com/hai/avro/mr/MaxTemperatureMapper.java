package com.hai.avro.mr;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by as on 2017/3/26.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    private GenericRecord record = new GenericData.Record(AvroGenericMaxTemperature.SCHEMA);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if (parser.isValidTemperature()) {
            record.put("year", parser.getYearInt());
            record.put("temperature", parser.getAirTemperature());
            record.put("stationId", parser.getStationId());
            context.write(new AvroKey<Integer>(parser.getYearInt()), new AvroValue<GenericRecord>(record));
        }
    }
}
