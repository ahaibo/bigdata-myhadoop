package com.hai.avro.mr;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        GenericRecord max = null;
        for (AvroValue<GenericRecord> value : values) {
            GenericRecord record = value.datum();
            if (max == null || (Integer) record.get("temperature") > (Integer) max.get("temperature")) {
                max = newWeatherRecord(record);
            }
        }
        context.write(new AvroKey(max), NullWritable.get());
    }

    private GenericRecord newWeatherRecord(GenericRecord value) {
        GenericRecord record = new GenericData.Record(AvroGenericMaxTemperature.SCHEMA);
        record.put("year", value.get("year"));
        record.put("temperature", value.get("temperature"));
        record.put("stationId", value.get("stationId"));
        return record;
    }
}
