package com.C201844100.DepartureDelayCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.C201844100.DepartureDelayCount.AirlinePerformanceParser;

import java.io.IOException;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputValue =  new IntWritable(1);
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		outputKey.set(parser.getYear() + "," + parser.getMonth());
		if(parser.isDepartureDelayAvilable()) {
		if(parser.getDepartureDelayTime() > 0) {
			outputKey.set("D," + parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		}
		}
		
		if(parser.isDepartureDelayAvilable()) {
			if(parser.getArriveDelayTime() > 0) {
				outputKey.set("A," + parser.getYear() + "," + parser.getMonth());
				context.write(outputKey, outputValue);
			}
		}
	}
	
}
