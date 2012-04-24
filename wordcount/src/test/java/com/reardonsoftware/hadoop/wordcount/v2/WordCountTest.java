package com.reardonsoftware.hadoop.wordcount.v2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    
    @Before
    public void setup() {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        
        Configuration conf = new Configuration();
        mapReduceDriver.setConfiguration(conf);
        reduceDriver.setConfiguration(conf);
        mapDriver.setConfiguration(conf);
        
        
        // TODO complete
    }
    
    @Test
    public void testMapper() {
        
    }

    @Test
    public void testReducer() throws IOException {
        
    }

    @Test
    public void testMapReduce() throws IOException {
        
    }
}
