package com.reardonsoftware.hadoop.wordcount;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"unchecked", "rawtypes"})
public class WordCountReducerTest {
    private WordCountReducer reducer;
    private Context context;
    
    @Before
    public void init() throws IOException, InterruptedException {
        reducer = new WordCountReducer();
        context = mock(Context.class);
    }

    @Test
    public void testSingleWord() throws IOException, InterruptedException {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(4), new IntWritable(7));
        
        reducer.reduce(new Text("foo"), values, context);
        
        verify(context).write(new Text("foo"), new IntWritable(12));
    }
}
