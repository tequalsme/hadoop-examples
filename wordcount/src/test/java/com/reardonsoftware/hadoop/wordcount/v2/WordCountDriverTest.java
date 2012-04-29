package com.reardonsoftware.hadoop.wordcount.v2;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class WordCountDriverTest {
    private Configuration conf;
    private Path input;
    private Path output;
    private FileSystem fs;
    
    @Before
    public void setup() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        
        input = new Path("src/test/resources/input");
        output = new Path("target/output");
        
        fs = FileSystem.getLocal(conf);
        fs.delete(output, true);
    }

    @Test
    public void test() throws Exception {
        WordCount wordCount = new WordCount();
        wordCount.setConf(conf);
        
        int exitCode = wordCount.run(new String[] {input.toString(), output.toString()});
        assertEquals(0, exitCode);
        
        validateOuput();
    }

    private void validateOuput() throws IOException {
        InputStream in = null;
        try {
            in = fs.open(new Path("target/output/part-r-00000"));
            
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            assertEquals("five\t1", br.readLine());
            assertEquals("four\t1", br.readLine());
            assertEquals("one\t3", br.readLine());
            assertEquals("six\t1", br.readLine());
            assertEquals("three\t1", br.readLine());
            assertEquals("two\t2", br.readLine());
            
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
