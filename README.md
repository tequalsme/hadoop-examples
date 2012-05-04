Read the complete article at [DZone](http://java.dzone.com/articles/effective-testing-strategies)

***

Effective Testing Strategies for MapReduce Applications
=======================================================

In this article I demonstrate various strategies that I have used to test Hadoop MapReduce applications, and discuss the pros and cons of each. I start with the venerable WordCount example, refactoring it slightly to demonstrate unit testing both the mapper and reducer portions. Next, I show how MRUnit can be used for these same unit tests as well as testing the mapper and reducer together. Finally, I show how the job's driver can be tested with the local job runner using test data on the local filesystem.

WordCount
---------

The example used throughout is Hadoop's own [WordCount](http://hadoop.apache.org/common/docs/current/mapred_tutorial.html#Example%3A+WordCount+v1.0). The original source from Hadoop's tutorial is in Listing 1.

    public class WordCount {
    
        public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
    
            public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    word.set(tokenizer.nextToken());
                    output.collect(word, one);
                }
            }
        }
    
        public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
            public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
                int sum = 0;
                while (values.hasNext()) {
                    sum += values.next().get();
                }
                output.collect(key, new IntWritable(sum));
            }
        }
    
        public static void main(String[] args) throws Exception {
            JobConf conf = new JobConf(WordCount.class);
            conf.setJobName("wordcount");
    
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
    
            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);
    
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
    
            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
            JobClient.runJob(conf);
        }
    }
Listing 1. The Original Version of Hadoop's WordCount

The first stage in any testing strategy is unit testing, and MapReduce is no different. To properly unit test WordCount I chose to refactor it slightly, eliminating deprecated API calls and moving the mapper and reducer inner classes to top-level classes. Though the inner classes are convenient for an example and could have been unit tested easily enough, I believe factoring them out makes for a better design and provides more flexibility in real-world situations. (Imagine the use case of skipping common words such as "a", "an", and "the" - a different mapper could be easily substituted with the existing combiner/reducer. Likewise, imagine applying a variable weight to words with a different reducer.)

There is one additional change I made to facilitate unit testing, which
I'll discuss in the next section. The refactored version is in Listing 2.
All source code for this article is available on [github](https://github.com/tequalsme/hadoop-examples).

    public class WordCount extends Configured implements Tool {
    
        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            
            Job job = new Job(conf);
            job.setJarByClass(WordCount.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            
            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(WordCountReducer.class);
            job.setReducerClass(WordCountReducer.class);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
    
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
            return (job.waitForCompletion(true) ? 0 : 1);
        }
    
        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new WordCount(), args);
            System.exit(res);
        }
    }
    
    public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        
        // protected to allow unit testing
        protected Text word = new Text();
    
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }
    
    public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
Listing 2. WordCount, refactored for testability

Unit Testing with Mocks
-----------------------

Using the refactored version of WordCount, we are ready to create unit
tests for the mapper and reducer. In each case this is done by mocking the
Context object and verifying the correct behavior. I chose [Mockito](http://code.google.com/p/mockito/) for my mocking framework.

The mapper's unit test is shown in Listing 3. In init() the mapper object is created, along with the Context mock. The test methods send content to the mapper and verify that the correct method(s) were called on the Context mock.

    public class WordCountMapperTest {
        private WordCountMapper mapper;
        private Context context;
        private IntWritable one;
        
        @Before
        public void init() throws IOException, InterruptedException {
            mapper = new WordCountMapper();
            context = mock(Context.class);
            mapper.word = mock(Text.class);
            one = new IntWritable(1);
        }
    
        @Test
        public void testSingleWord() throws IOException, InterruptedException {
            mapper.map(new LongWritable(1L), new Text("foo"), context);
            
            InOrder inOrder = inOrder(mapper.word, context);
            assertCountedOnce(inOrder, "foo");
        }
        
        @Test
        public void testMultipleWords() throws IOException, InterruptedException {
            mapper.map(new LongWritable(1L), new Text("one two three four"), context);
            
            InOrder inOrder = inOrder(mapper.word, context, mapper.word, context, mapper.word, context, mapper.word, context);
            
            assertCountedOnce(inOrder, "one");
            assertCountedOnce(inOrder, "two");
            assertCountedOnce(inOrder, "three");
            assertCountedOnce(inOrder, "four");
        }
        
        private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
        inOrder.verify(mapper.word).set(eq(w));
        inOrder.verify(context).write(eq(mapper.word), eq(one));
        }
    }
Listing 3. WordCountMapperTest

The slight change that I alluded to earlier is that I made the word member variable protected in WordCountMapper, and replaced it with a mock in the unit test. This was necessary because Hadoop reuses objects between successive calls to map() (and also reduce()); this is why the Mapper is able to call "word.set(tokenizer.nextToken());" rather than "word = new Text(tokenizer.nextToken());". This is done for performance reasons, but poses a problem when testing. In "testMultipleWords()", Mockito cannot verify sequential writes to objects that are reused. But by mocking the Text object, and using Mockito's InOrder, the test can be correctly written. I consider making one small change to the mapper to facilitate testing a no-brainer; if you disagree, consider using a different mocking framework that may not pose this issue.

Testing the reducer is similar to that of the mapper, without the complications of the reused Text object. The reducer's unit test is in Listing 4.

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
Listing 4. WordCountReducerTest

Testing Counters
----------------

Counters can be tested in a similar fashion: mock the Counter, obtain a reference to the mock when accessed, and verify that it was incremented properly. A mapper that uses a Counter is shown in Listing 5, and its unit test in Listing 6. 

    public class WordCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        
        enum Counters {
            TOTAL_WORDS
        }
        
        // protected to allow unit testing
        protected Text word = new Text();
    
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
                context.getCounter(Counters.TOTAL_WORDS).increment(1);
            }
        }
    }
Listing 5. WordCountMapperWithCounter

    public class WordCountMapperWithCounterTest {
        private WordCountMapperWithCounter mapper;
        private Context context;
        private Counter counter;
        private IntWritable one;
    
        @Before
        public void init() throws IOException, InterruptedException {
            mapper = new WordCountMapperWithCounter();
            context = mock(Context.class);
            mapper.word = mock(Text.class);
            one = new IntWritable(1);
    
            counter = mock(Counter.class);
            when(context.getCounter(WordCountMapperWithCounter.Counters.TOTAL_WORDS)).thenReturn(counter);
        }
    
        @Test
        public void testSingleWord() throws IOException, InterruptedException {
            mapper.map(new LongWritable(1L), new Text("foo"), context);
    
            InOrder inOrder = inOrder(mapper.word, context, counter);
            assertCountedOnce(inOrder, "foo");
        }
    
        @Test
        public void testMultipleWords() throws IOException, InterruptedException {
            mapper.map(new LongWritable(1L), new Text("one two three four"), context);
    
            InOrder inOrder = inOrder(mapper.word, context, counter, mapper.word, context, counter, mapper.word, context,
                    counter, mapper.word, context, counter);
    
            assertCountedOnce(inOrder, "one");
            assertCountedOnce(inOrder, "two");
            assertCountedOnce(inOrder, "three");
            assertCountedOnce(inOrder, "four");
        }
    
        private void assertCountedOnce(InOrder inOrder, String w) throws IOException, InterruptedException {
            inOrder.verify(mapper.word).set(eq(w));
            inOrder.verify(context).write(eq(mapper.word), eq(one));
            inOrder.verify(counter).increment(1);
        }
    }
Listing 6. WordCountMapperWithCounterTest

MRUnit
------

In many cases, unit testing the mapper and reducer with mocks may be
sufficient. However, there is an alternative approach that can offer an
additional level of coverage. [MRUnit](http://incubator.apache.org/mrunit/) is a unit testing framework for Hadoop. It began as an open source offering included in Cloudera's Distribution for Hadoop, and is now an Apache Incubator project. It provides classes for testing mappers and reducers separately and together.

The WordCount tests using MRUnit are shown in Listing 7. This class tests the mapper in isolation, the reducer in isolation, and the mapper and reducer together as a unit. In “setup()”, drivers are created for the mapper, reducer, and mapper-reducer, and the WordCount classes are set on the drivers. In “testMapper()”, calling “withInput()” passes input to the mapper, and the series of calls to “withOutput()” setup the expected output. “runTest()” then executes the mapper and verifies the expected output.

    public class WordCountMRUnitTest {
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
            // add config here as needed
            mapReduceDriver.setConfiguration(conf);
            reduceDriver.setConfiguration(conf);
            mapDriver.setConfiguration(conf);
        }
        
        @Test
        public void testMapper() {
            mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
            mapDriver.withOutput(new Text("cat"), new IntWritable(1));
            mapDriver.withOutput(new Text("cat"), new IntWritable(1));
            mapDriver.withOutput(new Text("dog"), new IntWritable(1));
            mapDriver.runTest();
        }
    
        @Test
        public void testReducer() throws IOException {
            List<IntWritable> values = new ArrayList<IntWritable>();
            values.add(new IntWritable(1));
            values.add(new IntWritable(1));
            reduceDriver.withInput(new Text("cat"), values);
            reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
            reduceDriver.runTest();
        }
    
        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
            mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
            mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
            mapReduceDriver.runTest();
        }
    }
Listing 7. WordCountMRUnitTest

Similar procedures are followed for testing the reducer in isolation, as well as testing the mapper/reducer combination. This ability, along with its ease of use, makes MRUnit a very attractive choice.
 
Testing the Driver
------------------

To this point I've shown ways to unit test the mapper and reducer both separately and as a unit. What has yet to be tested is the driver class. The simplest way to do so is to leverage the local job runner.

WordCountDriverTest (Listing 8.) demonstrates this approach. In “setup()”, the test creates a new Configuration and configures it to use the local filesystem and local job runner. It also creates Path objects to point to input data (Listing 9.) and a location to place output data. Finally, it gets a reference to the local filesystem, and deletes any previous output data.

The test method instantiates the driver class, passes in the Configuration object, and executes the job. The test then validates proper exit code and output contents.

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
Listing 8. WordCountDriverTest

    one
    two
    three four five
    one two
    six
    one
Listing 9. Input data for WordCountDriverTest

Next Steps
----------

Now that the program is sufficiently unit tested, it can be executed on a test cluster to identify potential integration and scaling issues. Or, the Hadoop classes MiniDFSCluster and MiniMRCluster could be leveraged to create additional tests that execute against a pseudo-cluster.

Conclusion
----------

Testing is an important part of developing MapReduce applications. I hope that the strategies presenting here will help you determine the best approaches for your needs.

References
----------

* [Hadoop MapReduce Tutorial](http://hadoop.apache.org/common/docs/current/mapred_tutorial.html), where WordCount is demonstrated
* [Hadoop: The Definitive Guide](http://hadoopbook.com/), by Tom White. This aptly-named book contains an excellent chapter on testing
* [MRUnit](http://incubator.apache.org/mrunit/)
* All source for this article is available on [github](https://github.com/tequalsme/hadoop-examples) at tequalsme/hadoop-examples.git

