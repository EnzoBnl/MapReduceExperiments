package com.enzobnl.wordcountmr;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/* ORDER PARTITIONER -> COMBINER
In Hadoop- The definitive guide 3rd edition, page 209, we have below words:

    Before it writes to disk, the thread first divides the data into partitions
    corresponding to the reducers that they will ultimately be sent to. Within each
     partition, the background thread performs an in-memory sort by key, and if
     there is a combiner function, it is run on the output of the sort. Running
      the combiner function makes for a more compact map output, so there is less
       data to write to local disk and to transfer to the reducer.

    Each time the memory buffer reaches the spill threshold, a new spill file is created,
    so after the map task has written its last output record, there could be several spill
     files. Before the task is finished, the spill files are merged into a single
     partitioned and sorted output file. The configuration property io.sort.factor
     controls the maximum number of streams to merge at once; the default is 10.

    If there are at least three spill files (set by the min.num.spills.for.combine
     property), the combiner is run again before the output file is written. Recall
      that combiners may be run repeatedly over th einput without affecting the final
      result. If there are only one or two spills, the potential reduction in map
      output size is not worth the overhead in invoking the combiner, so it is not run
      again for this map output.So combiner is run during merge spilled file.


RECORD READER -> ONMAPPERNODES( MAPPER -> PARTITIONER -> SORT -> COMBINER) -> SHUFFLE -> ONREDUCENODES(Fetches -> Merges/Sort ->Reduce)
*/

public class WordCount {
    private static final int NUM_REDUCERS = 2;
    public static class TagTokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        Pattern pattern = Pattern.compile("[ \t]*(<[^(> /!)]+>?).*");
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            if(matcher.matches()) {
                String tag = matcher.group(1);
                word.set(tag);
                context.write(word, ONE);
                switch(TagClosurePartitioner.partition(tag, NUM_REDUCERS)){
                    case 0:
                        word.set("<closed tag number>");
                        context.write(word, ONE);
                        break;
                    case 1:
                        word.set("<open tag number");
                        context.write(word, ONE);
                }
            }
        }
    }
    /*
    numPartitions corresponds to job.setNumReduceTasks(NUM_REDUCERS);
     */
    public static class TagClosurePartitioner
    extends Partitioner<Text, IntWritable>{
        public static int partition(String str, int numPartitions){
            return str.endsWith(">") ? 0 : 1 % numPartitions;
        }
        public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
            return TagClosurePartitioner.partition(text.toString(), numPartitions);
        }
    }

    /* COMBINER
    One constraint that your Combiner will have, unlike a Reducer, is that the input/output
     key and value types must match the output types of your Mapper.
     */
    /*
    There are two opportunities for running the Combiner, both on the map side of processing. (A very good online reference
    is from Tom White's "Hadoop: The Definitive Guide"
     - https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-6/shuffle-and-sort )

    The first opportunity comes on the map side after completing the in-memory sort by key of each partition,
    and before writing those sorted data to disk. The motivation for running the Combiner at this point is to reduce the
    amount of data ultimately written to local storage. By running the Combiner here, we also reduce the amount of data
    that will need to be merged and sorted in the next step. So to the original question posted, yes, the Combiner is
    already being applied at this early step.

    The second opportunity comes right after merging and sorting the spill files. In this case, the motivation for running
    the Combiner is to reduce the amount of data ultimately sent over the network to the reducers. This stage benefits
    from  the earlier application of the Combiner, which may have already reduced the amount of data to be processed
    by this step.
     */
    public static class TestCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(new Text(key.toString()+">"), result);
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // Conf
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        // Job
        Job job = Job.getInstance(conf, "word count mr");
        // Jar
        job.setJarByClass(WordCount.class);
        // Mapper
        job.setMapperClass(TagTokenizerMapper.class);
        // Partitionner (default is HashPartitioner)
        job.setPartitionerClass(TagClosurePartitioner.class);
        job.setNumReduceTasks(NUM_REDUCERS);
        // Combiner  (RUNS AFTER PARTITIONER) -> /!\ Reducer can be used only if red(k: t1, v: t2) -> (k': t1, v': t2)
        job.setCombinerClass(TestCombiner.class);
        // Reducer
        job.setReducerClass(IntSumReducer.class);
        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}