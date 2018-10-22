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
    public static class TagClosurePartitioner
    extends Partitioner<Text, IntWritable>{
        public static int partition(String str, int numPartitions){
            return str.endsWith(">") ? 0 : 1 % numPartitions;
        }
        public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
            return TagClosurePartitioner.partition(text.toString(), numPartitions);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
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
        job.setCombinerClass(IntSumReducer.class);
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