package examples;

import utilities.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;
/*
Standard Word Count
*/
public class WordCount extends Configured implements Tool{
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = Job.getInstance(this.getConf(), "Word Count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(Map_WordCount.class);
        job.setReducerClass(Reduce_WordCount.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setJarByClass(WordCount.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map_WordCount extends Mapper<Object, Text, Text, IntWritable>{
        Set<String> ignoredWords = new HashSet<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            
            for(String e : Tools.IgnoredWords){
                ignoredWords.add(e);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer stk = new StringTokenizer(value.toString(), Tools.Delimiters);
            while(stk.hasMoreTokens()){
                String e = stk.nextToken().trim().toLowerCase();
                if(ignoredWords.contains(e) == false){
                    context.write(new Text(e), new IntWritable(1));
                }
            }
        }
    }

    public static class Reduce_WordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable e : values){
                sum += e.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
