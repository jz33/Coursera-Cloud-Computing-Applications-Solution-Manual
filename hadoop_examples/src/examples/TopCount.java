package examples;

import utilities.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;
/*
Top Count
*/
public class TopCount extends Configured implements Tool {
    
    public static final String DEL = "##";
    
    public static Tuple2<Integer, String> FromString(String t){
        int p = t.indexOf(DEL);
        return new Tuple2<Integer, String>(Integer.parseInt(t.substring(0,p)), t.substring(p+2));
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/examples/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Top Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(Map_WordCount.class);
        jobA.setReducerClass(Reduce_WordCount.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopCount.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Count");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(Map_TopCount.class);
        jobB.setReducerClass(Reduce_TopCount.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopCount.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
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
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable e : values){
                sum += e.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Map_TopCount extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        List<Tuple2<Integer, String>> toSort = new ArrayList<Tuple2<Integer, String>>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            Tuple2<Integer, String> p = new Tuple2<Integer, String>(Integer.valueOf(value.toString()), key.toString());
            toSort.add(p);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(toSort, Collections.reverseOrder());
            int copySize = Math.min(toSort.size(), N);
            String[] stringArray = new String[copySize];
            for(int i = 0;i<copySize;i++){
                stringArray[i] = toSort.get(i).toString(DEL);
            }
            context.write(NullWritable.get(), new TextArrayWritable(stringArray));
        }
    }

    public static class Reduce_TopCount extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            
            List<Tuple2<Integer,String>> toSort = new ArrayList<Tuple2<Integer,String>> ();
            
            for(TextArrayWritable arr : values){
                for(Writable w : arr.get()){
                    String e = ((Text)w).toString();
                    Tuple2<Integer,String> p = FromString(e);
                    toSort.add(p);
                }
            }
            Collections.sort(toSort, Collections.reverseOrder());
            
            for(Integer i = 0; i<N;i++){
                context.write(new Text(toSort.get(i)._2), new IntWritable(toSort.get(i)._1));
            }
        }
    }

}
