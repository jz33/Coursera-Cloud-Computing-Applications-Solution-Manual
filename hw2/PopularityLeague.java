import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
/**
 * Similar to "TopPopularLinks"
 */
public class PopularityLeague extends Configured implements Tool {

    private static final String DEL = ": ";
    private static final String AND = "###";
    private static final IntWritable ZERO = new IntWritable(0);
    private static final IntWritable ONE = new IntWritable(1);
    
    private static final String[] split(String input){
        String[] res = new String[2];
        int pos = input.indexOf(AND);
        res[0] = input.substring(0,pos);
        res[1] = input.substring(pos+3);
        return res;
    }
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Title Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Popularity League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        
        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        Integer N;
        Set<Integer> leagues = new HashSet<>();
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            
            String path = conf.get("league");
            List<String> list = Arrays.asList(readHDFSFile(path, conf).split("\n"));
            for(String e : list){
                leagues.add(Integer.valueOf(e));
            }
        }

        /**
         * Map 1, filter and emit
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stk = new StringTokenizer(value.toString(), DEL);
            Integer parent = Integer.valueOf(stk.nextToken());
            if(leagues.contains(parent)){
                context.write(new IntWritable(parent), ZERO);
            }
            while(stk.hasMoreTokens()){
                Integer child = Integer.valueOf(stk.nextToken());
                if(leagues.contains(child)){
                    context.write(new IntWritable(child), ONE);
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable e : values){
                count += e.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, Text> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        
        /**
         * Map 2, do nothing
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text(key.toString() + AND + value.toString()));
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, Text, IntWritable, IntWritable> {
        Integer N;
        List<Pair<Integer,Integer>> toSort = new ArrayList<>();
            
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        
        /**
         * Reduce 2, single reducer, push everything into $toSort 
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text e : values){
                String strs[] = split(e.toString());
                toSort.add(new Pair<Integer,Integer>(Integer.valueOf(strs[1]), Integer.valueOf(strs[0])));
            }
        }
        
        /**
         * Sort
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(toSort.size() == 0) return;
            
            Collections.sort(toSort);
            Pair<Integer,Integer> e = toSort.get(0);
            Integer rank = 0;
            Integer prev = e.first;
            context.write(new IntWritable(e.second), ZERO);
            
            for(int i = 1;i < toSort.size();i++){
                e = toSort.get(i);
                if(e.first > prev){
                    rank = i;
                    prev = e.first;
                }
                context.write(new IntWritable(e.second), new IntWritable(rank));
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }
    
    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    /**
     * Modified
     * @param input
     * @return
     */
    public static Pair<Integer, String> fromString(String input){
        String arr[] = input.split("\\(|\\)|,");       
        return new Pair<Integer, String>(Integer.parseInt(arr[1]),arr[2]);
    }
        
    @Override
    public String toString() {
        return "(" + first + "," + second + ')';
    }
}
