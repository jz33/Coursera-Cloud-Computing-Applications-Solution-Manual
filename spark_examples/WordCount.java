import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.*;

public final class WordCount{
    
    public static final String delimiters = " \t,;.?!-:@[](){}_*/";
    
    public static void main(String[] args){
        if (args.length < 2){
            System.err.println("Usage: WordCount <input_data_path> <output_data_path>");
            System.exit(1);
        }
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount"));

        JavaRDD<String> words = sc.textFile(args[0]).flatMap(new FlatMapFunction<String, String>(){

            public Iterable<String> call(String value){
                StringTokenizer stk = new StringTokenizer(value, delimiters);
                List<String> context = new ArrayList<String>();
                while(stk.hasMoreTokens()){
                    String e = stk.nextToken().trim().toLowerCase();                   
                    context.add(e);
                }
                return context; 
            }
        });
        
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
            
            public Tuple2<String, Integer> call(String s){ 
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
            
            public Integer call(Integer a, Integer b){ 
                return a + b;
            }
        });
        
        counts.saveAsTextFile(args[1]);
        sc.stop();
    }
}
