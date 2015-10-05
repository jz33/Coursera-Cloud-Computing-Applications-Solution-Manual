import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import java.util.HashMap;
import java.util.regex.Pattern;

public final class RandomForestMP {

    private static class ParsePoint implements Function<String, Vector> {
        private static final Pattern SPACE = Pattern.compile(",");

        public Vector call(String line) {
            String[] tok = SPACE.split(line);
            double[] point = new double[tok.length-1];
            for (int i = 1; i < tok.length; ++i) {
                point[i-1] = Double.parseDouble(tok[i]);
            }
            return Vectors.dense(point);
        }
    }
    
    private static class DataToPoint implements Function<String, LabeledPoint> {
        private static final Pattern SPACE = Pattern.compile(",");

        public LabeledPoint call(String line) throws Exception {
            String[] tok = SPACE.split(line);
            double label = Double.parseDouble(tok[tok.length-1]);
            double[] point = new double[tok.length-1];
            for (int i = 0; i < tok.length - 1; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            return new LabeledPoint(label, Vectors.dense(point));
        }
    }
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Usage: RandomForestMP <training_data> <test_data> <results>");
            System.exit(1);
        }
        String training_data_path = args[0];
        String test_data_path = args[1];
        String results_path = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("RandomForestMP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<LabeledPoint> train = sc.textFile(training_data_path).map(new DataToPoint());
        JavaRDD<Vector> test = sc.textFile(test_data_path).map(new ParsePoint());

        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        /*
        http://spark.apache.org/docs/latest/api/java/index.html
        */
        final org.apache.spark.mllib.tree.model.RandomForestModel model = 
        org.apache.spark.mllib.tree.RandomForest.trainClassifier(
            train,
            numClasses, 
            categoricalFeaturesInfo,
            numTrees,
            featureSubsetStrategy,
            impurity,
            maxDepth,
            maxBins,
            seed
        );
        
        JavaRDD<LabeledPoint> clusters = test.map(new Function<Vector, LabeledPoint>() {
            public LabeledPoint call(Vector points) {
                return new LabeledPoint(model.predict(points), points);
            }
        });

        clusters.saveAsTextFile(results_path);
        sc.stop();
    }
}
