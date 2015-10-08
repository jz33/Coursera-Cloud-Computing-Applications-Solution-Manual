package utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public final class Tools{
    
    public static final String Delimiters = " \t,;.?!-:@[](){}_*/";
    
    public static final String[] IgnoredWords = { "i", "me", "my", "myself", "we", "our", "ours",
            "ourselves", "you", "your", "yours", "yourself", "yourselves",
            "he", "him", "his", "himself", "she", "her", "hers", "herself",
            "it", "its", "itself", "they", "them", "their", "theirs",
            "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "having", "do", "does", "did",
            "doing", "a", "an", "the", "and", "but", "if", "or", "because",
            "as", "until", "while", "of", "at", "by", "for", "with", "about",
            "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out",
            "on", "off", "over", "under", "again", "further", "then", "once",
            "here", "there", "when", "where", "why", "how", "all", "any",
            "both", "each", "few", "more", "most", "other", "some", "such",
            "no", "nor", "not", "only", "own", "same", "so", "than", "too",
            "very", "s", "t", "can", "will", "just", "don", "should", "now" };
            
    public static String readHDFSFile(String hdfsPath, Configuration conf) throws java.io.IOException{
        Path path = new Path(hdfsPath);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataInputStream file = fs.open(path);
        java.io.BufferedReader buf = new java.io.BufferedReader(new java.io.InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while((line = buf.readLine()) != null){
            everything.append(line + "\n");
        }
        return everything.toString();
    }
}
