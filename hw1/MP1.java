package hw1;

import static java.lang.System.out;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MP1
{   
    Random generator;
    String userName;

    String inputFileName;
    Integer inputFileLineCount = 50000;

    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = { "i", "me", "my", "myself", "we", "our", "ours",
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

    private Integer cap = 20;
    private Set<String> stopWords;
    
    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public void dumpList(List<Map.Entry<String, Integer>> s) {
        out.println("list: ");
        for (Map.Entry<String, Integer> e : s) {
            out.println(e.getKey() + " : " + e.getValue());
        }
        out.println();
    }

    public void dumpQueue(Queue<Map.Entry<String, Integer>> q) {
        out.println("queue: ");
        while (q.size() != 0) {
            Map.Entry<String, Integer> e = q.poll();
            out.println(e.getKey() + " : " + e.getValue());
        }
        out.println();
    }

    public void dumpMap(Map<String, Integer> s) {
        out.println("list: ");
        for (Map.Entry<String, Integer> e : s.entrySet()) {
            out.println(e.getKey() + " : " + e.getValue());
        }
        out.println();
    }

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = inputFileLineCount;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    /**
     * Find large frequency elements by min heap
     * @param freq
     * @return
     */
    public String[] FindLargesByHeap(Map<String, Integer> freq) {
        String[] ret = new String[cap];

        /*
         * Construct a min heap
         * Notice the capacity
         */
        int capacity = cap + 20;
        PriorityQueue<Map.Entry<String, Integer>> queue = new PriorityQueue<Map.Entry<String, Integer>>(
                capacity, new Comparator<Map.Entry<String, Integer>>(){

                    /*
                     * Notice the comparison function is exactly reverse to that of sort
                     */
                    public int compare(Entry<String, Integer> o1,
                            Entry<String, Integer> o2) {
                        int comp = o1.getValue().compareTo(o2.getValue());
                        if (comp != 0) {
                            return comp;
                        }
                        else {
                            return o2.getKey().compareTo(o1.getKey());
                        }
                    }
                    
                });

        for (Map.Entry<String, Integer> e : freq.entrySet()) {
            queue.add(e);
            if(queue.size() > capacity)
                queue.poll();
        }
        
        // reverse & filter the queue
        ArrayList<String> arr = new ArrayList<String>(cap);
        
        while(queue.size() > 0){
            Map.Entry<String, Integer> e = queue.poll();
            if (stopWords.contains(e.getKey()) == false) {
                arr.add(e.getKey());
                //out.println(e + " : " + e.getValue());
            }
        }
        
        int size = arr.size();
        for(int i = 0;i < cap && size - 1 - i > -1;i++)
        {
            ret[i] = arr.get(size - 1 - i);
        }
        return ret;
    }

    /**
     * Find large frequency elements by sort
     * @param freq
     * @return
     */
    public String[] FindLargesBySort(Map<String, Integer> freq) {
        String[] ret = new String[cap];

        List<Map.Entry<String, Integer>> arr = new ArrayList<Map.Entry<String, Integer>>(
                freq.entrySet());
        Collections.sort(arr, new Comparator<Map.Entry<String, Integer>>(){

            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2) {
                int comp = o2.getValue().compareTo(o1.getValue());
                if(comp != 0){
                    return comp;
                } else {
                    return o1.getKey().compareTo(o2.getKey());
                }
            }        
        });

        for (int i = 0, j = 0; i < arr.size() && j < cap; i++) {
            String e = arr.get(i).getKey();
            if (stopWords.contains(e) == false) {
                ret[j++] = e;
                // out.println(e + " : " + arr.get(i).getValue());
            }
        }
        return ret;
    }

    public String[] process() throws Exception {
        String[] lines = new String[inputFileLineCount];
        int ctr = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(
                inputFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                lines[ctr++] = line;
            }
        } catch (Exception e) {
            out.println(e);
        }

        Integer[] indexes = getIndexes();
        ConcurrentHashMap<String, Integer> freq = new ConcurrentHashMap<String, Integer>();

        for (Integer i : indexes) {
            StringTokenizer stk = new StringTokenizer(lines[i], delimiters);
            while (stk.hasMoreElements()) {
                String e = stk.nextToken().trim().toLowerCase();

                Integer found = freq.get(e);
                if (found != null) {
                    freq.put(e, found + 1);
                }
                else {
                    freq.put(e, 1);
                }
            }
        }
        //dumpMap(freq);

        String[] ret = FindLargesBySort(freq);
        //String[] ret = FindLargesByHeap(freq);
        
        //String[] ret = new String[cap];
        return ret;
    }
    
    public void preprocess(){
        stopWords = new HashSet<String>();
        for (String e : stopWordsArray) {
            stopWords.add(e);
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            
            mp.preprocess();
            String[] topItems = mp.process();
            for (String item : topItems) {
                System.out.println(item);
            }
        }
    }
}
