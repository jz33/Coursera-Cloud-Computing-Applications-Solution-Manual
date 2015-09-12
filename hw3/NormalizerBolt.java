
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.task.TopologyContext;

import java.util.*;
import java.util.concurrent.*;

public class NormalizerBolt extends BaseBasicBolt {
    private List<String> commonWords = Arrays.asList("the", "be", "a", "an", "and",
      "of", "to", "in", "am", "is", "are", "at", "not", "that", "have", "i", "it",
      "for", "on", "with", "he", "she", "as", "you", "do", "this", "but", "his",
      "by", "from", "they", "we", "her", "or", "will", "my", "one", "all", "s", "if",
      "any", "our", "may", "your", "these", "d" , " ", "me" , "so" , "what" , "him" );
      
    private ConcurrentMap<String, Boolean> map = new ConcurrentHashMap<String, Boolean>();
    
    @Override
    public void prepare(Map stormConf, TopologyContext context){
        for(String s : commonWords){
            map.put(s,true);
        }
    }
    
    /**
    *@tuple : (String)
    */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //for(int i = 0;i<tuple.size();i++){
            //String word = tuple.getString(i).toLowerCase();
            String word = tuple.getString(0).toLowerCase();
            if(map.containsKey(word) == false){
                collector.emit(new Values(word));
            }
        //}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
