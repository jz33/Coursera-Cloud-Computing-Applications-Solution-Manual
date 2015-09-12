
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {

    /**
    *@tuple : (String)
    */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split("[\\s~`!@#$%^&*(-)+=_:;'\",.<>?/\\\\0-9"+"\\]\\[\\}\\{]+");

        for(String w : words){
            w = w.trim();
            if(w.length() > 0){
                collector.emit(new Values(w));
            }
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
