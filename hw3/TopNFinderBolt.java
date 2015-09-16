import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.*;

import backtype.storm.task.TopologyContext;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class TopNFinderBolt extends BaseBasicBolt {

    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();
    private int N;

    private ConcurrentMap<String, Integer> map;

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    public void prepare(Map stormConf, TopologyContext context){
        map = new ConcurrentHashMap<String, Integer>();
    }
    /**
    *@tuple : (String, Integer)
    */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String s = tuple.getString(0);
        Integer i = tuple.getInteger(1);
        
        Integer val = map.get(s);
        if(val == null){
            map.put(s,i); 
        }
        else{
            map.put(s,i + val);
        }

        //reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            collector.emit(new Values(printMap()));
            lastReportTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("top-N"));
    }

    public String printMap() {
        StringBuffer sb = new StringBuffer();
        sb.append("top-words = [ ");
        
        Vector<Entry<String, Integer>> vec = new Vector<Entry<String, Integer>>(map.entrySet());
        
        Collections.sort(vec, new Comparator<Entry<String, Integer>>(){
            
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        
        int left = Math.max(vec.size() - N, 0);
        for(int i = left ; i < vec.size(); i++){
            sb.append("(" + vec.get(i).getKey() + " , " + vec.get(i).getValue().toString() + ") , ");
        }
        
        int lastCommaIndex = sb.lastIndexOf(",");
        sb.deleteCharAt(lastCommaIndex + 1);
        sb.deleteCharAt(lastCommaIndex);
        sb.append("]");
        return sb.toString();
    }
}
