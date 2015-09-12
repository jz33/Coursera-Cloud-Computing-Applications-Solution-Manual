import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.*;

import backtype.storm.task.TopologyContext;

import java.util.*;
import java.util.concurrent.*;

/*
TBD
*/
public class TopNFinderBolt extends BaseBasicBolt {

    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();
    private int N;

    /**
    * @Values : String, Integer
    */
    private ConcurrentSkipListSet<Values> set;


    public TopNFinderBolt(int N) {
        this.N = N;
    }

    public void prepare(Map stormConf, TopologyContext context){
        set = new ConcurrentSkipListSet<Values>(new Comparator<Values>(){

            @Override
            public int compare(Values o1, Values o2) {
                return o1.get(1).compareTo(o2.get(1));
            }
            
        });
    }
    /**
    *@tuple : (String, Integer)
    */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String s = tuple.getString(0);
        Integer i = tuple.getInteger(1);
        
        set.add(new Values(s,i));
        
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
        
        Vector<Values> vec = new Vector<Values>();
        vec.addAll(set);
        
        int left = Math.max(vec.size() - N, 0);
        for(int i = left ; i < vec.size(); i++){
            sb.append("(" + vec.get(i).get(0) + " , " + vec.get(i).get(1).toString() + ") , ");
        }
        
        int lastCommaIndex = sb.lastIndexOf(",");
        sb.deleteCharAt(lastCommaIndex + 1);
        sb.deleteCharAt(lastCommaIndex);
        sb.append("]");
        return sb.toString();
    }
}
