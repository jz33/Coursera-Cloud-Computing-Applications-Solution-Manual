
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.*;

/*
http://wpcertification.blogspot.com/2014/02/helloworld-apache-storm-word-counter.html
*/
public class FileReaderSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private TopologyContext context;

    private FileReader fileReader;
    private boolean isDone = false;
    
    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector){

        String filename = config.get("input").toString();
        try {
            fileReader = new FileReader(filename);
        } 
        catch (IOException e){
            e.printStackTrace();
        }

        this.context = context;
        this.collector = collector;
    }

    @Override
    public void nextTuple(){
        
        if(isDone){
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e){
                // interruption is normal
            }
        }
		
		BufferedReader reader = new BufferedReader(fileReader);
        String line;
		
        try {
            while ((line = reader.readLine()) != null){
                line = line.trim();
                if(line.length() > 0){
                    collector.emit(new Values(line));
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        finally{  
            isDone = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }

    @Override
    public void close() {
        try {
			fileReader.close();
		} 
        catch (IOException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
