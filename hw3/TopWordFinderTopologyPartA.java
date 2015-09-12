
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TopWordFinderTopologyPartA {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 4);
        builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 16).fieldsGrouping("split", new Fields("word"));

        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
    }
}