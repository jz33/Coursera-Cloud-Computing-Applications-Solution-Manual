
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

public class TopWordFinderTopologyPartC {

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        config.put("input",args[0]);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
 
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FileReaderSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 4).shuffleGrouping("spout");
        builder.setBolt("normalize", new NormalizerBolt(), 4).shuffleGrouping("split");
        builder.setBolt("count", new WordCountBolt(), 4).fieldsGrouping("normalize", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        Thread.sleep(2 * 60 * 1000);
        cluster.shutdown();
    }
}
