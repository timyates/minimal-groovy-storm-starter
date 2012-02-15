package storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.generated.StormTopology
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import backtype.storm.utils.Utils

/**
 * The simplest example of getting Storm running
 *
 * Taken from the Storm Starter: https://github.com/nathanmarz/storm-starter
 */

public class WordCountExample {
    public static void main(String[] args) throws Exception {
     
        StormTopology topology = new TopologyBuilder().with {
            setSpout( "spout", new RandomSentenceSpout(), 5 )
            setBolt(  "split", new SplitSentenceBolt(), 8 ).shuffleGrouping("spout")
            setBolt(  "count", new WordCountBolt(), 12 ).fieldsGrouping("split", new Fields("word") )
            createTopology()
        }

        Config conf = new Config()
        conf.debug = true
        
        if( args?.length ) {
            conf.numWorkers = 3
            StormSubmitter.submitTopology( args[ 0 ], conf, topology )
        }
        else {
            conf.maxTaskParallelism = 3

            LocalCluster cluster = new LocalCluster()
            cluster.submitTopology( "word-count", conf, topology )
        
            Thread.sleep( 10000 )

            cluster.shutdown()
        }
    }
}

class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector collector
    Random rand
    List sentences = [ "the cow jumped over the moon",
                       "an apple a day keeps the doctor away",
                       "four score and seven years ago",
                       "snow white and the seven dwarfs",
                       "i am at two with nature" ]

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector
        this.rand = new Random()
    }

    @Override
    public void nextTuple() {
        Utils.sleep( 100 )
        collector.emit( new Values( sentences[ rand.nextInt( sentences.size() ) ] ) )
    }

    @Override
    public void ack( Object id ) { }

    @Override
    public void fail( Object id ) { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( "word" ) )
    }
}

class SplitSentenceBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( "word" ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String line = tuple.getString( 0 )
        line.split().each { word ->
            collector.emit( new Values( word ) )
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null
    }
}

class WordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = [:]

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String word = tuple.getString( 0 )
        Integer count = counts[ word ] ?: 0
        counts[ word ] = ++count
        collector.emit( new Values( word, count ) )
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "word", "count" ) )
    }
}

