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
import javax.servlet.GenericServlet
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse

import org.apache.log4j.Logger

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.Context
import org.mortbay.jetty.servlet.ServletHolder

/**
 * An example showing Storm poetry
 *
 * Inspired by "Ruby Poetry" by Andrew McDonough
 * http://blog.andrewmcdonough.com/blog/2012/02/23/ruby-poetry/
 *
 * The file 'feeds.txt' used by this example is shamelessly lifted from his
 * github project that goes with that blog entry:
 * https://github.com/andrewmcdonough/ruby-poetry
 *
 */

public class PoetryExample {
    static main( args ) {
        StormTopology topology = new TopologyBuilder().with {
            setSpout( 'spout',   new LineFetchingSpout() )
            setBolt(  'clean',   new CleanFilterBolt()   ).shuffleGrouping( 'spout'   )
            setBolt(  'length',  new LengthFilterBolt()  ).shuffleGrouping( 'clean'   )
            setBolt(  'rhyme',   new RhymingBolt()       ).shuffleGrouping( 'length'  )
            setBolt(  'couplet', new CoupletBolt(), 1    ).fieldsGrouping(  'rhyme', new Fields( 'tail' ) )
            createTopology()
        }

        Config conf = new Config()
        conf.debug = true
        
        conf.maxTaskParallelism = 3

        def web = new RSSServer()

        LocalCluster cluster = new LocalCluster()
        cluster.submitTopology( 'poetry', conf, topology )
    
        Thread.sleep( 15000 )

        cluster.shutdown()
        web.stop()
    }
}

// A single instance webserver which will feed headlines to the LineFetchingSpout
class RSSServer extends GenericServlet {
    public static Logger LOG = Logger.getLogger( RSSServer )
    List titles
    Iterator titleIterator
    Server server = null
    int curr = 0

    public RSSServer() {
        server = new Server( 8080 )
        this.titles = RSSServer.class.getResource( '/feeds.txt' ).text.tokenize('\n').findAll { u ->
            u.take( 1 ) == 'h'
        }.collect { u ->
            try {
                LOG.info "Scanning $u"
                new URL( u ).withReader { r -> new XmlSlurper().parse( r ).channel.item.title*.text() }
            }
            catch( e ) {}
        }.flatten()

        Collections.shuffle( this.titles )
        this.titleIterator = titles.iterator()

        Context root = new Context( server, "/", Context.SESSIONS )
        root.addServlet( new ServletHolder( this ), "/*" )
        server.start()
    }

    void stop() {
        server?.stop()
    }

    synchronized String getNextItem() {
        titleIterator.hasNext() ? titleIterator.next() : ''
    }

    @Override
    public void service( ServletRequest req, ServletResponse res ) {
        res.writer.println getNextItem()
    }
}

class LineFetchingSpout extends BaseRichSpout {
    SpoutOutputCollector collector

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector
    }

    @Override
    public void nextTuple() {
        String line = new URL( 'http://localhost:8080/data' ).text.trim()
        if( line ) collector.emit( new Values( line ) )
    }

    @Override
    public void ack( Object id ) { }

    @Override
    public void fail( Object id ) { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'line' ) )
    }
}

// Throws away lines that are too short or long for poetry
class LengthFilterBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'line' ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String line = tuple.getString( 0 )
        if( line.length() > 30 && line.length() < 70 ) {
            collector.emit( new Values( line ) )
        }
    }
}

// Clean up our lines to remove tags, etc
class CleanFilterBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'line' ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String line = tuple.getString( 0 )
        String clean = line.replaceAll( /^(AUDIO|VIDEO): /, '' ) // Remove prefixes
                           .replaceAll( /^(.+?) \|.*$/, '\$1' )   // And authors
        collector.emit( new Values( clean ) )
    }
}

// Splits the end off a line, and returns this as a new component
class RhymingBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'tail', 'line' ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String line = tuple.getString( 0 )
        String tail = line.replaceAll( /[^a-zA-Z ]/, '' )[ -3..-1 ]
        collector.emit( new Values( tail, line ) )
    }
}

// Maintain a map of tail:line.  When we get a second value for a given tail,
// send them off
class CoupletBolt extends BaseBasicBolt {
    Map<String, String> rhymes = [:]

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( 'couplet' ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        String tail = tuple.getString( 0 )
        String line = tuple.getString( 1 )

        if( rhymes[ tail ] && rhymes[ tail ] != line ) {
            collector.emit( new Values( "\n\n${rhymes[ tail ]}\n$line\n\n" ) )
            rhymes[ tail ] = ''
        }
        else {
            rhymes[ tail ] = line
        }
    }
}
