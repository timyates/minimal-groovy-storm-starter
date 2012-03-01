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

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.Context
import org.mortbay.jetty.servlet.ServletHolder

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

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
 * A Wesbserver scans a list of RSS feeds for titles
 * The spouts then read titles from this webserver, and sends them on to a series of bolts
 *   - CleanFilterBolt   -- Removes prefixes and suffixes
 *   - LengthFilterBolt  -- Only lets titles withing a certain length pass
 *   - RhymingBolt       -- Emits a Tuple consisting of [ title[ -3..-1 ], title ]
 *   - CoupletBolt       -- Joins Tuples by the title[ -3..-1 ] stub and sends them to the Webserver
 */

public class PoetryExample {
    static main( args ) {
        StormTopology topology = new TopologyBuilder().with {
            setSpout( 'spout',   new LineFetchingSpout() )
            setBolt(  'clean',   new CleanFilterBolt()   ).shuffleGrouping( 'spout'   )
            setBolt(  'length',  new LengthFilterBolt()  ).shuffleGrouping( 'clean'   )
            setBolt(  'rhyme',   new RhymingBolt()       ).shuffleGrouping( 'length'  )
            setBolt(  'couplet', new CoupletBolt()       ).fieldsGrouping(  'rhyme', new Fields( 'tail' ) )
            createTopology()
        }

        Config conf = new Config()
        conf.debug = false
        
        conf.maxTaskParallelism = 3

        def web = new RSSServer()

        LocalCluster cluster = new LocalCluster()
        cluster.submitTopology( 'poetry', conf, topology )
    
        // Waith 15s for something to happen, then kill it
        Thread.sleep( 15000 )

        cluster.shutdown()
        web.stop()

        // Print out any poems we were sent
        web.poems.each {
            println "$it\n"
        }
    }
}

// A single instance webserver which will feed headlines to the LineFetchingSpout
class RSSServer extends HttpServlet {
    List titles
    Iterator titleIterator
    Server server = null
    int curr = 0
    List poems = []

    public RSSServer() {
        server = new Server( 8080 )

        // Get all the News article titles, and shuffle them
        this.titles = RSSServer.class.getResource( '/feeds.txt' )
                                     .text
                                     .tokenize('\n')
                                     .findAll { u -> u.take( 1 ) == 'h' }
                                     .collect { u ->
                                        try { new URL( u ).withReader { r -> new XmlSlurper().parse( r ).channel.item.title*.text() } }
                                        catch( e ) {}
                                     }.flatten()

        Collections.shuffle( this.titles )
        this.titleIterator = titles.iterator()

        // Then fire up a web server
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

    synchronized void addPoem( String poem ) {
        poems << poem
    }

    @Override
    public void doGet( HttpServletRequest req, HttpServletResponse res ) {
        if( req.getParameter( 'query' ) == 'get' ) {
            res.writer.println getNextItem()
        }
        else if( req.getParameter( 'query' ) == 'put' ) {
            addPoem( req.getParameter( 'poem' ) )
            res.writer.println 'Thanks'
        }
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
        String line = new URL( 'http://localhost:8080/?query=get' ).text.trim()
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
        String clean = line.replaceAll( /^(AUDIO|VIDEO): /, '' )  // Remove prefixes
                           .replaceAll( /^(.+?) \|.*$/, '\$1' )   // And authors
        collector.emit( new Values( clean ) )
    }
}

// Splits the 3 chars off the end of the title, and returns this as a new element of our tuple
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
// send them off as a joined rhyming couplet
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

        // If we have a match, and it's not just the same line repeated
        if( rhymes[ tail ] && rhymes[ tail ] != line ) {
            // Join the two together
            def rhyme = "${rhymes[ tail ]}\n$line"
            // Clear the match for this tail
            rhymes[ tail ] = ''
            // Send the rhyme to the webserver
            String resp = new URL( "http://localhost:8080/?query=put&poem=${java.net.URLEncoder.encode( rhyme )}" ).text
            // And send our tuple on down the topology
            collector.emit( new Values( rhyme ) )
        }
        else {
            // First title with this tail.  Store it for later
            rhymes[ tail ] = line
        }
    }
}
