package pa2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WriterBolt extends BaseRichBolt {
	private int emitFrequency = 10;
	private static final Logger LOG = LogManager.getLogger( WriterBolt.class );

	private final Map<String, Long> wordcount = new HashMap<String, Long>();

	private BufferedWriter writer;
	


	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put( Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency );
		return conf;
	}

	@Override
	public void execute(Tuple tuple) {
		if ( tuple.getSourceComponent().equals( Constants.SYSTEM_COMPONENT_ID )
				&& tuple.getSourceStreamId()
						.equals( Constants.SYSTEM_TICK_STREAM_ID ) )
		{
			write();
		} else
		{
			wordcount.put( tuple.getStringByField( "word" ),
					tuple.getLongByField( "count" ) );
		}

	}
//Sorting code adapted from stackoverflow
private static <K, V extends Comparable<V>> Map<K, V> sortMapDecending(
			Map<K, V> map, int size) {
		return map.entrySet().stream()
				.sorted( Map.Entry
						.comparingByValue( Comparator.reverseOrder() ) )
				.limit( size )
				.collect( Collectors.toMap( Map.Entry::getKey,
						Map.Entry::getValue, (e1, e2) -> e1,
						LinkedHashMap::new ) );
	}


	private void write() {
		int size = wordcount.size() > 100 ? 100 : wordcount.size();
		if ( size == 0 )
		{
			return;
		}
		StringBuilder write_file = new StringBuilder( Instant.now().toString() );
		write_file .append( " -<" );

		Map<String, Long> output = sortMapDecending( wordcount, size );

		wordcount.clear();


		for ( Entry<String, Long> entry : output.entrySet() )
		{
	//		write_file .append( entry.getKey() ).append( "><" );
//With value
			write_file .append( entry.getKey() ).append("-").append( entry.getValue()).append( "><" );
		}
		LOG.info( write_file .toString() + "\n\n" );
		try
		{
			writer.write( write_file .toString() + "\n\n" );
			writer.flush();
		} catch ( IOException e )
		{
			LOG.error( e.getMessage(), e );
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		try
		{
			writer = new BufferedWriter(new FileWriter( "/s/chopin/k/grad/sonudilp/PA2/apache-storm-2.1.0/examples/teite_new/twitter-count/output.txt", true ));
		} catch ( IOException e )
		{
			LOG.error( e.getMessage(), e );
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
