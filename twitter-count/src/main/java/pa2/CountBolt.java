package pa2;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class CountBolt extends BaseRichBolt {


	private Map<String, Item> counts = new HashMap<String, Item>();
	private int bucket = 1;
	private long items = 0;
	private double epsilon = 0.005;
	private int capacity=( int ) ( 1 / epsilon );
	private double threshold = 0.006;
	private OutputCollector collector;
	private int emitFrequency = 10;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put( Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency );
		return conf;
	}

	@Override
	public void execute(Tuple input) {
		if ( input.getSourceComponent().equals( Constants.SYSTEM_COMPONENT_ID )
				&& input.getSourceStreamId()
						.equals( Constants.SYSTEM_TICK_STREAM_ID ) )
		{
					int total = counts.size();
					counts.values().removeIf( value -> value.frequency < ( threshold
						- epsilon ) / total );

					int size = counts.size() > 100 ? 100 : counts.size();
					if ( size == 0 )
					{
						return;
					}
					Map<String, Item> output = sortMapDecending( counts, size );
					for ( Entry<String, Item> entry : output.entrySet() )
					{
						collector.emit(new Values( entry.getKey(), entry.getValue().actual() ) );
					}
		} else
		{
			if ( ++items % capacity == 0 )
			{
				counts.values().removeIf( value -> value.deconstruct( bucket ) );
				++bucket;
			}
			insert( input );
		}
	}

	private void insert(Tuple input) {
		String s = input.getStringByField( "hash" );
		Item item = counts.get( s );
		if ( item == null )
		{
			item = new Item( bucket - 1 );
		}
		item.increment();
		counts.put( s, item );
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

	private final static class Item implements Comparable<Item> {

		private final int delta;
		private long frequency = 0L;

		private Item(int delta) {
			this.delta = delta;
		}

		private long actual() {
			return frequency + delta;
		}

		private void increment() {
			frequency++;
		}

		private boolean deconstruct(int currentBucket) {
			return frequency + delta <= currentBucket;
		}

		@Override
		public int compareTo(Item o) {
			return Long.compare( this.actual(), o.actual() );
		}

		@Override
		public String toString() {
			return "( " + frequency + ", " + delta + " )";
		}
	}
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "word", "count" ) );
	}

}
