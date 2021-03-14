package pa2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import pa2.CountBolt;
import pa2.WriterBolt;
import pa2.TwitterSpout;

public class TwitterTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( "twitter_spout", new TwitterSpout() );
		builder.setBolt( "tweet_count_bolt", new CountBolt(), 4)
				.setNumTasks( 4 )
				.fieldsGrouping( "twitter_spout", new Fields( "hash" ));
		builder.setBolt( " writer_bolt", new WriterBolt() )
				.globalGrouping( "tweet_count_bolt" );
		Config conf = new Config();
		conf.setDebug(false);
               if(args.length > 0)
		{
		conf.setNumWorkers( 4 );
		StormSubmitter.submitTopology( "parallel_twitter", conf,
		builder.createTopology() );
		}		
		else
		{
                                conf.setMaxTaskParallelism(1);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology( "non_parallel_twitter", conf, builder.createTopology() );
                              Thread.sleep(240000);
                               cluster.shutdown();
			}
}
}
