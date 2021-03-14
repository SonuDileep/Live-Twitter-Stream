package pa2;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;

public class TwitterSpout extends BaseRichSpout {

	 TwitterStream twitterStream;

	String consumerKey = "D84PiGAWNJYl66NxSh6YZiewb";
	String consumerSecret = "KDz1Xcx5vMNYOkKHm3H6WAnvq0vC2vYUQibk1yIAcyoCU0AMnW" ;
	String accessToken = "119358275-zhlzAufheAqOoAy3Vhco7bwDurNP0Rm2FklmGGFC";
	String accessTokenSecret = "JvBze7TfQ2oFIsRPLyMrLR9RqNFnyXvSsCKTsRrUh6x74" ;

	 SpoutOutputCollector collector;
	 LinkedBlockingQueue<Status> queue = null;

	@Override
	public void open(Map<String, Object> conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>( 1000 );
		this.collector = collector;

		StatusListener statusListener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer( status );
			}

			@Override
			public void onException(Exception ex) {}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {}

			@Override
			public void onStallWarning(StallWarning warning) {}

		};

		ConfigurationBuilder cb = new ConfigurationBuilder();
				               	cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);
		twitterStream = new TwitterStreamFactory(
				cb.build() ).getInstance();
		FilterQuery tweetFilterQuery = new FilterQuery().language("en");
		twitterStream.addListener(statusListener);
		twitterStream.filter(tweetFilterQuery);
		twitterStream.sample(); 
	}

	@Override
	public void nextTuple() {
		Status status = queue.poll();
		if ( status == null )
		{
			Utils.sleep( 50 );
		} else
		{
				for ( HashtagEntity e : status.getHashtagEntities() )
				{
					collector.emit( new Values( e.getText() ) );
				}
			}
		
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "hash" ) );
	}

}
