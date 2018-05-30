package old;

import com.google.gson.JsonObject;
import de.umass.lastfm.Track;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

//try to import lastfm-java methods
import de.umass.lastfm.User;

public class GetUserData {
//make user data index in elasticsearch , using usernames from elasticsearch and data from last.fm api
    public static int USERS_SIZE = 1000;
    public static String aggr_name = "unique_users";
    public static String namesFile = "lastfm-users-small";
    //public static String userDataFile = "lastfm-userdata-test";
    public static String tracksByUserFile = "lastfm-usertracks-test";
    public static String tagsByTrackFile = "lastfm-tracktags-test";

    //my last-fm info:
//    Application name	recommender
//    API key	685a323d182636518e80a296f620c8a2
//    Shared secret	6a3c1e6497036ab224ccfd5186e40537
//    Registered to	crodwell

    public static String APIKey = "685a323d182636518e80a296f620c8a2";

    public static void main( String[] args )
    {
        try {
            // connects to the local Elasticsearch
            RestHighLevelClient elasticClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")));

            // Creates the index if it hasn't been created
            // Uncomment if you want to create the index

            //elasticClient.indices().create(new CreateIndexRequest(tracksByUserFile));
            //elasticClient.indices().create(new CreateIndexRequest(tagsByTrackFile));

            //get users from elasticsearch.
            // request to get users is:
//    {
//        "size" : 0,
//            "aggs" : {
//        "distinct_users" : {
//            "terms" : {
//                "field" : "username",
//                        "size" : 1000
//            }
//        }
//    }
//    }

            SearchSourceBuilder namesRequestBuilder = new SearchSourceBuilder();
            namesRequestBuilder.size(0);
            TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms(aggr_name).field("username").size(USERS_SIZE);
            namesRequestBuilder.aggregation(aggregationBuilder);

            SearchRequest getNamesRequest = new SearchRequest(namesFile);
            getNamesRequest.source(namesRequestBuilder);
            SearchResponse namesResponse = elasticClient.search(getNamesRequest);

            Aggregations resp_aggr = namesResponse.getAggregations();
            Terms resp_terms = resp_aggr.get(aggr_name);
            ArrayList<String> names = new ArrayList<String>();
            for (Terms.Bucket b: resp_terms.getBuckets()){
                names.add(b.getKeyAsString());
            }
            int results_count = names.size();
            //do we need to make sure it got all the users? it will only get USERS_SIZE of them in one go

            HashSet<Track> tracksInSet = new HashSet<>(); //try to get all the tracks in a hash - is this too big for java memory?

            Collection<Track> topTracks;
            int i=1;
            for (String name: names){
                topTracks = User.getTopTracks(name, APIKey);
                for (Track t: topTracks){
                    tracksInSet.add(t); //does this get unique tracks, or will it repeat tracks for different users?
                    JsonObject esObj = new JsonObject();
                    esObj.addProperty("username", name);
                    esObj.addProperty("trackname", t.getName());
                    sendIndexToElastic(elasticClient,esObj, tracksByUserFile, i); //does the id matter for sequential messages to same index?
                    i++;

                }
            }

            //make Hashmap of (track name -> tags) for all the tracks found
            // or just use elasticsearch if too big for hashmap in java
            HashMap<String, String> trackTags = new HashMap<>();
            i=1;
            for (Track t: tracksInSet) {
                Collection<String> Tags = t.getTags();
                for (String tag : Tags){
                    trackTags.put(t.getName(), tag);
                    JsonObject esObj = new JsonObject();
                    esObj.addProperty("tag", tag);
                    esObj.addProperty("trackname", t.getName());
                    sendIndexToElastic(elasticClient,esObj, tagsByTrackFile, i);
                    i++;
                }
            }

            elasticClient.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void sendIndexToElastic(RestHighLevelClient client, JsonObject esObj, String indexname, int id) throws IOException {
        IndexRequest request = new IndexRequest(
                indexname, // index
                "message",  // type
                Integer.toString(id)); // document id
        request.source(esObj.toString(), XContentType.JSON);
        client.index(request);
    }


}
