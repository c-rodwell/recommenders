package esclient;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.FileReader;

public class GetUserData {
//make user data index in elasticsearch , using usernames from elasticsearch and data from last.fm api
    public static int USERS_SIZE = 1000;
    public static String aggr_name = "unique users";
    public static String namesFile = "lastfm-users-small";
    public static String userDataFile = "lastfm-userdata-test";

    public static void main( String[] args )
    {
        try {
            // connects to the local Elasticsearch
            RestHighLevelClient elasticClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")));

            // Creates the index if it hasn't been created
            // Uncomment if you want to create the index
//            CreateIndexRequest request1 = new CreateIndexRequest(userDataFile);
//            elasticClient.indices().create(request1);


            //request to get users is:

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

//            JsonObject getusersObj = new JsonObject();
//            getusersObj.addProperty("size",0);
//            JsonObject aggs = new JsonObject();
//            JsonObject distinctUsers = new JsonObject();
//            JsonObject terms = new JsonObject();
//            terms.addProperty("field", "username");
//            terms.addProperty("size", "1000");
//            distinctUsers.add("terms", terms);
//            aggs.add("distinct_users", distinctUsers);
//            getusersObj.add("aggs", aggs);

            SearchSourceBuilder namesRequestBuilder = new SearchSourceBuilder();
            namesRequestBuilder.size(0);
            TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms(aggr_name).field("username").size(USERS_SIZE);
            namesRequestBuilder.aggregation(aggregationBuilder);

            SearchRequest getNamesRequest = new SearchRequest(namesFile);
            getNamesRequest.types("doc");
            getNamesRequest.source(namesRequestBuilder);
            SearchResponse namesResponse = elasticClient.search(getNamesRequest);

            Aggregations resp_aggr = namesResponse.getAggregations();
            Terms resp_terms = resp_aggr.get(aggr_name);

//            JsonArray data = obj.get("data").getAsJsonArray();
//            for (int i = 0 ; i < data.size(); i++) {
//                String username = data.getAsJsonArray().get(i).getAsJsonArray().get(9).getAsString();
//                String playCount = data.getAsJsonArray().get(i).getAsJsonArray().get(14).getAsString();
//
//                JsonObject esObj = new JsonObject();
//                esObj.addProperty("username", username);
//                esObj.addProperty("playcount", Integer.parseInt(playCount));
//                System.out.println(esObj.toString());
//
//                // inserts into the index
//                IndexRequest request = new IndexRequest(
//                        "lastfm-users-small", // index
//                        "message",  // type
//                        Integer.toString(i)); // document id
//                request.source(esObj.toString(), XContentType.JSON);
//                client.index(request);
//
//            }

            elasticClient.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
