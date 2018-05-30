package old;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileReader;

public class LoadUserNames
{

    public static void main( String[] args )
    {

        // TODO Can probably multi-thread this to make it faster for larger datasets
        try {

            // reads the file
            FileReader fileReader = new FileReader(System.getProperty("user.dir") + "/data-import/src/main/resources/users-small.json");
            JsonReader jsonReader = new JsonReader(fileReader);
            JsonParser parser = new JsonParser();
            JsonObject obj = parser.parse(jsonReader).getAsJsonObject();

            // connects to the local Elasticsearch
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")));

            // Creates the index if it hasn't been created
            // Uncomment if you want to create the index
            CreateIndexRequest request1 = new CreateIndexRequest("lastfm-users");
            client.indices().create(request1);

            JsonArray data = obj.get("data").getAsJsonArray();
            for (int i = 0 ; i < data.size(); i++) {
                String username = data.getAsJsonArray().get(i).getAsJsonArray().get(9).getAsString();
                String playCount = data.getAsJsonArray().get(i).getAsJsonArray().get(14).getAsString();

                JsonObject esObj = new JsonObject();
                esObj.addProperty("username", username);
                esObj.addProperty("playcount", Integer.parseInt(playCount));
                System.out.println(esObj.toString());

                // inserts into the index
                IndexRequest request = new IndexRequest(
                    "lastfm-users", // index
                    "message",  // type
                    Integer.toString(i)); // document id
                request.source(esObj.toString(), XContentType.JSON);
                client.index(request);

            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
