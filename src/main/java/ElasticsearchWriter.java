import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class ElasticsearchWriter {

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String ES_SCHEME = "http";

    private static final String CSV_PATH = "netflix_titles.csv";
    private static final String INDEX_NAME = "netflix_content";

    private final RestHighLevelClient client;

    public ElasticsearchWriter() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(ES_HOST, ES_PORT, ES_SCHEME)));
    }

    public void populateIndex() throws IOException {
        BufferedReader csvReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(CSV_PATH)));

        String[] columns = csvReader.readLine().split(",");
        String row = null;

        // read data from csv
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split("[\"]?,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)[\"]?|\"$");
            Map<String, Object> fieldsMap = new LinkedHashMap<>();

            // transform each row into a map of fields
            for (int i = 1; i < columns.length; i++) {
                if (i < data.length && !data[i].isEmpty()) {
                    String fieldName = columns[i];
                    switch (fieldName) {
                        case "dateAdded":
                            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MMMM d, u", Locale.ENGLISH);
                            LocalDate date = LocalDate.parse(data[i], dateFormatter);
                            fieldsMap.put(fieldName, date);
                            break;
                        case "releaseYear":
                            fieldsMap.put(fieldName, Integer.parseInt(data[i]));
                            break;
                        case "cast":
                        case "listedIn":
                            String[] arrayElements = data[i].split(", ");
                            fieldsMap.put(fieldName, arrayElements);
                            break;
                        default:
                            fieldsMap.put(fieldName, data[i]);
                            break;
                    }
                }
            }

            // save data to Elasticsearch
            IndexRequest request = new IndexRequest(INDEX_NAME).source(fieldsMap);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        }

        client.close();
    }
}
