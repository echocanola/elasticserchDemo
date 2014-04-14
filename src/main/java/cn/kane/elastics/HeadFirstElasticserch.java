package cn.kane.elastics;
import io.searchbox.annotations.JestId;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Search;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;


/*
 * @(#)HeadFirstElasticserch.java	2013年11月16日
 *
 * @Company <Opportune Technology Development Company LTD.>
 */

/**
 * @Project <CL-Allocation tool>
 * @version <1.0>
 * @Author  <Administrator>
 * @Date    <2013年11月16日>
 * @description 
 */
public class HeadFirstElasticserch {

    public static void main(String[] args) throws Exception {
        String serverUri = "http://127.0.0.1:9200/" ;
        //client-config
        ClientConfig.Builder clientConfBuilder = new ClientConfig.Builder(serverUri) ;
//        clientConfBuilder.addServer(serverUris);//u can add server
//        clientConfBuilder.maxTotalConnection(maxTotalConnection);//maxTotalConn
//        clientConfBuilder.maxTotalConnectionPerRoute(httpRoute,maxTotalConnection);//maxTotalConnPerRoute
        clientConfBuilder.multiThreaded(true);
        clientConfBuilder.discoveryEnabled(false);
        ClientConfig clientConf = clientConfBuilder.build();
        //client-factory
        JestClientFactory jestClientFactory = new JestClientFactory() ;
        jestClientFactory.setClientConfig(clientConf);
       //client 
        JestClient jestClient = jestClientFactory.getObject() ;

       //create index
//        CreateIndex.Builder createIndexBuilder = new CreateIndex.Builder("index-name") ;
//        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
//        settingsBuilder.put("number_of_shards",5);
//        settingsBuilder.put("number_of_replicas",1);
//        createIndexBuilder.settings(settingsBuilder.internalMap());
//        **********************************configuration**********************************
//        createIndexBuilder.settings(Map<String,String> settings);
//        createIndexBuilder.setHeader(Map<String,object> headers);
//        createIndexBuilder.setParameter(Map<String,object> parameters);
//        CreateIndex createIndex = createIndexBuilder.build() ;
////        createIndex.getData(gson);
//        JestResult addIndexJestResult = jestClient.execute(createIndex);
//        System.out.println(addIndexJestResult.getJsonString());
        
        //add content
        List<News> contents = new ArrayList<News>() ;
        for(int i=0;i<1000;i++){
            News news = new News();
            news.setId(i+1);
            news.setTitle("TITLE-"+news.getId());
            news.setContent("CONTENT-"+news.getId());
            contents.add(news);
        }
        
        //add with index
//        Index.Builder indexBuilder = new Index.Builder(contents)
//            .index("index-name")
//            .type("type-name");
//        Index index = indexBuilder.build();
//        JestResult indexContentsJestResult = jestClient.execute(index);
//        System.out.println(jestResult.getErrorMessage());
//        //add with bulk
//        Bulk bulk = new Bulk.Builder()
//            .defaultIndex("index-name")
//            .defaultType("type-name")
//            .addAction(new Index.Builder(contents).build())
//            .addAction(new Delete.Builder("index-name", "type-name", "1").build())
//            .build();
//        JestResult bulkContentsJestResult = jestClient.execute(bulk);
        
        //serch
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "TIT");
        searchSourceBuilder.query(queryBuilder);
        Search.Builder searchBuilder = new Search.Builder(searchSourceBuilder.toString());
        Search search = searchBuilder.build();
        List<News> queryResult = null ;
        JestResult searchJestResult = jestClient.execute(search);
        queryResult = searchJestResult.getSourceAsObjectList(News.class);
//        jestClient.executeAsync(search, new MyJestResultHandler());//async
        System.out.println(queryResult.size());
        jestClient.shutdownClient();
    }

}

class MyJestResultHandler implements JestResultHandler<JestResult>{

    public void completed(JestResult result) {
        System.out.println("work-done");
    }

    public void failed(Exception ex) {
        ex.printStackTrace();
    }
    
}

class News {
    @JestId
    private int id;
    private String title;
    private String content;

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
    }
}
