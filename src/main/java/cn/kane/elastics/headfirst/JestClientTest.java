/*
 * @(#)JestClient.java	2013年11月30日
 *
 * @Company <Opportune Technology Development Company LTD.>
 */
package cn.kane.elastics.headfirst;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;


/**
 * @Project <CL-Allocation tool>
 * @version <1.0>
 * @Author  <Administrator>
 * @Date    <2013年11月30日>
 * @description 
 */
public class JestClientTest {

    public static void main(String[] args){
        String serverUri = "http://127.0.0.1:9200/" ;
        ClientConfig.Builder clientConfBuilder = new ClientConfig.Builder(serverUri) ;
        clientConfBuilder.multiThreaded(true);
        clientConfBuilder.discoveryEnabled(false);
        ClientConfig clientConf = clientConfBuilder.build();
        //client-factory
        JestClientFactory jestClientFactory = new JestClientFactory() ;
        jestClientFactory.setClientConfig(clientConf);
       //client 
        JestClient jestClient = jestClientFactory.getObject() ;
        //add
//        JestClientTest.addContents(jestClient);
        JestClientTest.search(jestClient);
    }
    
    public static void addContents(JestClient jestClient){
        for(int i=0;i<10;i++){
            News news = new News();
            news.setId(i+1);
            news.setTitle("TITLE-"+news.getId());
            news.setContent("CONTENT-"+news.getId());
            try {
                //add with bulk
                Bulk bulk = new Bulk.Builder().addAction(new Index.Builder(news).index("newsindex").type("newstype").build()).build();
                JestResult bulkContentsJestResult = jestClient.execute(bulk);
                System.out.println(bulkContentsJestResult.getJsonString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void search(JestClient jestClient){
      //serch
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "TITLE-1");
        searchSourceBuilder.query(queryBuilder);
        Search.Builder searchBuilder = new Search.Builder(searchSourceBuilder.toString());
        Search search = searchBuilder.build();
        JestResult searchJestResult;
        try {
            List<News> queryResult = null ;
            searchJestResult = jestClient.execute(search);
            queryResult = searchJestResult.getSourceAsObjectList(News.class);
            System.out.println(queryResult.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
