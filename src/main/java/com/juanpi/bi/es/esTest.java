package com.juanpi.bi.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;


/**
 * Created by wmky_kk on 2017/3/15.
 */
public class esTest {
    private static Client client = null;
    public static final Logger logger = Logger.getLogger(esTest.class);

    public static void main(String args[]){
    // 创建连接elasticsearch服务的client
        Properties properties = new Properties();
        String esPropertiesPath = "/es.properties";
        PropertyConfigurator.configure("log4j.properties");
        logger.info("kaikai start!");
        // 使用反射getResourceAsStream加载properites文件
        InputStream in = esTest.class.getResourceAsStream(esPropertiesPath); // 使用反斜杠\表示转义；斜杠'/'代表了工程ElasticSearch的根目录
        try{
            properties.load(in);
        } catch (IOException e){
            logger.error("kaikai 读取配置文件IO异常" + e.getMessage() + "\n" + e.getStackTrace());
        }
        String clusterName = properties.getProperty("es.clusterName");
        String port = properties.getProperty("es.port");
        String nodes = properties.getProperty("es.nodes");

        Settings settings = Settings.settingsBuilder().put("cluster.name",clusterName).build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build();

        for (String node : nodes.split(",")) {
            try {
                InetSocketTransportAddress iSTA = new InetSocketTransportAddress(InetAddress.getByName(node), Integer.valueOf(port));
                transportClient.addTransportAddress(iSTA);
            } catch (UnknownHostException e){
                logger.error("kaikai 连接ES时，未知主机名异常" + e.getMessage()+ "\n" + e.getStackTrace());
                logger.info("connect to es Host" + node + "is failed!");
            }
        }
        client = transportClient;
        // elasticsearch的java客户端，支持多种方式构建索引数据，这里有两种方式的代码示例：使用jsonbuilder构建数据
        /*IndexRequestBuilder indexRequestBuilder = client.prepareIndex("comment_index","comment_ugc","comment_12306");
        try {
        IndexResponse indexResponse = indexRequestBuilder.setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("author", "569874")
                .field("author_name", "riching")
                .field("mark", 232)
                .field("body", "北京不错，但是人太多了")
                .field("createDate", "20130801175520")
                .field("valid", true)
                .endObject())
                .setTTL(8000)
                .execute().actionGet();
            System.out.println(indexResponse.getId());
        } catch (IOException e){
            logger.info("indexRequestBuilder" + e.getMessage());
        }
*/
        // 第二种方法创建索引 json
        Student student = new Student("kaikai",1,20112160);
        ObjectMapper objectMapper = new ObjectMapper();
        String stu = null;
        try{
            stu = objectMapper.writeValueAsString(student);
            System.out.println("stu " + stu);
        } catch(Exception e){
            logger.info("对象转json出错" + e.getMessage());
        }
//        String jsonValue = "{\"name\":\"kaikai\",\"sex\":1,\"number\":20112160}";
//        JSON json = JSONObject.parseObject(jsonValue);
//        System.out.println(JSON.toJSONString(json,SerializerFeature.PrettyFormat));
        IndexResponse indexResponse = client.prepareIndex("dim_user_kkindex","info_type","stu_2160").setSource(stu).execute().actionGet();
        System.out.println(indexResponse.getIndex());

        // 4.根据ID获取数据
        GetResponse getResponse = client.prepareGet("dim_user_kkindex", "info_type", "stu_2160").execute().actionGet();
        logger.info("2160数据 " + getResponse.getSourceAsString());

        //5、查询索引
        SearchRequestBuilder builder = client.prepareSearch("comment_index").setTypes("comment_ugc").setSearchType(SearchType.DEFAULT).setFrom(0).setSize(100);
        BoolQueryBuilder qb = QueryBuilders.boolQuery().must(new QueryStringQueryBuilder("kaikai").field("name"));
//                .should(new QueryStringQueryBuilder("太多").field("body"));
        builder.setQuery(qb);
        SearchResponse response = builder.execute().actionGet();
        System.out.println("  " + response);
        System.out.println(response.getHits().getTotalHits());

        //6.删除索引。
//        DeleteResponse response2 = client.prepareDelete("comment_index", "comment_ugc", "comment_123674") .setOperationThreaded(false).execute().actionGet();
//        System.out.println(response2.getId());

    }
}
