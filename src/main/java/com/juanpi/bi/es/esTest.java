package com.juanpi.bi.es;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


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

    }
    // elasticsearch的java客户端，支持多种方式构建索引数据，这里有两种方式的代码示例：使用jsonbuilder构建数据

}
