package com.itheima.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.itheima.es.Article;

public class ElasticSearchClientTest {
    private TransportClient client;

    @Before
    public void init() throws UnknownHostException {
        //1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my‐elasticsearch")
                .build();
        //2、创建一个客户端Client对象
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9303));
    }

    @Test
    public void createIndex() throws UnknownHostException {
        //1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my‐elasticsearch")
                .build();
        //2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9303));
        //3、使用client对象创建一个索引库
        client.admin().indices().prepareCreate("index_hello")
                //执行操作
                .get();
        //4、关闭client对象
        client.close();
    }

    @Test
    public void setMappings() throws IOException {
        //1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder().
                put("cluster.name", "my‐elasticsearch").build();
        //2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);

        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9303));
        //创建一个mappings信息
        /*
        {
            "hello": {
                "properties": {
                "id": {
                "store": true,
                "type": "long"
                },
                "title": {
                "analyzer": "ik_smart",
                "store": true,
                "type": "text"
                },
                "content": {
                "analyzer": "ik_smart",
                "store": true,
                "type": "text"
                }
                }
        }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        //使用client把mapping信息设置到索引库中
        client.admin().indices().preparePutMapping("index_hello")
                //设置要做映射的type
                .setType("article")
                //mapping信息，可以是XContentBuilder对象可以是json格式的字符串
                .setSource(builder)
                //执行操作
                .get();
        //关闭链接
        client.close();

    }

    @Test
    public void testAddDocument() throws IOException {
        //设置docment内容
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 2)
                .field("title", "韩国新增新冠肺炎确诊病例107例 累计确诊8086例")
                .field("content", "当地时间13日0时至14日0时，韩国新增107例新冠肺炎确诊病例，累计确诊8086例")
                .endObject();

        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId("2")
                .setSource(builder)
                .get();

        client.close();
    }

    @Test
    public void testAddDocument2() throws IOException {
        //设置docment内容
        Article article = new Article();
        article.setId(3);
        article.setContent("第三条信息");
        article.setTitle("第三条title");
        ObjectMapper objectMapper = new ObjectMapper();
        String content = objectMapper.writeValueAsString(article);
        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId(String.valueOf(article.getId()))
                .setSource(content)
                .get();

        client.close();

    }

    @Test
    public void testAddDocument3() throws IOException {
        //设置docment内容
        for (int i = 4; i < 100; i++) {
            Article article = new Article();
            article.setId(i);
            article.setTitle("第"+i+"条title"+"西班牙累计确诊6393例确诊病例 正式限制");
            article.setContent("第"+i+"条信息");
            ObjectMapper objectMapper = new ObjectMapper();
            String content = objectMapper.writeValueAsString(article);
            client.prepareIndex()
                    .setIndex("index_hello")
                    .setType("article")
                    .setId(String.valueOf(article.getId()))
                    .setSource(content)
                    .get();
        }
        client.close();

    }
}
