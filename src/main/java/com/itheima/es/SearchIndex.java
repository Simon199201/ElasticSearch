package com.itheima.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
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
    public void testSearchById() {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        search(queryBuilder);
    }


    @Test
    public void testSearchByTerm() {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "确诊");
        search(queryBuilder);
    }

    private void search(QueryBuilder queryBuilder) {
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(5)
                .get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("查询结果总记录数：" + totalHits);

        //查询结果列表
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            System.out.println("打印文档对象: " + searchHit.getSourceAsString());
            System.out.println("取文档的属性：");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }

        //关闭client
        client.close();

    }

    @Test
    public void testQueryString() throws Exception {
        //创建一个querybuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("累计生效")
                .defaultField("title");
//        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("正式限制");
//        search(queryBuilder);
        searchForHighLight(queryBuilder,"title");
    }


    private void searchForHighLight(QueryBuilder queryBuilder, String highLightField) {
        HighlightBuilder field = new HighlightBuilder();
        field.field(highLightField);
        field.preTags("<em>");
        field.postTags("</em>");

        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(5)
                .highlighter(field)
                .get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("查询结果总记录数：" + totalHits);

        //查询结果列表
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            System.out.println("打印文档对象: " + searchHit.getSourceAsString());
            System.out.println("取文档的属性：");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println("高亮结果**************");
            System.out.println(highlightFields);
            HighlightField field1 = highlightFields.get(highLightField);
            if (field1 != null) {
                Text[] fragments = field1.getFragments();
                if (fragments != null) {
                    System.out.println("高亮结果是："+fragments[0].toString());
                }
            }

        }

        //关闭client
        client.close();

    }
}
