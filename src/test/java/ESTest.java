import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ESTest {
    public Client client;

    //获取一个es的java客户端，通过该客户端连接es服务器
    @Before
    //节点客户端，传输客户端
    public void getClinet() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("cluster.name", "bigdata");
        Settings.Builder setting = Settings.builder().put(map);

        //创建传输客户端
        //添加多个节点，某一个宕机，可以切换到其他节点
        client = TransportClient.builder().settings(setting).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop01"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop02"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop03"), 9300));

    }

    @Test
    //创建Document, json串的形式
    public void creatDoc() {
        String doc1 = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"es是一个基于lucence的搜索服务器\"," +
                "\"content\":\"es是一个分布式的全文搜索引擎\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex("store", "books", "1")
                .setSource(doc1).get();
        //解析返回内容，进行展示
        System.out.println("索引:" + indexResponse.getIndex());
        System.out.println("类型:" + indexResponse.getType());
        System.out.println("ID:" + indexResponse.getId());
        System.out.println("是否创建成功:" + indexResponse.isCreated());
        client.close();
    }

    //使用map创建doc
    @Test
    public void createDoc_2() {
        Map<String, Object> doc_2 = new HashMap<String, Object>();
        doc_2.put("id", "2");
        doc_2.put("title", "学习大数据方法");
        doc_2.put("content", "多思考，多看源码");
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2")
                .setSource(doc_2).get();
        //解析返回内容，进行展示
        System.out.println("索引:" + indexResponse.getIndex());
        System.out.println("类型:" + indexResponse.getType());
        System.out.println("ID:" + indexResponse.getId());
        System.out.println("是否创建成功:" + indexResponse.isCreated());
        client.close();
    }

    //es提供的帮助类来创建
    @Test
    public void createDoc_3() throws Exception {
        XContentBuilder doc_3 = XContentFactory.jsonBuilder()
                .field("id", "3")
                .field("title", "es是java开发的")
                .field("content", "es是开源的")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex("bolg", "article", "3")
                .setSource(doc_3).get();
        //解析返回内容，进行展示
        System.out.println("索引:" + indexResponse.getIndex());
        System.out.println("类型:" + indexResponse.getType());
        System.out.println("ID:" + indexResponse.getId());
        System.out.println("是否创建成功:" + indexResponse.isCreated());
        client.close();
    }

    //搜索数据
    @Test
    public void getData_1() {
        GetResponse getResponse = client.prepareGet("store", "books", "1").get();
        System.out.println(getResponse.getSourceAsString());
        client.close();
    }

    //取多条数据
    @Test
    public void getData_2() {
        MultiGetResponse getRequest = client.prepareMultiGet()
                .add("store", "books", "1")
                .add("blog", "article", "3").get();

        //遍历获取每一条数据
        for (MultiGetItemResponse item : getRequest) {
            GetResponse response = item.getResponse();
            System.out.println(response.getSourceAsString());
        }
        client.close();
    }

    //更新数据，
    @Test
    public void updateData_1() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("store");
        updateRequest.type("books");
        updateRequest.id("2");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject().field("id", "2")
                .field("title", "es")
                .field("content", "它提供了一个分布式多用户的全文搜索引擎"));
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println("索引:" + updateResponse.getIndex());
        System.out.println("类型:" + updateResponse.getType());
        System.out.println("ID:" + updateResponse.getId());
        client.close();
    }

    //更新，先查询结果，如果有数据则进行更新，如果没有数据则插入一条新的数据
    @Test
    public void updateData_2() throws Exception {
        IndexRequest indexRequest = new IndexRequest("blog", "article", "6")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", 4)
                        .field("title", "插入新数据")
                        .field("content", "更新或插入content")
                        .endObject());
        //进行判断当前数据是否已经存在，设置更新的数据
        //执行更新或插入操作
        UpdateRequest updateRequest = new UpdateRequest("blog", "article", "6")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "仅仅更新title")
                        .endObject())
                .upsert(indexRequest);
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println("索引:" + updateResponse.getIndex());
        System.out.println("类型:" + updateResponse.getType());
        System.out.println("ID:" + updateResponse.getId());
        System.out.println("Version:" + updateResponse.getVersion());
        System.out.println("是否创建成功:" + updateResponse.isCreated());

        client.close();
    }

    //删除记录
    @Test
    public void delData() {
        DeleteResponse deleteResponse = client.prepareDelete("store", "books", "2").get();
        System.out.print("是否删除成功：" + deleteResponse.isFound());
        client.close();
    }

    //创建索引
    @Test
    public void createIndex() {
        CreateIndexResponse res = client.admin().indices().prepareCreate("blog5").get();
        System.out.println(res.toString());
        client.close();
    }

    //创建一个带有映射信息的索引，type，ID字段信息
    @Test
    public void createIndex2() throws Exception {
        XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("id")
                .field("type", "Integer")
                .field("", "")
                .endObject()
                .startObject()
                .field("", "")
                .endObject();
    }
    @Test
    public void searchTest(){
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.queryStringQuery("源码")).get();

        // 词条查询
        // 注：中文之间有默认分隔符，不能直接查询一个词语，只能单查一个字  要想查词语用BoolQueryBuilder
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.termQuery("content","源")).get();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("content","源"))
                .must(QueryBuilders.termQuery("content","码"));
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(boolQueryBuilder).get();

        // 通配符查询
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.wildcardQuery("content","多*")).get();

        // 模糊查询  英文单词可以写错，中文貌似不可以
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.fuzzyQuery("titlt","学习小数据")).get();

        //字段匹配查询
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.matchPhrasePrefixQuery("title","大数据")).get();

        // 范围查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                // from to  是一个范文 includeLower，includeUpper 忽略大小写
                .setQuery(QueryBuilders.rangeQuery("title").from("学习").to("方法")
                        .includeLower(true).includeUpper(true)).get();
        // 总结: 对中文不太友好

        SearchHits hits = searchResponse.getHits();
        // SearchHits遍历可迭代的集合
        Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()){
            SearchHit searchHit = it.next();
            // 打印结果
            System.out.println(searchHit.getSourceAsString());
        }
    }

}
