package com.mao.esdemo.util;

import com.alibaba.fastjson.JSON;
import com.mao.esdemo.entity.EsEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/27 16:59
 * @description
 */
@Component
public class EsUtil {

    @Value("${es.host}")
    public String host;
    @Value("${es.port}")
    public int port;
    @Value("${es.scheme}")
    public String scheme;

    public static final String INDEX_NAME = "book-index";

    public static final String CREATE_INDEX = "{\n" +
            "    \"properties\": {\n" +
            "      \"id\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"userId\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"name\":{\n" +
            "        \"type\":\"text\"\n" +
            "      },\n" +
            "      \"url\":{\n" +
            "        \"type\":\"text\",\n" +
            "        \"index\": true\n" +
            "      }\n" +
            "    }\n" +
            "  }";

    private RestHighLevelClient client = null;

    /**
     * 初始化连接
     * 被@PostConstruct注释的方法将会在对应类注入到Spring后调用,确保index的生成
     */
    @PostConstruct
    public void init() {
        try {
            if (client != null) {
                client.close();
            }
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));

            //创建索引
            createIndex(INDEX_NAME, "book_alias");
        } catch (Exception e) {
            try {
                client.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            System.out.println("系统异常" + e);
        }
    }

    /**
     * 创建一个index
     *
     * @param indexName
     * @param aliasName
     */
    public void createIndex(String indexName, String aliasName) {

        if (this.indexExist(indexName)) {
            return;
        }

        CreateIndexRequest request = new CreateIndexRequest(indexName);

        //number_of_shards      是数据分片数，默认为5，有时候设置为3
        //number_of_replicas    是数据备份数，如果只有一台机器，设置为0
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0));
        //创建索引时创建文档类型映射
        request.mapping(CREATE_INDEX, XContentType.JSON);
        //为索引设置一个别名
        request.alias(new Alias(aliasName));

        try {
            //同步执行，还有异步执行方式createAsync
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

            if (!response.isAcknowledged()) {
                throw new RuntimeException("初始化失败");
            }
        } catch (IOException e) {
            System.out.println("系统异常" + e);
        }
    }

    /**
     * 判断某个index是否存在
     *
     * @param index
     * @return
     * @throws Exception
     */
    public boolean indexExist(String index) {
        GetIndexRequest request = new GetIndexRequest(index);

        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);

        boolean status = false;
        try {
            status = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("系统异常" + e);
        }
        return status;
    }

    /**
     * 插入/更新一条记录
     *
     * @param index
     * @param entity
     */
    public void insertOrUpdateOne(String index, EsEntity entity) {
        IndexRequest request = new IndexRequest(index);//索引请求对象

        request.id(entity.getId());
        //指定索引文档内容
        request.source(JSON.toJSONString(entity.getData()), XContentType.JSON);

        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            System.out.println("insertOrUpdateOne状态=" + response.status());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量插入数据
     *
     * @param index
     * @param list
     */
    public void insertBatch(String index, List<EsEntity> list) {
        BulkRequest request = new BulkRequest();

        for (EsEntity esEntity : list) {
            request.add(new IndexRequest(index)
                    .id(esEntity.getId())
                    .source(JSON.toJSONString(esEntity.getData()), XContentType.JSON));
        }

        try {
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

            System.out.println("insertBatch状态=" + response.status());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除单个数据
     *
     * @param index
     * @param id
     */
    public void deleteOne(String index, String id) {
        DeleteRequest request = new DeleteRequest(index, id);

        try {
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("系统异常" + e);
        }
    }

    /**
     * 批量删除数据
     *
     * @param index
     * @param idList
     * @param <T>
     */
    public <T> void deleteBatch(String index, Collection<T> idList) {
        BulkRequest request = new BulkRequest();

        idList.forEach(item -> request.add(new DeleteRequest(index, item.toString())));

        try {
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

            System.out.println("deleteBatch状态=" + response.status());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 使用SearchSourceBuilder进行搜索
     *
     * @param index
     * @param builder
     * @param c
     * @param <T>
     * @return
     */
    public <T> List<T> search(String index, SearchSourceBuilder builder, Class<T> c) {
        SearchRequest request = new SearchRequest(index);
        request.source(builder);
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            SearchHit[] hits = response.getHits().getHits();

            List<T> res = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                res.add(JSON.parseObject(hit.getSourceAsString(), c));
            }
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 按index名删除index
     *
     * @param index
     * @return
     */
    public boolean deleteIndex(String index) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);

            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

            if (response.isAcknowledged()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * delete by query
     *
     * @param index
     * @param builder
     */
    public void deleteByQuery(String index, QueryBuilder builder) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(builder);
        //设置批量操作数量,最大为10000
        request.setBatchSize(10000);
        request.setConflicts("proceed");
        try {
            client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
