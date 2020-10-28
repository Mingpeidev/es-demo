package com.mao.esdemo;

import com.mao.esdemo.controller.EsController;
import com.mao.esdemo.entity.Book;
import com.mao.esdemo.util.EsUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class EsDemoApplicationTests {

    @Resource
    private EsUtil esUtil;

    @Autowired
    private EsController bookService;

    @Test
    void test() {
        RestHighLevelClient client=null;
        try {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("192.168.31.15", 9200, "http")));


            IndexRequest request = new IndexRequest("posts");
            request.id("1");
            String jsonString = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON);
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            if (client!=null){
                try {
                    client.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }
    }

    @Test
    public void getOne() {
        System.out.println(bookService.getById(1).toString());
    }

    @Test
    public void getAll() {
        List<Book> res = bookService.getAll();
        res.forEach(System.out::println);
    }

    @Test
    public void addOneTest() {
        bookService.putOne(new Book(1, 1, "格林童话"));
        bookService.putOne(new Book(2, 1, "美人鱼"));
    }

    @Test
    public void addBatchTest() {
        List<Book> list = new ArrayList<>();
        list.add(new Book(3, 1, "第一本书"));
        list.add(new Book(4, 1, "第二本书"));
        bookService.putList(list);
    }

    @Test
    public void deleteBatch() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(3);
        bookService.deleteBatch(list);
    }

    @Test
    public void deleteByQuery(){
        bookService.deleteByUserId(1);
    }

}
