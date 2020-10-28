package com.mao.esdemo.controller;

import com.mao.esdemo.entity.Book;
import com.mao.esdemo.entity.EsEntity;
import com.mao.esdemo.util.EsUtil;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/27 17:13
 * @description
 */
@RestController
@Service
public class EsController {

    private String INDEX_NAME = "book-index";

    @Resource
    private EsUtil esUtil;

    /**
     * @param id 获取某一个
     */
    @GetMapping("/{id}")
    public Book getById(@PathVariable("id") int id) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new TermQueryBuilder("id", id));
        List<Book> res = esUtil.search(INDEX_NAME, builder, Book.class);
        if (res.size() > 0) {
            return res.get(0);
        } else {
            return null;
        }
    }

    /**
     * 获取全部
     */
    @GetMapping("/")
    public List<Book> getAll() {
        return esUtil.search(INDEX_NAME, new SearchSourceBuilder(), Book.class);
    }

    /**
     * 根据关键词搜索某用户下的书
     *
     * @param content 关键词
     */
    @GetMapping("/search")
    public List<Book> searchByUserIdAndName(int userId, String content) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.termQuery("userId", userId));
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", content));
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(10).query(boolQueryBuilder);
        return esUtil.search(INDEX_NAME, builder, Book.class);

    }

    /**
     * 单个插入
     *
     * @param book book
     */
    @PutMapping("/")
    public void putOne(@RequestBody Book book) {
        EsEntity<Book> entity = new EsEntity<>(book.getId().toString(), book);
        esUtil.insertOrUpdateOne(INDEX_NAME, entity);
    }

    /**
     * 批量插入
     *
     * @param books books
     */
    @PutMapping("/many")
    public void putList(@RequestBody List<Book> books) {
        List<EsEntity> list = new ArrayList<>();
        books.forEach(item -> list.add(new EsEntity<>(item.getId().toString(), item)));
        esUtil.insertBatch(INDEX_NAME, list);
    }

    /**
     * 批量删除
     *
     * @param list list
     */
    @DeleteMapping("/deleteBatch")
    public void deleteBatch(List<Integer> list) {
        esUtil.deleteBatch(INDEX_NAME, list);
    }

    /**
     * delete by query 根据用户id删除数据
     *
     * @param userId userId
     */
    @DeleteMapping("/userId/{userId}")
    public void deleteByUserId(@PathVariable("userId") int userId) {
        esUtil.deleteByQuery(INDEX_NAME, new TermQueryBuilder("userId", userId));
    }
}
