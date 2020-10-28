package com.mao.esdemo.entity;

/**
 * @author Mingpeidev
 * @date 2020/10/27 17:08
 * @description
 */
public class EsEntity<T> {
    private String id;
    private T data;

    public EsEntity() {
    }

    public EsEntity(String id, T data) {
        this.data = data;
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
