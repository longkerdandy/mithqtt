package com.github.longkerdandy.mithqtt.http.entity;


/**
 * Simple Result Response
 */
@SuppressWarnings("unused")
public class ResultEntity<T> {

    private T result;

    public ResultEntity(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}