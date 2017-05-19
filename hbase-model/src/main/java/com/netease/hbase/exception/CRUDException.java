package com.netease.hbase.exception;

import com.alibaba.fastjson.JSONObject;

/**
 *
 * Created by hzcaojiajun on 2017/3/22.
 */
public class CRUDException extends Exception {

    public interface Code {
        int UNKNOW_ERROR = 500;
        int HBASE_NO_RETRY_ERROR = 1001;
        int HBASE_RETRY_ERROR = 1002;
        int IO_ERROR = 1003;
    }

    private int code;
    private String message;
    private Throwable cause;

    public CRUDException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
        this.cause = cause;
    }

    public CRUDException(int code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    public CRUDException(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", code);
        if (message != null) {
            jsonObject.put("message", message);
        }
        if (cause != null) {
            jsonObject.put("cause", cause.toString());
        }
        return jsonObject.toString();
    }
}
