package com.netease.hbase.sample;

import com.alibaba.fastjson.JSONObject;
import com.netease.hbase.dao.CRUDDao;
import com.netease.hbase.exception.CRUDException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * Created by hzcaojiajun on 2017/5/2.
 */
public class Test {

    private static CRUDDao<MessageModel> dao = new CRUDDao<>(MessageModel.class);

    public static void main(String[] args) throws CRUDException {

        MessageModel model = getModel(1L, 2L, 123456L);
        boolean put = put(model);
        System.out.println(put);

        MessageModel messageModel = get(model.fromUid, model.toUid, model.timestamp);
        System.out.println(JSONObject.toJSONString(messageModel));

        boolean delete = delete(model.fromUid, model.toUid, model.timestamp);
        System.out.println(delete);

        List<MessageModel> modelList = getModelList(1L, 3L, 123L, 133L);
        boolean[] booleans = batchPut(modelList);
        System.out.println(JSONObject.toJSONString(booleans));
        System.out.println(booleans.length);

        List<MessageModel> scan = scan(1L, 3L, 123L, 133L + 1L);
        System.out.println(JSONObject.toJSONString(scan));
        System.out.println(scan.size());

        batchDelete(modelList);

        scan = scan(1L, 3L, 123L, 133L + 1L);
        System.out.println(JSONObject.toJSONString(scan));
        System.out.println(scan.size());
    }

    private static MessageModel getModel(Long fromUid, Long toUid, Long timetamp) {
        MessageModel model = new MessageModel();
        model.fromUid = fromUid;
        model.toUid = toUid;
        model.timestamp = timetamp;
        model.body = "hello";
        model.extra = new HashMap<>();
        model.extra.put("key1", "value1");
        model.extra.put("key2", "value2");
        return model;
    }

    private static List<MessageModel> getModelList(Long fromUid, Long toUid, Long startTimestamp, Long endTimestamp) {
        List<MessageModel> list = new ArrayList<>();
        for (long i=startTimestamp; i<=endTimestamp; i++) {
            list.add(getModel(fromUid, toUid, i));
        }
        return list;
    }


    private static boolean put(MessageModel model) throws CRUDException {
        return dao.put(model);
    }

    private static MessageModel get(Long fromUid, Long toUid, Long timestamp) throws CRUDException {
        MessageModel model = new MessageModel();
        model.fromUid = fromUid;
        model.toUid = toUid;
        model.timestamp = timestamp;
        boolean b = dao.get(model);
        if (b) {
            return model;
        } else {
            return null;
        }
    }

    private static boolean delete(Long fromUid, Long toUid, Long timestamp) throws CRUDException {
        MessageModel model = new MessageModel();
        model.fromUid = fromUid;
        model.toUid = toUid;
        model.timestamp = timestamp;
        CRUDDao<MessageModel> dao = new CRUDDao<>(MessageModel.class);
        return dao.delete(model);
    }

    private static boolean[] batchPut(List<MessageModel> list) throws CRUDException {
        return dao.batchPut(list);
    }

    private static boolean[] batchDelete(List<MessageModel> list) throws CRUDException {
        return dao.batchDelete(list);
    }

    private static List<MessageModel> scan(Long fromUid, Long toUid, Long startTimestamp, Long endTimestamp) throws CRUDException {
        return dao.getList(MessageModel.class, MessageModel.getScan(fromUid, toUid, startTimestamp, endTimestamp));
    }
}
