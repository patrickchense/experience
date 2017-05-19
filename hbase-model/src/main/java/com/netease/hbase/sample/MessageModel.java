package com.netease.hbase.sample;

import com.netease.hbase.model.AbstractHBaseModel;
import com.netease.hbase.model.annotation.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 *
 * Created by hzcaojiajun on 2017/5/2.
 */
@HBaseTableName(name = "message")
public class MessageModel extends AbstractHBaseModel {

    @HBaseField(id = 1)
    public Long fromUid;

    @HBaseField(id = 2)
    public Long toUid;

    @HBaseField(id = 3)
    public String body;

    @HBaseField(id = 4)
    public Long timestamp;

    @HBaseField(id = 5)
    @HBaseMapField
    public Map<String, String> extra;

    @HBaseColumnFamily
    private static final byte[] CF_D = Bytes.toBytes("d");

    @HBaseColumn(id = 1)
    private static final byte[] COL_D_FROM_UID = Bytes.toBytes("fromUid");

    @HBaseColumn(id = 2)
    private static final byte[] COL_D_TO_UID = Bytes.toBytes("toUid");

    @HBaseColumn(id = 3)
    private static final byte[] COL_D_BODY = Bytes.toBytes("body");

    @HBaseColumn(id = 4)
    private static final byte[] COL_D_TIMESTAMP = Bytes.toBytes("timestamp");

    @HBaseColumn(id = 5)
    @HBaseMapColumn
    private static final byte[] COL_D_EXTRA = Bytes.toBytes("extra");

    @Override
    public void check() {
        if (fromUid == null || toUid == null || timestamp == null) {
            throw new RuntimeException("fromUid/toUid/timestamp not be null");
        }
    }

    public static Scan getScan(Long fromUid, Long toUid, Long startTimestamp, Long endTimestamp) {
        byte[] start = Bytes.add(md5Bytes(fromUid), md5Bytes(toUid), Bytes.toBytes(startTimestamp));
        byte[] end = Bytes.add(md5Bytes(fromUid), md5Bytes(toUid), Bytes.toBytes(endTimestamp));

        Scan scan = new Scan(start, end);
        scan.setCaching(50);
        scan.setSmall(true);
        scan.addFamily(CF_D);
        return scan;
    }

    @Override
    public byte[] getRowKey() {
        check();
        return Bytes.add(md5Bytes(fromUid), md5Bytes(toUid), Bytes.toBytes(timestamp));
    }
}
