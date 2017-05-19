package com.netease.hbase.dao;

import com.netease.hbase.model.AbstractHBaseModel;
import com.netease.hbase.exception.CRUDException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created by hzcaojiajun on 2017/3/3.
 */
public class CRUDDao<T extends AbstractHBaseModel> extends AbstractDao {

    private static final Logger logger = LoggerFactory.getLogger(CRUDDao.class);
    private static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];

    public CRUDDao(Class<T> clazz) {
        super();
        String tableName = AbstractHBaseModel.getTableName(clazz);
        if (tableName != null) {
            try {
                getHTable(tableName);
            } catch (Exception e) {
                logger.error("init HTable connect fail, ex = {}", e.toString(), e);
            }
        }
    }

    private enum Exec {
        PUT,
        GET,
        DELETE,
        ;
    }

    private enum BatchExec {
        BATCH_PUT,
        BATCH_GET,
        BATCH_DELETE,
        ;
    }

    /**
     * put方法
     * @param model model
     * @return 成功 or 失败
     * @throws CRUDException 异常
     */
    public boolean put(T model) throws CRUDException {
        return model != null && exec(model, Exec.PUT);
    }

    /**
     * batchPut方法，一次性put多个对象
     * @param models 对象列表
     * @return 返回一个boolean数组，true代表对应的对象put成功，false代表失败
     * @throws CRUDException 异常
     */
    public boolean[] batchPut(List<T> models) throws CRUDException {
        if (models == null || models.isEmpty()) {
            return EMPTY_BOOLEAN_ARRAY;
        }
        return exec(models, BatchExec.BATCH_PUT);
    }

    /**
     * delete方法
     * @param model model
     * @return 成功 or 失败
     * @throws CRUDException 异常
     */
    public boolean delete(T model) throws CRUDException {
        return model != null && exec(model, Exec.DELETE);
    }

    /**
     * batchDelete方法，一次性删除多个对象
     * @param models 对象列表
     * @return 返回一个boolean数组，true代表对象的对象put成功，false代表失败
     * @throws CRUDException 异常
     */
    public boolean[] batchDelete(List<T> models) throws CRUDException {
        if (models == null || models.isEmpty()) {
            return EMPTY_BOOLEAN_ARRAY;
        }
        return exec(models, BatchExec.BATCH_DELETE);
    }

    /**
     * get方法
     * @param model model
     * @return 成功 or 失败
     * @throws CRUDException 异常
     */
    public boolean get(T model) throws CRUDException {
        return model != null && exec(model, Exec.GET);
    }

    /**
     * batchGet方法，一次性获取多个
     * @param models 对象列表
     * @return 返回一个boolean数组，true代表对象的对象put成功，false代表失败
     * @throws CRUDException 异常
     */
    public boolean[] batchGet(List<T> models) throws CRUDException {
        if (models == null || models.isEmpty()) {
            return EMPTY_BOOLEAN_ARRAY;
        }
        return exec(models, BatchExec.BATCH_GET);
    }

    /**
     * 查询scan出来的result数量
     * @param clazz 类型，用于获取表名
     * @param scan Scan对象
     * @return rowCount
     * @throws CRUDException 异常
     */
    public int rowCount(Class<T> clazz, Scan scan) throws CRUDException {
        int count = 0;
        if (scan == null) return count;
        ResultScanner scanner = null;
        HTableInterface table = null;
        String tableName = AbstractHBaseModel.getTableName(clazz);
        try {
            table = getHTable(tableName);
            scanner = table.getScanner(scan);
            if (scanner != null) {
                for (Result result : scanner) {
                    if (result != null) {
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            handlerException(clazz, e, "rowCount");
        } finally {
            close(tableName, scanner, table);
        }
        return count;
    }

    /**
     * 根据Scan获取一组对象
     * @param clazz clazz对象，用于实例化
     * @param scan Scan对象
     * @return 一组对象
     * @throws CRUDException 异常
     */
    public List<T> getList(Class<T> clazz, Scan scan) throws CRUDException {
        List<T> list = new ArrayList<>();
        if (scan == null) return list;
        ResultScanner scanner = null;
        HTableInterface table = null;
        String tableName = AbstractHBaseModel.getTableName(clazz);
        try {
            table = getHTable(tableName);
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T t = clazz.newInstance();
                boolean parseResult = t.parseResult(result);
                if (parseResult) {
                    list.add(t);
                }
            }
        } catch (Exception e) {
            handlerException(clazz, e, "scan");
        } finally {
            close(tableName, scanner, table);
        }
        return list;
    }

    /**
     * 策略模式执行CRUD操作
     * @param t 实例
     * @param exec 操作类型
     * @return 成功 or 失败
     * @throws CRUDException 异常
     */
    private boolean exec(T t, Exec exec) throws CRUDException {
        HTableInterface table = null;
        try {
            table = getHTable(t.getTableName());
            switch (exec) {
                case DELETE:
                    Delete delete = t.toDelete();
                    if (delete == null) return false;
                    table.delete(delete);
                    return true;
                case PUT:
                    Put put = t.toPut();
                    if (put == null) return false;
                    table.put(put);
                    return true;
                case GET:
                    Get get = t.toGet();
                    if (get == null) return false;
                    Result result = table.get(get);
                    boolean parseResult = t.parseResult(result);
                    if (parseResult) {
                        return true;
                    }
            }
        } catch (Exception e) {
            handlerException(t.getClass(), e, exec.toString());
        } finally {
            close(t.getTableName(), table);
        }
        return false;
    }

    private boolean[] exec(List<T> list, BatchExec exec) throws CRUDException {
        boolean[] res = new boolean[list.size()];
        HTableInterface table = null;
        try {
            table = getHTable(list.get(0).getTableName());
            switch (exec) {
                case BATCH_DELETE:
                    List<Delete> deletes = new ArrayList<>();
                    for (T t : list) {
                        deletes.add(t.toDelete());
                    }
                    Object[] result = new Object[list.size()];
                    table.batch(deletes, result);
                    for (int i = 0; i < result.length; i++) {
                        res[i] = result[i] != null;
                    }
                    return res;
                case BATCH_PUT:
                    List<Put> puts = new ArrayList<>();
                    for (T t : list) {
                        puts.add(t.toPut());
                    }
                    result = new Object[puts.size()];
                    table.batch(puts, result);
                    for (int i = 0; i < result.length; i++) {
                        res[i] = result[i] != null;
                    }
                    return res;
                case BATCH_GET:
                    List<Get> gets = new ArrayList<>();
                    for (T t : list) {
                        gets.add(t.toGet());
                    }
                    Result[] results = table.get(gets);
                    for (int i = 0; i < results.length; i++) {
                        T t = list.get(i);
                        res[i] = t.parseResult(results[i]);
                    }
                    return res;
            }
        } catch (Exception e) {
            handlerException(list.get(0).getClass(), e, exec.toString());
        } finally {
            close(list.get(0).getTableName(), table);
        }
        return res;
    }

    //处理各种异常的情况
    private void handlerException(Class clazz, Exception e, String desc) throws CRUDException {
        if (e instanceof DoNotRetryIOException) {
            onException(clazz, e, desc, CRUDException.Code.HBASE_NO_RETRY_ERROR, true);
        } else if (e instanceof HBaseIOException) {
            onException(clazz, e, desc, CRUDException.Code.HBASE_RETRY_ERROR, true);
        } else if (e instanceof IOException) {
            onException(clazz, e, desc, CRUDException.Code.IO_ERROR, true);
        } else {
            onException(clazz, e, desc, CRUDException.Code.UNKNOW_ERROR, false);
        }
    }

    //处理异常的情况
    private void onException(Class clazz, Exception e, String desc, int code, boolean isConfReload) throws CRUDException {
        if (isConfReload) {
            onHBaseConnectionFail();
        }
        String instanceName = clazz.getSimpleName();
        logger.error("{} {} fail, ex = {}", instanceName, desc, e.toString(), e);
        throw new CRUDException(code, desc + " error", e);
    }

    //关闭相关资源
    private void close(String tableName, Closeable... object) {
        for (Closeable o : object) {
            if (o != null) {
                try {
                    o.close();
                } catch (IOException e) {
                    String instanceName = o.getClass().getSimpleName();
                    logger.error("{} close fail, tableName = {}, ex = {}", instanceName, tableName,  e.toString(), e);
                }
            }
        }
    }
}
