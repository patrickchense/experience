package com.netease.hbase.model;

import com.netease.hbase.model.annotation.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by hzcaojiajun on 2017/3/3.
 */
public abstract class AbstractHBaseModel {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHBaseModel.class);

    private static final String SEPARATOR = "|";

    private static final Map<Class, Map<Integer, Field>> cacheFieldMap = new ConcurrentHashMap<>();
    private static final Map<Class, Map<Integer, byte[]>> cacheColumnMap = new ConcurrentHashMap<>();
    private static final Map<Class, byte[]> familyMap = new ConcurrentHashMap<>();
    private static Map<Class, String> tableNameMap = new ConcurrentHashMap<>();
    private static final Map<Class, byte[]> mapColumnMap = new HashMap<>();
    private static Map<Class, Field> mapFieldMap = new HashMap<>();

    private void _getFields(List<Field> list, Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        if (fields != null) {
            Collections.addAll(list, fields);
        }
        Class superClazz = clazz.getSuperclass();
        if (!Objects.equals(superClazz.getName(), "java.lang.Object")) {
            _getFields(list, superClazz);
        }
    }

    private Field _getMapField() {
        if (mapFieldMap.containsKey(this.getClass())) {
            return mapFieldMap.get(this.getClass());
        } else {
            Field field = null;
            Map<Integer, Field> fieldMap = _getFieldMap();
            for (Field f : fieldMap.values()) {
                if (f.getAnnotation(HBaseMapField.class) != null) {
                    field = f;
                    break;
                }
            }
            mapFieldMap.put(this.getClass(), field);
            return field;
        }
    }

    private byte[] _getMapColumn() {
        try {
            if (mapColumnMap.containsKey(this.getClass())) {
                return mapColumnMap.get(this.getClass());
            } else {
                _getColumnMap();
                return mapColumnMap.get(this.getClass());
            }
        } catch (Exception e) {
            logger.error("get map column error, ex = {}", e.toString(), e);
        }
        return null;
    }

    //获取Field映射关系
    private Map<Integer, Field> _getFieldMap() {
        Map<Integer, Field> subMap = cacheFieldMap.get(this.getClass());
        if (subMap == null) {
            subMap = new ConcurrentHashMap<>();
            cacheFieldMap.put(this.getClass(), subMap);
        }
        if (subMap.isEmpty()) {
            List<Field> fields = new ArrayList<>();
            _getFields(fields, this.getClass());
            for (Field field : fields) {
                HBaseField annotation = field.getAnnotation(HBaseField.class);
                if (annotation != null) {
                    int id = annotation.id();
                    subMap.put(id, field);
                }
            }
        }
        return subMap;
    }

    //获取行映射关系
    private Map<Integer, byte[]> _getColumnMap() {
        try {
            Map<Integer, byte[]> subMap = cacheColumnMap.get(this.getClass());
            if (subMap == null) {
                subMap = new ConcurrentHashMap<>();
                cacheColumnMap.put(this.getClass(), subMap);
            }
            if (subMap.isEmpty()) {
                List<Field> fields = new ArrayList<>();
                _getFields(fields, this.getClass());
                for (Field field : fields) {
                    HBaseColumn annotation = field.getAnnotation(HBaseColumn.class);
                    if (annotation != null) {
                        int id = annotation.id();
                        field.setAccessible(true);
                        Class<?> clazz = field.getType();
                        if (clazz != byte[].class) {
                            throw new RuntimeException("HBaseColumn should be byte array");
                        }
                        byte[] bytes = (byte[]) field.get(this);
                        subMap.put(id, bytes);

                        if (field.getAnnotation(HBaseMapColumn.class) != null) {
                            mapColumnMap.put(this.getClass(), bytes);
                        }
                    }
                }
            }
            return subMap;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("_getColumnMap error", e);
        }
    }

    //获取family
    private byte[] _getFamily() {
        try {
            byte[] familyRaw = familyMap.get(this.getClass());
            if (familyRaw == null) {
                List<Field> fields = new ArrayList<>();
                _getFields(fields, this.getClass());
                for (Field field : fields) {
                    HBaseColumnFamily annotation = field.getAnnotation(HBaseColumnFamily.class);
                    if (annotation != null) {
                        field.setAccessible(true);
                        Class<?> clazz = field.getType();
                        if (clazz != byte[].class) {
                            throw new RuntimeException("HBaseColumnFamily should be byte array");
                        }
                        familyRaw = (byte[]) field.get(this);
                        familyMap.put(this.getClass(), familyRaw);
                    }
                }
            }
            return familyRaw;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("_getFamily error", e);
        }
    }

    /**
     * 获取当前model对应的表名
     * @return 表名
     */
    public String getTableName() {
        return getTableName(this.getClass());
    }

    /**
     * 获取表名的静态方法
     * @param clazz class对象
     * @param <T> 泛型，AbstractHBaseModel的子类
     * @return 表名
     */
    public static <T extends AbstractHBaseModel> String getTableName(Class<T> clazz) {
        String tableName = tableNameMap.get(clazz);
        if (tableName == null) {
            try {
                HBaseTableName annotation = clazz.getAnnotation(HBaseTableName.class);
                if (annotation == null) {
                    throw new RuntimeException("HBaseTableName missing");
                }
                tableName = annotation.name();
                tableNameMap.put(clazz, tableName);
            } catch (Exception e) {
                logger.error("getTableName error, ex = {}, class = {}", e.toString(), clazz, e);
            }
        }
        return tableName;
    }

    /**
     * rowKey的定义，子类需实现
     */
    public abstract byte[] getRowKey();

    /**
     * 基本检查，在生成Put/Delete/Get时会调用该方法进行检查，子类可重写
     */
    public void check() {
    }

    /**
     * 默认filter，在Get的时候设置
     * @return Filter
     */
    public Filter getDefaultFilter() {
        return null;
    }

    /**
     * 获取model对应的Put对象
     * @return Put对象
     */
    public Put toPut() {
        check();
        try {
            Map<Integer, Field> fieldMap = _getFieldMap();
            Map<Integer, byte[]> columnMap = _getColumnMap();
            byte[] family = _getFamily();

            Put put;
            byte[] rowKey = getRowKey();
            if (rowKey == null) {
                throw new RuntimeException("rowKey is null");
            }
            put = new Put(rowKey);
            for (Map.Entry<Integer, Field> entry : fieldMap.entrySet()) {
                Integer index = entry.getKey();
                Field field = entry.getValue();
                field.setAccessible(true);
                Object o = field.get(this);
                if (o == null) continue;
                byte[] columnRaw = columnMap.get(index);
                if (columnRaw == null) continue;
                if (o instanceof Long) {
                    put.add(family, columnRaw, Bytes.toBytes((Long) o));
                } else if (o instanceof Integer) {
                    put.add(family, columnRaw, Bytes.toBytes((Integer) o));
                } else if (o instanceof Short) {
                    put.add(family, columnRaw, Bytes.toBytes((Short) o));
                } else if (o instanceof String) {
                    put.add(family, columnRaw, Bytes.toBytes((String) o));
                } else if (o instanceof Float) {
                    put.add(family, columnRaw, Bytes.toBytes((Float) o));
                } else if (o instanceof Double) {
                    put.add(family, columnRaw, Bytes.toBytes((Double) o));
                } else if (o instanceof BigDecimal) {
                    put.add(family, columnRaw, Bytes.toBytes((BigDecimal) o));
                } else if (o instanceof Map) {
                    Map map = ((Map) o);
                    for (Object key : map.keySet()) {
                        if (key instanceof String) {
                            Object value = map.get(key);
                            if (value instanceof String) {
                                byte[] raw = Bytes.add(columnRaw, Bytes.toBytes(SEPARATOR), Bytes.toBytes((String) key));
                                put.add(family, raw, Bytes.toBytes((String)value));
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("not support field type");
                }
            }
            return put;
        } catch (Exception e) {
            logger.error("toInsertPut error, ex = {}", e.toString(), e);
            return null;
        }
    }

    /**
     * 将Result对象解析为model对象
     * @return 是否成功
     */
    public boolean parseResult(Result result) {
        if (result == null) {
            return false;
        }
        try {
            Map<Integer, Field> fieldMap = _getFieldMap();
            byte[] family = _getFamily();
            Map<Integer, byte[]> columnMap = _getColumnMap();
            boolean res = false;
            for (Map.Entry<Integer, Field> entry : fieldMap.entrySet()) {
                Integer index = entry.getKey();
                Field field = entry.getValue();
                field.setAccessible(true);
                if (field.get(this) != null) {
                    continue;
                }
                byte[] columnRaw = columnMap.get(index);
                if (columnRaw == null) continue;
                byte[] raw = result.getValue(family, columnRaw);
                if (raw == null) continue;
                Class<?> type = field.getType();
                if (type == Long.class) {
                    field.set(this, Bytes.toLong(raw));
                } else if (type == Integer.class) {
                    field.set(this, Bytes.toInt(raw));
                } else if (type == Short.class) {
                    field.set(this, Bytes.toShort(raw));
                } else if (type == String.class) {
                    field.set(this, Bytes.toString(raw));
                } else if (type == Float.class) {
                    field.set(this, Bytes.toString(raw));
                } else if (type == Double.class) {
                    field.set(this, Bytes.toString(raw));
                } else if (type == BigDecimal.class) {
                    field.set(this, Bytes.toString(raw));
                } else {
                    throw new IllegalArgumentException("not support field type");
                }
                res = true;//如果一个field都没有设置过，则返回false
            }
            byte[] mapColumn = _getMapColumn();
            Field field = _getMapField();
            if (mapColumn != null && field != null) {
                byte[] prefix = Bytes.add(mapColumn, Bytes.toBytes(SEPARATOR));
                while (result.advance()) {
                    Cell cell = result.current();
                    if (cell != null) {
                        byte[] qualifierArray = cell.getQualifierArray();
                        if (qualifierArray != null && Bytes.contains(qualifierArray, prefix)) {
                            byte[] keyRaw = Bytes.copy(qualifierArray,
                                    cell.getQualifierOffset() + prefix.length, cell.getQualifierLength() - prefix.length);
                            String key = Bytes.toString(keyRaw);
                            Object o = field.get(this);
                            if (o == null) {
                                o = new HashMap<>();
                                field.set(this, o);
                            }
                            ((Map)o).put(key, Bytes.toString(CellUtil.cloneValue(cell)));
                            res = true;
                        }
                    }
                }
            }
            return res;
        } catch (IllegalAccessException e) {
            logger.error("parseResult error, ex = {}", e.toString(), e);
            return false;
        }
    }

    /**
     * 获取model对应的Delete对象
     * @return Delete对象
     */
    public Delete toDelete() {
        check();
        byte[] rowKey = getRowKey();
        if (rowKey == null) {
            throw new RuntimeException("rowKey is null");
        }
        return new Delete(rowKey);
    }

    /**
     * 获取model对应的Get对象
     * @return Get对象
     */
    public Get toGet() {
        check();
        byte[] rowKey = getRowKey();
        if (rowKey == null) {
            throw new RuntimeException("rowKey is null");
        }
        Get get = new Get(rowKey);
        Filter defaultFilter = getDefaultFilter();
        if (defaultFilter != null) {
            Filter f = get.getFilter();
            if (f != null) {
                Filter filterList = new FilterList(f, defaultFilter);
                get.setFilter(filterList);
            } else {
                get.setFilter(defaultFilter);
            }
        }
        return get;
    }

    //将一个对象进行MD5 hash，作为一个工具方法由子类调用
    protected static byte[] md5Bytes(Object obj) {
        try {
            byte[] bytes = String.valueOf(obj).getBytes();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(bytes, 0, bytes.length);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("error while md5 hash");
        }
    }
}
