package com.netease.hbase.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by hzcaojiajun on 2017/3/3.
 */
class AbstractDao {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDao.class);

    private static Configuration globalConf = null;
    private static HConnection _conn = null;
    private static boolean init = false;

    private static final String HBASE_CONF_FILE_LOCATION = "hbase.xml";

    AbstractDao() {
        synchronized (AbstractDao.class) {
            if (init)
                return;
            _reload();
            //定时更新kerberos认证，设定为5个小时更新一次
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        UserGroupInformation.getCurrentUser().reloginFromKeytab();
                        logger.info("HBase: relogin from keytab success!");
                    } catch (IOException e) {
                        logger.error("HBase: relogin from keytab fail! ex = {}", e.toString(), e);
                    }
                }
            }, 5, 5, TimeUnit.HOURS);
            init = true;
        }
    }

    /**
     * 获取表，包含重试逻辑
     * @param tableName 表名
     * @return 表
     */
    HTableInterface getHTable(String tableName) throws Exception {
        int tryCount = 0;
        while (tryCount < 3) {
            try {
                return _getTable(tableName);
            } catch (Exception ex) {
                tryCount ++;
                logger.error("get table fail at tryCount = {}, ex = {}, retry after 5 sec", tryCount, ex.toString(), ex);
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    logger.error("sleep error, ex = {}", e.toString(), e);
                }
                _reload();
            }
        }
        logger.error("getHTable from HBase fail! table = {}", tableName);
        throw new RuntimeException("getHTable error");
    }

    /**
     * 处理失败时的逻辑
     */
    void onHBaseConnectionFail(){
        logger.warn("HBase: connection fail, trigger reconnect...");
        _reload();
    }

    //释放连接
    private void _disposeConn() {
        if (_conn != null && !_conn.isClosed()) {
            try {
                _conn.close();
                logger.info("close HBase connection success!");
            } catch (IOException e) {
                logger.error("close HBase connection fail! ex = {}", e.toString(), e);
            }
        }
    }

    //获取连接
    private HConnection _getConn() throws IOException {
        if (_conn == null || _conn.isClosed()) {
            _conn = HConnectionManager.createConnection(globalConf);
        }
        return _conn;
    }

    //获取表（线程安全）
    private synchronized HTableInterface _getTable(String tblName) throws IOException {
        try {
            return _getConn().getTable(tblName);
        } catch (IOException e) {
            logger.warn("get HTable from connection fail! ex = {}", e.toString(), e);
            _disposeConn();
            throw e;
        }
    }

    //重新加载配置文件并且重新登录
    private void _reload() {
        try {
            _reloadConfFromFile();
            _kerberosLogin();
        } catch (Exception e) {
            logger.error("HBase: get configure from file fail! ex = {}", e.toString(), e);
        }
    }

    //加载配置文件
    private void _reloadConfFromFile() {
        URL url = Thread.currentThread().getContextClassLoader().getResource(HBASE_CONF_FILE_LOCATION);
        if (url == null)
            throw new RuntimeException("HBase configuration file = '" + HBASE_CONF_FILE_LOCATION + "' not found!");
        Configuration conf;
        try {
            conf = HBaseConfiguration.create();
        } catch (Exception ex) {
            logger.error("create HBaseConfiguration fail!", ex);
            conf = new Configuration();
        }
        conf.addResource(new Path(url.getPath()));
        logger.info("HBase: Get HBase configuration file from " + url.getPath());
        globalConf = conf;
    }

    //登录
    private void _kerberosLogin() {
        String keytabFile = globalConf.get("nim.hbase.security.keytab.file", null);
        String principal =  globalConf.get("nim.hbase.kerberos.principal", null);
        if (keytabFile != null && keytabFile.trim().length() > 0) {
            try {
                UserGroupInformation.setConfiguration(globalConf);
                UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
                logger.info("HBase: kerberos login success!");
            } catch (IOException e) {
                logger.error("HBase: kerberos login fail! ex = {}", e.toString(), e);
            }
        }
    }
}
