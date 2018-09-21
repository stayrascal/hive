package org.apache.hive.service.cli.history;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hive.service.cli.history.exception.ConnectZkException;
import org.apache.hive.service.cli.history.exception.CreateZkNodeException;
import org.apache.hive.service.cli.history.exception.DeleteZkNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Optional;
import java.util.function.Function;

public class ZkExecuteRecordService implements ExecuteRecordService {

  private Logger logger = LoggerFactory.getLogger(getClass().getName());
  private CuratorFramework zooKeeperClient;
  private String sqlHistoryRootNamespace;
  private String operationRootNamespace;

  public ZkExecuteRecordService(HiveConf hiveConf) {
    zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
    sqlHistoryRootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE);
    operationRootNamespace = hiveConf.getVar(HiveConf.ConfVars.OPERATION_ZOOKEEPER_NAMESPACE);
    createRootNamespaceIfNotExist(sqlHistoryRootNamespace);
    createRootNamespaceIfNotExist(operationRootNamespace);

    /*InetAddress serverIPAddress = null;
    String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    }
    try {
      if (hiveHost != null && !hiveHost.isEmpty()) {
        serverIPAddress = InetAddress.getByName(hiveHost);
      } else {
        serverIPAddress = InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      throw new ServiceException(e);
    }

    String portString = null;
    int portNum = 0;
    if (HiveServer2.isHTTPTransportMode(hiveConf)) {
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }
    } else {
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }
    }

    String ServerInstanceURI = serverIPAddress.getHostName() + ":" + portNum;*/
  }

  private String createRootNamespaceIfNotExist(String rootPath) {
    try {
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(ZOOKEEPER_PATH_SEPARATOR + rootPath);
      logger.info("Created the root name space: " + rootPath + " on ZooKeeper!");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        logger.error("Unable to create root namespace: " + rootPath + " on ZooKeeper", e);
        throw new CreateZkNodeException("Unable to create root namespace: " + rootPath + " on ZooKeeper", e);
      }
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create root namespace: " + rootPath + " on ZooKeeper", e);
    }
    return sqlHistoryRootNamespace;
  }

  @Override
  public ExecuteRecord createRecordNode(String sql) {
    ExecuteRecord executeRecord = ExecuteRecordFactory.buildNewRecord(sql);
    String pathPrefix = ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace
            + ZOOKEEPER_PATH_SEPARATOR + executeRecord.getSql();
    byte[] data = ExecuteRecordFactory.convertRecordToBytes(executeRecord);
    try {
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(pathPrefix, data);
      logger.info("Created a znode on ZooKeeper for executeRecord uri: " + pathPrefix);
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create znode: " + pathPrefix + " on ZooKeeper", e);
    }
    return executeRecord;
  }

  @Override
  public ExecuteRecord updateRecordNode(ExecuteRecord record) {
    String path = ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace + ZOOKEEPER_PATH_SEPARATOR + record.getSql();
    byte[] data = ExecuteRecordFactory.convertRecordToBytes(record);
    try {
      zooKeeperClient.setData().forPath(path, data);
    } catch (Exception e) {
      e.printStackTrace();
    }

    createOperationNode(record.getOperationId(), record.getSql());
    return record;
  }

  @Override
  public Optional<ExecuteRecord> getExecuteRecordBySql(String sql) {
    return searchNodeData(
            sqlHistoryRootNamespace,
            DigestUtils.md5Hex(sql).toUpperCase(),
            ExecuteRecordFactory::convertByteToRecord).map(obj -> (ExecuteRecord) obj);
  }

  private void createOperationNode(String operationId, String sqlId) {
    String pathPrefix = ZOOKEEPER_PATH_SEPARATOR + operationRootNamespace
            + ZOOKEEPER_PATH_SEPARATOR + operationId;
    try {
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(pathPrefix, sqlId.getBytes());
      logger.info("Created a operation znode on ZooKeeper: " + pathPrefix);
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create znode: " + pathPrefix + " on ZooKeeper", e);
    }
  }

  private Optional<Object> searchNodeData(String path, String node, Function<byte[], Object> function) {
    String nodePath = ZOOKEEPER_PATH_SEPARATOR + path + ZOOKEEPER_PATH_SEPARATOR + node;
    try {
      if (zooKeeperClient.checkExists().forPath(nodePath) != null) {
        byte[] bytes = zooKeeperClient.getData().forPath(nodePath);
        if (bytes != null && bytes.length > 0) {
          return Optional.of(function.apply(bytes));
        }
      }
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed.", e);
    }
    return Optional.empty();
  }

  @Override
  public Optional<String> getSqlByOperationId(String operationId) {
    return searchNodeData(
            operationRootNamespace,
            operationId,
            bytes -> new String(bytes, Charset.forName("UTF-8"))).map(String::valueOf);
  }

  @Override
  public Optional<ExecuteRecord> getRecordByOperationId(String operationId) {
    return getSqlByOperationId(DigestUtils.md5Hex(operationId).toUpperCase())
            .map(sql -> getExecuteRecordBySql(sql).get());
  }

  @Override
  public void deleteRecordNode(String md5SqlId) {
    String nodePath = ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace + ZOOKEEPER_PATH_SEPARATOR + md5SqlId;
    try {
      zooKeeperClient.delete().forPath(nodePath);
    } catch (Exception e) {
      throw new DeleteZkNodeException("Delete node: " + nodePath + "failed.", e);
    }
  }
}