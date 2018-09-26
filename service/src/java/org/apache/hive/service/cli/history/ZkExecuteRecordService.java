package org.apache.hive.service.cli.history;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.history.exception.ConnectZkException;
import org.apache.hive.service.cli.history.exception.CreateZkNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkExecuteRecordService implements ExecuteRecordService {

  private Logger logger = LoggerFactory.getLogger(getClass().getName());
  private CuratorFramework zooKeeperClient;
  private String sqlHistoryRootNamespace;
  private String operationRootNamespace;
  private String hiveServerRootNamespce;
  private String hostName;

  protected ZkExecuteRecordService(HiveConf hiveConf) {
    zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
    sqlHistoryRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE);
    operationRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.OPERATION_ZOOKEEPER_NAMESPACE);
    hiveServerRootNamespce = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    hostName = getHostName(hiveConf);
    createRootNamespaceIfNotExist(sqlHistoryRootNamespace);
    createRootNamespaceIfNotExist(operationRootNamespace);
  }

  private String getHostName(HiveConf hiveConf) {
    InetAddress serverIPAddress = null;
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

    return serverIPAddress.getHostName();
  }


  private String createRootNamespaceIfNotExist(String rootPath) {
    try {
      zooKeeperClient.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(rootPath);
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
    executeRecord.setHostName(hostName);
    String pathPrefix = sqlHistoryRootNamespace + ZOOKEEPER_PATH_SEPARATOR + executeRecord.getSql();
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
    String path = sqlHistoryRootNamespace + ZOOKEEPER_PATH_SEPARATOR + record.getSql();
    record.setHostName(hostName);
    byte[] data = ExecuteRecordFactory.convertRecordToBytes(record);
    try {
      zooKeeperClient.setData().forPath(path, data);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return record;
  }

  @Override
  public Optional<ExecuteRecord> getExecuteRecordBySql(String sql) {
    return searchNodeData(
        sqlHistoryRootNamespace,
        DigestUtils.md5Hex(sql).toUpperCase(),
        ExecuteRecordFactory::convertByteToRecord).map(obj -> (ExecuteRecord) obj);
  }

  @Override
  public Optional<ExecuteRecord> getExecuteRecordByMD5Sql(String md5Sql) {
    return searchNodeData(
        sqlHistoryRootNamespace,
        md5Sql,
        ExecuteRecordFactory::convertByteToRecord).map(obj -> (ExecuteRecord) obj);
  }

  @Override
  public void createOperationNode(ExecuteRecord record) {
    String pathPrefix = operationRootNamespace + ZOOKEEPER_PATH_SEPARATOR + record.getOperationId();
    try {
      zooKeeperClient.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(pathPrefix, record.getSql().getBytes());
      logger.info("Created a operation znode on ZooKeeper: " + pathPrefix);
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create znode: " + pathPrefix + " on ZooKeeper", e);
    }
  }

  private Optional<Object> searchNodeData(String prefixPath, String node, Function<byte[], Object> function) {
    String nodePath = prefixPath + ZOOKEEPER_PATH_SEPARATOR + node;
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
  public Optional<ExecuteRecord> getRecordByOperationId(String md5OperationId) {
    Optional<String> md5SqlOpt = getSqlByOperationId(md5OperationId);
    if (md5SqlOpt.isPresent()) {
      return getExecuteRecordByMD5Sql(md5SqlOpt.get());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public void deleteRecordNode(String md5SqlId) {
    String nodePath = sqlHistoryRootNamespace + ZOOKEEPER_PATH_SEPARATOR + md5SqlId;
    try {
      zooKeeperClient.delete().forPath(nodePath);
    } catch (Exception e) {
      logger.error("Delete record node: " + nodePath + " failed. " + e.getMessage());
    }
  }

  @Override
  public boolean isOriginalServerRestarted(ExecuteRecord record) {
    String originalServer = record.getHostName();
    try {
      Optional<String> serverNode = zooKeeperClient
          .getChildren()
          .forPath(hiveServerRootNamespce)
          .stream()
          .filter(nodeName -> nodeName.contains(originalServer))
          .findFirst();
      if (serverNode.isPresent()) {
        Stat nodeStat = getNodeStat(hiveServerRootNamespce + ZOOKEEPER_PATH_SEPARATOR + serverNode.get());
        return nodeStat.getCtime() > record.getStartTime();
      } else {
        return true;
      }
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed of node: " + hiveServerRootNamespce, e);
    }
  }

  @Override
  public void deleteOperationNode(String originalMD5OperationId) {
    String nodePath = operationRootNamespace + ZOOKEEPER_PATH_SEPARATOR + originalMD5OperationId;
    try {
      zooKeeperClient.delete().forPath(nodePath);
    } catch (Exception e) {
      logger.error("Delete operation node: " + nodePath + " failed. " + e.getMessage());
    }
  }

  private Stat getNodeStat(String nodeName) {
    try {
      return zooKeeperClient.checkExists().forPath(nodeName);
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed of node: " + nodeName, e);
    }
  }
}