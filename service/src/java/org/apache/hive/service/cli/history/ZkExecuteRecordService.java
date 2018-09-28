package org.apache.hive.service.cli.history;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.history.exception.ConnectZkException;
import org.apache.hive.service.cli.history.exception.CreateZkNodeException;
import org.apache.hive.service.cli.history.parse.ParseFunction;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.List;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

public class ZkExecuteRecordService implements ExecuteRecordService {

  private Logger logger = LoggerFactory.getLogger(getClass().getName());
  private CuratorFramework zooKeeperClient;
  private String sqlHistoryRootNamespace;
  private String operationRootNamespace;
  private String hiveServerRootNamespace;
  private String finishedRootNamespace;
  private String hostName;

  protected ZkExecuteRecordService(HiveConf hiveConf) {
    zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
    sqlHistoryRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE);
    operationRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.OPERATION_ZOOKEEPER_NAMESPACE);
    hiveServerRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    finishedRootNamespace = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.FINISHED_EXECUTION_ZOOKEEPER_NAMESPACE);

    hostName = getHostName(hiveConf);
    createRootNamespaceIfNotExist(sqlHistoryRootNamespace);
    createRootNamespaceIfNotExist(operationRootNamespace);
    createRootNamespaceIfNotExist(finishedRootNamespace);
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
  public ExecuteRecord getExecuteRecordBySql(String sql) {
    return getExecuteRecordByMD5Sql(DigestUtils.md5Hex(sql).toUpperCase());
  }

  @Override
  public ExecuteRecord getExecuteRecordByMD5Sql(String md5Sql) {
    return (ExecuteRecord) searchNodeData(sqlHistoryRootNamespace, md5Sql,
        new ParseFunction() {
          @Override
          public Object parse(byte[] bytes) {
            return ExecuteRecordFactory.convertByteToRecord(bytes);
          }
        }
    );
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

  private Object searchNodeData(String prefixPath, String node, ParseFunction parse) {
    String nodePath = prefixPath + ZOOKEEPER_PATH_SEPARATOR + node;
    try {
      if (zooKeeperClient.checkExists().forPath(nodePath) != null) {
        byte[] bytes = zooKeeperClient.getData().forPath(nodePath);
        if (bytes != null && bytes.length > 0) {
          return parse.parse(bytes);
        }
      }
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed.", e);
    }
    return null;
  }

  @Override
  public String getSqlByOperationId(String operationId) {
    return (String) searchNodeData(operationRootNamespace, operationId, new ParseFunction<String>() {
      @Override
      public String parse(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
      }
    });

  }

  @Override
  public ExecuteRecord getRecordByOperationId(String md5OperationId) {
    String md5SqlOpt = getSqlByOperationId(md5OperationId);
    if (md5SqlOpt != null) {
      return getExecuteRecordByMD5Sql(md5SqlOpt);
    } else {
      return null;
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
  public boolean isOriginalServerRestartedOrRemoved(ExecuteRecord record) {
    String originalServer = record.getHostName();
    try {
      List<String> nodes = zooKeeperClient.getChildren().forPath(hiveServerRootNamespace);
      for (String node : nodes) {
        if (nodes.contains(originalServer)) {
          Stat nodeStat = getNodeStat(hiveServerRootNamespace + ZOOKEEPER_PATH_SEPARATOR + node);
          return nodeStat.getCtime() > record.getStartTime();
        }
      }
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed of node: " + hiveServerRootNamespace, e);
    }
    return true;
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

  @Override
  public void archiveFinishedNode(String nodeName) {
    try {
      String newPath = finishedRootNamespace + ZOOKEEPER_PATH_SEPARATOR + nodeName;
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(newPath, nodeName.getBytes(Charset.forName("UTF-8")));
    } catch (Exception e) {
      logger.error("Archive node: " + nodeName + " failed.");
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