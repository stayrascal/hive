package org.apache.hive.service.cli.history;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hive.service.cli.history.exception.ConnectZkException;
import org.apache.hive.service.cli.history.exception.CreateZkNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ZkExecuteRecordService implements ExecuteRecordService {

  private Logger logger = LoggerFactory.getLogger(getClass().getName());
  private HiveConf hiveConf;
  private CuratorFramework zooKeeperClient;
  private String sqlHistoryRootNamespace;

  public ZkExecuteRecordService(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
    sqlHistoryRootNamespace = createRootNamespaceIfNotExist();
  }

  private String createRootNamespaceIfNotExist() {
    String sqlHistoryRootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE);
    try {
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace);
      logger.info("Created the root name space: " + sqlHistoryRootNamespace + " on ZooKeeper for beelinesql");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        logger.error("Unable to create beelineSQL namespace: " + sqlHistoryRootNamespace + " on ZooKeeper", e);
        throw new CreateZkNodeException("Unable to create beelineSQL namespace: " + sqlHistoryRootNamespace + " on ZooKeeper", e);
      }
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create beelineSQL namespace: " + sqlHistoryRootNamespace + " on ZooKeeper", e);
    }
    return sqlHistoryRootNamespace;
  }

  @Override
  public ExecuteRecord saveExecuteRecord(String sql) {
    ExecuteRecord executeRecord = ExecuteRecordFactory.buildNewRecord(sql);
    String pathPrefix = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace
            + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + executeRecord.getSql();
    try {
      zooKeeperClient.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(pathPrefix, executeRecord.getNodeData());
      logger.info("Created a znode on ZooKeeper for executeRecord uri: " + pathPrefix);
    } catch (Exception e) {
      throw new CreateZkNodeException("Unable to create znode: " + pathPrefix + " on ZooKeeper", e);
    }
    return executeRecord;
  }

  @Override
  public ExecuteRecord updateExecuteRecord(PersistentEphemeralNode node, ExecuteRecord record) {
    try {
      node.setData(record.getNodeData());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return record;
  }

  @Override
  public Optional<ExecuteRecord> getExecuteRecordBySql(String sql) {
    String md5Sql = DigestUtils.md5Hex(sql).toUpperCase();
    String path = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + sqlHistoryRootNamespace
            + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + md5Sql;
    try {
      if (zooKeeperClient.checkExists().forPath(path) != null) {
        byte[] bytes = zooKeeperClient.getData().forPath(path);
        if (bytes != null && bytes.length > 0) {
          return Optional.of(ExecuteRecordFactory.parseRecordFromStrBytes(bytes));
        }
      }
    } catch (Exception e) {
      throw new ConnectZkException("Connect Zookeeper failed.", e);
    }
    return Optional.empty();
  }
}
