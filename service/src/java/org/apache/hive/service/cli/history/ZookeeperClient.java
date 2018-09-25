package org.apache.hive.service.cli.history;

import org.apache.hadoop.hive.conf.HiveConf;

public class ZookeeperClient {
  private static ZkExecuteRecordService instance = null;

  public synchronized static ZkExecuteRecordService getInstance(HiveConf hiveConf) {
    if (instance == null) {
      instance = new ZkExecuteRecordService(hiveConf);
    }
    return instance;
  }

  public static ZkExecuteRecordService getInstance() {
    if (instance == null) {
      throw new IllegalArgumentException("ZkExecuteRecordService not ready");
    }
    return instance;
  }

  private ZookeeperClient() {
  }
}
