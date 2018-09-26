package org.apache.hive.service.cli.history;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class YarnSingleton {
  private static YarnClient instance = null;

  public synchronized static YarnClient getInstance() {
    if (instance == null) {
      instance = new YarnClientImpl();
      instance.init(new YarnConfiguration());
      instance.start();
    }
    return instance;
  }

  private YarnSingleton() {
  }
}
