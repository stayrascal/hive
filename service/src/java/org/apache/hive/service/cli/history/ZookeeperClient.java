package org.apache.hive.service.cli.history;

public class ZookeeperClient {
    private static ZookeeperClient ourInstance = new ZookeeperClient();

    public static ZookeeperClient getInstance() {
        return ourInstance;
    }

    private ZookeeperClient() {
    }
}
