package org.apache.hive.service.cli.history;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.LOG;
import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

public class ZooKeeperFinishedJobCleanUp extends Thread {

    private CuratorFramework zooKeeperClient;

    private HiveConf hiveConf;

    private ZkExecuteRecordService zkExecuteRecordService;

    private Configuration configuration;

    private static final Long ZK_CLEANUP_FINISHED_JOB_INTERVAL =
            10000L;


    private static final Long ZK_CLEANUP_FINISHED_JOB_OUTDATED_THRESHOLD =
            1000 * 60 * 60 * 24L;

    ZooKeeperFinishedJobCleanUp(HiveConf hiveConf) {
        this.zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
        this.hiveConf = hiveConf;
        this.zkExecuteRecordService = new ZkExecuteRecordService(hiveConf);
        this.configuration = new Configuration();
    }

    public void run() {

        try {

            LeaderSelector leaderSelector = new LeaderSelector(zooKeeperClient, "/leader", new LeaderSelectorListener() {

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception {

                    System.out.println(":I am leader.");
                    doCleanUpJob();
                    Thread.sleep(ZK_CLEANUP_FINISHED_JOB_INTERVAL);

                }

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

                }

            });

            leaderSelector.autoRequeue();
            leaderSelector.start();

        } catch (Exception e) {
            LOG.error("Deleted outdated job failed: " + e.getMessage(), e);
        }
    }

    private void doCleanUpJob() throws Exception {
        List<String> finishedJobIds = getFinishedJobIds();
        System.out.println(finishedJobIds);
        for (String node : finishedJobIds) {
            ExecuteRecord recordNode =
                    zkExecuteRecordService.getExecuteRecordByMD5Sql(node);
            if (recordNode != null && shouldBeDeleted(recordNode)) {
                deleteOutdatedFinishedNode(node);
                deleteFinishedRecord(node);
                deleteResultFromHDFS(recordNode);
            }
        }
    }

    private void deleteFinishedRecord(String node) throws Exception {
        String nodePath = buildNodePath(node, hiveConf.getVar(HiveConf.ConfVars.FINISHED_EXECUTION_ZOOKEEPER_NAMESPACE));
        deleteZooKeeperNode(nodePath);
    }

    private void deleteOutdatedFinishedNode(String node) throws Exception {
        String nodePath = buildNodePath(node, hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE));
        deleteZooKeeperNode(nodePath);
    }

    private void deleteZooKeeperNode(String nodePath) throws Exception {
        if (zooKeeperClient.checkExists().forPath(nodePath) != null) {
            LOG.info("start to delete node " + nodePath);
            zooKeeperClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
            LOG.info("finish delete node " + nodePath);
        }
    }

    private void deleteResultFromHDFS(ExecuteRecord recordNode) throws IOException {
        Path filePath = new Path(recordNode.getRetUrl());
        FileSystem fileSystem = filePath.getFileSystem(configuration);
        if (fileSystem.exists(filePath)) {
            LOG.info("start to delete HDFS file: " + recordNode.getRetUrl());
            fileSystem.delete(filePath, true);
            LOG.info("finish delete HDFS file.");

        }
    }

    private String buildNodePath(String node, String path) {
        return ZOOKEEPER_PATH_SEPARATOR + path
                + ZOOKEEPER_PATH_SEPARATOR + node;
    }

    private boolean shouldBeDeleted(ExecuteRecord recordNode) {
        if (recordNode.getEndTime() == null) {
            return false;
        }
        return new Date().getTime() - recordNode.getEndTime()
                > ZK_CLEANUP_FINISHED_JOB_OUTDATED_THRESHOLD;
    }

    private List<String> getFinishedJobIds() throws Exception {

        String finishedRecordPath = ZOOKEEPER_PATH_SEPARATOR
                + hiveConf.getVar(HiveConf.ConfVars.FINISHED_EXECUTION_ZOOKEEPER_NAMESPACE);

        return zooKeeperClient.getChildren().forPath(finishedRecordPath);
    }

}
