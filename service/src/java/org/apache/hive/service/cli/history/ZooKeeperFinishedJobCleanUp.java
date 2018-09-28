package org.apache.hive.service.cli.history;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.LOG;
import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

public class ZooKeeperFinishedJobCleanUp extends Thread {

    private static final String HDFS_PATH = "hdfs://localhost:9000/user/hadoop/hello";

    private CuratorFramework zooKeeperClient;

    private HiveConf hiveConf;

    private FileSystem fileSystem;

    private ZkExecuteRecordService zkExecuteRecordService;

    private static final Long ZK_CLEANUP_FINISHED_JOB_INTERVAL =
            10000L;


    private static final Long ZK_CLEANUP_FINISHED_JOB_OUTDATED_THRESHOLD =
            1000 * 60 * 60 * 24L;

    public ZooKeeperFinishedJobCleanUp(HiveConf hiveConf) throws URISyntaxException, IOException {
        this.zooKeeperClient = CuratorFrameworkSingleton.getInstance(hiveConf);
        this.hiveConf = hiveConf;
        this.zkExecuteRecordService = new ZkExecuteRecordService(hiveConf);
        this.fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
    }

    public void run() {

        while (true) {
            try {
                try {
                    List<String> finishedNodeList = getFinishedJobIds();
                    for (String node : finishedNodeList) {
                        ExecuteRecord recordNode =
                                zkExecuteRecordService.getExecuteRecordByMD5Sql(node);
                        if (recordNode != null && nodeShouldBeDeleted(recordNode)) {
                            deleteOutdatedFinishedNode(node);
                            deleteFinishedRecord(node);
                            deleteResultFromHDFS(recordNode);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Deleted outdated job failed: " + e.getMessage());
                }

                Thread.sleep(ZK_CLEANUP_FINISHED_JOB_INTERVAL);

            } catch (Exception e) {
                LOG.error("Deleted outdated job failed: " + e.getMessage(), e);
            }
        }
    }

    private void deleteFinishedRecord(String node) throws Exception {
        String nodePath = buildNodePath(node, hiveConf.getVar(HiveConf.ConfVars.FINISHED_EXECUTION_ZOOKEEPER_NAMESPACE));
        LOG.info("start to delete node " + nodePath);
        zooKeeperClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
        LOG.info("finish delete node " + nodePath);
    }

    private void deleteResultFromHDFS(ExecuteRecord recordNode) throws IOException {
        Path filePath = new Path(recordNode.getRetUrl());
        if (fileSystem.exists(filePath)) {
            LOG.info("start to delete HDFS file: " + recordNode.getRetUrl());
            fileSystem.delete(filePath, true);
            LOG.info("finish delete HDFS file.");

        }
    }

    private void deleteOutdatedFinishedNode(final String node) throws Exception {
        String nodePath = buildNodePath(node, hiveConf.getVar(HiveConf.ConfVars.HIVE_SQL_HISTORY_ZOOKEEPER_NAMESPACE));
        LOG.info("start to delete node " + nodePath);
        zooKeeperClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
        LOG.info("finish delete node " + nodePath);
    }

    private String buildNodePath(String node, String path) {
        return ZOOKEEPER_PATH_SEPARATOR + path
                + ZOOKEEPER_PATH_SEPARATOR + node;
    }

    private boolean nodeShouldBeDeleted(ExecuteRecord recordNode) {
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
