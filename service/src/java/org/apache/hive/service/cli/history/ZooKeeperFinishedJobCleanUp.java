package org.apache.hive.service.cli.history;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.LOG;
import static org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR;

public class ZooKeeperFinishedJobCleanUp extends Thread {

    private static final String FINISHED_RECORD_PATH_PREFIX = "/beelinesql/";

    private static final String FINISHED_RECORD_ID_PATH_PREFIX = "/finishedrecords";

    private static final String HDFS_PATH = "hdfs://localhost:9000/user/hadoop/hello";

    private CuratorFramework zooKeeperClient;

    private HiveConf hiveConf;

    private FileSystem fileSystem;

    private ZkExecuteRecordService zkExecuteRecordService;

    private static final Long ZK_CLEANUP_FINISHED_JOB_INTERVAL =
            10000L;


    private static final Long ZK_CLEANUP_FINISHED_JOB_OUTDATED_THRESHOLD =
            1000*60*60*24L;

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
                    List<String> deletedNodeList = new ArrayList<>();
                    System.out.println(finishedNodeList);
                    for(String node : finishedNodeList){
                        ExecuteRecord recordNode = zkExecuteRecordService.getExecuteRecordByMD5Sql(node);
                        if(recordNode != null && shouldDelete(recordNode)){
                            deleteOutdatedFinishedNode(node);
                            deletedNodeList.add(node);
                            deleteResultFromHDFS(recordNode);
                        }
                    }
                    updateFinishedNodeIdList(finishedNodeList, deletedNodeList);

                } catch (Exception e) {
                    LOG.error("Deleted outdated job failed: " + e.getMessage());
                }

                Thread.sleep(ZK_CLEANUP_FINISHED_JOB_INTERVAL);

            } catch (Exception e) {
                LOG.error("Deleted outdated job failed: " + e.getMessage(), e);
            }
        }
    }

    private void deleteResultFromHDFS(ExecuteRecord recordNode) throws IOException {
        Path filePath = new Path(recordNode.getRetUrl());
        if(fileSystem.exists(filePath)) {
            fileSystem.delete(filePath, true);
        }
    }

    private void updateFinishedNodeIdList(List<String> finishedNodeList, List<String> deletedNodeList) throws Exception {
        finishedNodeList.removeAll(deletedNodeList);
        String updateNodeIds = getUpdateNodeString(finishedNodeList);
        zooKeeperClient.setData().forPath(FINISHED_RECORD_ID_PATH_PREFIX,
                updateNodeIds.getBytes(Charset.forName("UTF-8")));
    }

    private String getUpdateNodeString(List<String> finishedNodeList) {
        if(CollectionUtils.isEmpty(finishedNodeList)){
            return "";
        }
        StringBuilder str = new StringBuilder();
        for(String node : finishedNodeList){
            str.append(node);
        }
        return str.toString();
    }

    private void deleteOutdatedFinishedNode(final String node) throws Exception {
        String nodePath = buildNodePath(node);
        LOG.info("start to delete node "+nodePath);
        zooKeeperClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
        LOG.info("finish delete node "+nodePath);
    }

    private String buildNodePath(String node) {
        return FINISHED_RECORD_PATH_PREFIX +node;
    }

    private boolean shouldDelete(ExecuteRecord recordNode) {
        if(recordNode.getEndTime() == null){
            return false;
        }
        return new Date().getTime() - recordNode.getEndTime() > ZK_CLEANUP_FINISHED_JOB_OUTDATED_THRESHOLD;
    }

    private List<String> getFinishedJobIds() throws IOException {

        String finishedRecordPath = ZOOKEEPER_PATH_SEPARATOR + hiveConf.getVar(HiveConf.ConfVars.FINISHED_EXECUTION_ZOOKEEPER_NAMESPACE);

        try {
            byte[] bytes = zooKeeperClient.getData().forPath(finishedRecordPath);
            String toBeDeletedIds = new String(bytes, Charset.forName("UTF-8"));
            ArrayList<String> nodeIds = new ArrayList<>();
            Collections.addAll(nodeIds, toBeDeletedIds.split(","));
            return nodeIds;
        } catch (Exception e) {
            throw new IOException("Can't get tracking children", e);
        }
    }

}
