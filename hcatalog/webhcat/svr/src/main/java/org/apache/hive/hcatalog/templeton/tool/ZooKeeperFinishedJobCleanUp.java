package org.apache.hive.hcatalog.templeton.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ZooKeeperFinishedJobCleanUp extends Thread {

    protected Configuration appConf;

    private static ZooKeeperFinishedJobCleanUp thisClass = null;

    private static final String ZK_CLEANUP_FINISHED_JOB_INTERVAL =
            "templeton.zookeeper.cleanup.finished_job_interval";

    private static final String ZK_CLEANUP_FINISHED_JOB_THRESHOLD =
            "templeton.zookeeper.cleanup.finished_job_threshold";

    private static final Log log =
            LogFactory.getLog(ZooKeeperFinishedJobCleanUp.class);

    private static boolean isRunning = false;

    private ZooKeeperFinishedJobCleanUp(Configuration appConf) {
        this.appConf = appConf;
    }

    public static ZooKeeperFinishedJobCleanUp getInstance(Configuration appConf) {
        if (thisClass != null) {
            return thisClass;
        }
        thisClass = new ZooKeeperFinishedJobCleanUp(appConf);
        return thisClass;
    }

    public static void startInstance(Configuration appConf) {
        if (!isRunning) {
            getInstance(appConf).start();
        }
    }

    public void run() {

    }
}
