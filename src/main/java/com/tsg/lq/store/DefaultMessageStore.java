package com.tsg.lq.store;

import com.tsg.lq.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tsguang on 2019/8/22.
 */
public class DefaultMessageStore implements MessageStore{

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class);
    private static final long PURGE_WAIT_TIME = 1800000L;   //间隔多久检查清楚一次队列文件
    private final ConcurrentHashMap<String, CommitLog> map = new ConcurrentHashMap();
    private List<String> topics = new ArrayList();
    private static int timerCount = 1;

    private String dataDir;
    private Timer timer;


    /**
     * TODO 感觉应该模仿tomcat, 生命周期管理！！！ 下版本待定吧
     * @param dataDir
     */
    public DefaultMessageStore(String dataDir){
        this.dataDir = dataDir;
    }

    @Override
    public boolean load() {



        return false;
    }

    @Override
    public void start() throws Exception {
        File directoryFile = new File(this.dataDir);
        if (!directoryFile.exists()) {
            directoryFile.mkdirs();
        }

        File[] files = directoryFile.listFiles();
        int fileLength = files.length;

        File tempFile = null;
        String tempFileName = null;
        int tempIndex = 0;
        for (int i = 0; i < fileLength; i++) {
            tempFile = files[i];
            tempFileName = tempFile.getName();
            if (tempFileName.startsWith(Constants.DEFAULT_QUEUE_FILE_PREFIX)) {
                tempIndex = tempFileName.indexOf(Constants.DEFAULT_QUEUE_FILE_SUFFIX);
                if (tempIndex >= 0) {
                    if (CommitLog.checkCompetedOnOpen(tempFile.getAbsolutePath())) {
                        tempFile.delete();
                        logger.info("data file dropped, filename=" + tempFile.getAbsolutePath());
                    } else {
                        String topic = tempFileName.substring(Constants.DEFAULT_QUEUE_FILE_PREFIX.length(), tempIndex);
                        this.getQueueFile(topic);
                    }
                }
            }
        }

        this.timer = new Timer("DefaultMessageStore-Timer-" + timerCount);
        timerCount++;
        this.timer.schedule(new TimerTask() {
            public void run() {
                purgeFinishedFiles();
            }
        }, PURGE_WAIT_TIME, PURGE_WAIT_TIME);
        logger.info("DefaultMessageStore started");

    }

    @Override
    public synchronized void shutdown() {
        for (Map.Entry<String,CommitLog> entry : map.entrySet()){
            String topic = entry.getKey();
            CommitLog commitLog = entry.getValue();
            if (commitLog != null){
                commitLog.close();
                logger.info("queue {} closed", topic);
            }
        }

    }

    @Override
    public void destroy() {

    }

    @Override
    public synchronized void purgeFinishedFiles() {
        ArrayList<String> removeList = new ArrayList();
        Iterator<CommitLog> it = this.map.values().iterator();

        while (it.hasNext()) {
            CommitLog queue = it.next();
            if (queue.isClosed()) {
                removeList.add(queue.getTopic());
            } else {
                try {
                    queue.purge();
                } catch (IOException var5) {
                    logger.error("queue {} purge failed", queue.getTopic());
                }
            }
        }
        for (String topic : removeList){
            this.map.remove(topic);
            this.purgeClosedQueueFiles(topic);
        }

    }

    @Override
    public void purgeClosedQueueFiles(String topic) {
        File[] files = new File(this.dataDir).listFiles();
        for (File file : files){
            String name = file.getName();
            if (name.startsWith(Constants.DEFAULT_QUEUE_FILE_PREFIX + topic)) {
                int p = name.indexOf(Constants.DEFAULT_QUEUE_FILE_SUFFIX);
                if (p >= 0 && CommitLog.checkCompetedOnOpen(file.getAbsolutePath())) {
                    file.delete();
                    logger.info("data file dropped, filename=" + file.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public List<String> getTopicList() {
        return this.topics;
    }

    @Override
    public synchronized CommitLog getQueueFile(String topic) throws IOException {
        CommitLog queue = this.map.get(topic);
        if (queue != null){
            return queue;
        }
        synchronized (this.map){
            if (queue == null) {
                queue = new CommitLog(this.dataDir, topic);
                this.map.put(topic, queue);
                this.topics.add(topic);

            }
        }
        return queue;
    }
}
