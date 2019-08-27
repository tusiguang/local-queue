package com.tsg.lq.store;

import java.io.IOException;
import java.util.List;

/**
 * Created by tsguang on 2019/8/22.
 */
public interface MessageStore {

    boolean load();

    void start() throws Exception;

    void shutdown();

    void destroy();

    void purgeFinishedFiles();

    void purgeClosedQueueFiles(String topic);


    /**
     * 获取topic列表
     * @return
     */
    List<String> getTopicList();

    CommitLog getQueueFile(String topic) throws IOException;

}
