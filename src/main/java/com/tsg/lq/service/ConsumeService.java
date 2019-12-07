package com.tsg.lq.service;

import com.tsg.lq.common.Message;

/**
 * consume 与 topic 一对一关系
 *
 * Created by tsguang on 2019/11/27.
 */
public interface ConsumeService {

    void listen(Message message)throws Exception;

    /**
     * 获取并发数
     * @return
     */
    default int getConcurrentNum(){
        return 1;
    }

    /**
     * 获取重试时间间隔
     * @return
     */
    default long getRetryInterval(){
        return 1000;
    }

    /**
     * 获取重试次数
     * @return
     */
    default int getRetryTimes(){
        return 3;
    }

}
