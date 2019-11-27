package com.tsg.lq.service;

import com.tsg.lq.common.Message;

/**
 * consume 与 topic 一对一关系
 *
 * Created by tsguang on 2019/11/27.
 */
public interface ConsumeService {

    void consume(Message message);

}
