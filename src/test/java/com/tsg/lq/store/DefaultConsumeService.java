package com.tsg.lq.store;

import com.tsg.lq.common.Message;
import com.tsg.lq.service.ConsumeService;
import com.tsg.lq.service.LocalQueueManager;

/**
 * Created by tsguang on 2019/12/7.
 */
public class DefaultConsumeService implements ConsumeService {

    private LocalQueueManager manager;

    public DefaultConsumeService(String topic,LocalQueueManager manager) {
        this.manager = manager;
        manager.register(topic,this);
    }

    @Override
    public void listen(Message message) throws Exception{
        Thread.sleep(300);
        System.out.println(message.getText());
    }

    @Override
    public int getConcurrentNum() {
        return 1;
    }
}
