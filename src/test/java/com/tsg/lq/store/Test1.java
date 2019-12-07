package com.tsg.lq.store;

import com.tsg.lq.service.ConsumeService;
import com.tsg.lq.service.LocalQueueManager;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by tsguang on 2019/12/7.
 */
public class Test1 {

    private static final String DATA_DIR = "E:/test/lq";
    private static final String topic = "testTopic";


    private LocalQueueManager manager;
    private ConsumeService consumeService;


    @Before
    public void testBefore(){
        try {
            manager = new LocalQueueManager(DATA_DIR);
            consumeService = new DefaultConsumeService(topic,manager);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1()throws  Exception{
        for (int i = 200;i < 300;i++){
            manager.saveToQueue(topic, String.format("第%s条消息", i));
        }
        Thread.sleep(1000000000);
    }

}
