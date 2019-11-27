package com.tsg.lq.store;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tsguang on 2019/11/27.
 */
public class MessageStoreTest {

    private static Logger logger = LoggerFactory.getLogger(MessageStoreTest.class);

    private static final String DATA_DIR = "E:/test/lq";

    @Test
    public void test1()throws Exception{
        MessageStore messageStore = new DefaultMessageStore(DATA_DIR);

        String topic = "testTopic";

        CommitLog queueFile = messageStore.getQueueFile(topic);

//        long idx1 = queueFile.putAndReturnIdx("第一条消息");
//        long idx2 = queueFile.putAndReturnIdx("第二条消息");
//        System.out.println(idx1);
//        System.out.println(idx2);


        long idx1 = 4294967296L;
        long idx2 = 4294967297L;


        String str1 = queueFile.getString(idx1);
        String str2 = queueFile.getString(idx2);
        System.out.println(str1);

        System.out.println(str2);
        logger.info(str1);
        logger.info(str2);
    }

}
