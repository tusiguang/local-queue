package com.tsg.lq.store;

import org.junit.Test;

/**
 * Created by tsguang on 2019/8/17.
 */
public class CommitLogTest {

    private static final String DATA_DIR = "E:/test/lq";

    @Test
    public void test1()throws Exception{
        String topic = "testTopic";

        CommitLog commitLog = new CommitLog(DATA_DIR, topic);

        long idx1 = commitLog.putAndReturnIdx("第一条消息");
        long idx2 = commitLog.putAndReturnIdx("第二条消息");
        System.out.println(idx1);
        System.out.println(idx2);



    }

    @Test
    public void test2()throws Exception{
        long idx1 = 4294967296L;
        long idx2 = 4294967297L;

        String topic = "testTopic";

        CommitLog commitLog = new CommitLog(DATA_DIR, topic);

        String str1 = commitLog.getString(idx1);
        String str2 = commitLog.getString(idx2);
        System.out.println(str1);
        System.out.println(str2);


    }

}
