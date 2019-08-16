package com.tsg.lq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 消息存储管理
 *
 * Created by tsguang on 2019/8/16.
 */
public class CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private static final int CACHE_SIZE = 100;  //缓存消息的带下，提高读的效率
    private String dataDir; //存放队列文件的目录
    private String queueName;   //对应的队列名称
    private ArrayList<MappedFile> fileList = new ArrayList();   //TODO tsg : linkedList?? 一个队列对应多个文件，一个文件对应一个MappedFile
    private MappedFile readFile;   //当前读取的文件
    private MappedFile writeFile;  //当前写入的文件
    private Lock lock = new ReentrantLock();
    private Condition notEmpty;
    private volatile boolean closed;

}
