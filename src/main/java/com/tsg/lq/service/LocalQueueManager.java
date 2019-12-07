package com.tsg.lq.service;

import com.tsg.lq.common.Message;
import com.tsg.lq.store.CommitLog;
import com.tsg.lq.store.DefaultMessageStore;
import com.tsg.lq.store.MessageStore;
import com.tsg.lq.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by tsguang on 2019/8/27.
 */
public class LocalQueueManager {

    private static Logger logger = LoggerFactory.getLogger(LocalQueueManager.class);

    /**
     * 消息存储服务
     */
    private MessageStore messageStore;

    /**
     * 拉取消息的请求参数，类似于Rocket mq里的pullRequest
     * 一个topic对应一个LinkedBlockingQueue<Message>
     */
    private final Map<String,LinkedBlockingQueue<Message>> pullRequestQueueMap = new ConcurrentHashMap<>();

    /**
     * 线程轮询执行需要执行的任务，pull,commit,rollback 等
     * 为避免一个topic设置过多并发消费，影响其他topic消费效率，此queue应一个topic只有一个runnable
     *
     * 可能多线程执行，LinkedBlockingQueue -> ConcurrentLinkedQueue
     */
    private ConcurrentLinkedQueue<Runnable> waitingRunnableList = new ConcurrentLinkedQueue<>();

    /**
     * 没有消息接收，处于等待中的队列
     */
    private ConcurrentLinkedQueue<String> waitingQueueNameList;

    /**
     * 消息订阅的消费者
     */
    private Map<String,ConsumeService> consumeServiceMap = new ConcurrentHashMap<>();

    private String dataDir;

    /**
     * 处理线程数
     */
    private int threadNum;

    /**
     * 处理任务队列大小
     */
    private int queueSize;

    /**
     * 无特别意义，本地队列表示，应用去取名区分
     */
    private static String queueTypeName = "local-queue";


    private ThreadFactory threadFactory;

    private ThreadPoolExecutor pool;

    /**
     * 发送线程，v1 - 使用单线程
     */
    private Thread sendThread;


    /**
     * 重试定时器
     */
    private Timer timer = new Timer(queueTypeName + "_retrytimer");

    private AtomicBoolean shutdown = new AtomicBoolean();

    private AtomicBoolean hasIOException = new AtomicBoolean();

    private AtomicBoolean beforeCloseFlag = new AtomicBoolean();


    private Lock lock = new ReentrantLock(false);
    private Condition hasNewData =  lock.newCondition();

    public LocalQueueManager(String dataDir)throws Exception{
        this(dataDir,1,20000);
    }

    public LocalQueueManager(String dataDir,int threadNum,int queueSize)throws Exception{
        this.dataDir = dataDir;
        this.threadNum = threadNum;
        this.queueSize = queueSize;
        initManager();
    }

    public void initManager()throws Exception {
        waitingQueueNameList = new ConcurrentLinkedQueue<String>();
        messageStore = new DefaultMessageStore(this.dataDir);
        messageStore.start();

        List<String> queueNames = messageStore.getTopicList();
        if (queueNames != null && queueNames.size() > 1){
            queueNames.forEach(item -> waitingQueueNameList.offer(item));
        }

        threadFactory = new NamedThreadFactory(queueTypeName);
        pool = new ThreadPoolExecutor(this.threadNum, this.threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(this.queueSize));
//        pool = new ThreadPoolExecutor(this.threadNum, this.threadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(this.queueSize), threadFactory);
        pool.prestartAllCoreThreads();
        sendThread = new Thread(queueTypeName + "_sendingThread"){
            @Override
            public void run() {
                sendData();
            }
        };
    }

    private void sendData() {
        lock.lock();
        while (!shutdown.get()) {
            try {
                sendDataInternal();
                if (!needRun()){
                    hasNewData.await(1000,TimeUnit.MILLISECONDS);
                }
            } catch (Throwable t) {
                logger.error("exception in sendData, e = " + t.getMessage(), t);
            }
        }
        lock.unlock();
    }

    private void sendDataInternal()throws Exception{

        //1、有需要commit,rollback的先处理掉

        while (!waitingRunnableList.isEmpty()){ //TODO tsg : 怪怪的，改成线程池处理？
            Runnable runnable = waitingRunnableList.poll();
            if (runnable != null){
                runnable.run();
            }
        }

        //发送消息

        pullRequestQueueMap.forEach((topic, linkedQueue) -> {
            Message message = linkedQueue.poll();
            try {
                if (message != null){
                    CommitLog queueFile = messageStore.getQueueFile(topic);
                    checkAndSend(topic, message, queueFile);
                }
            }catch (Exception e){
                logger.error("localQueue send message fail",e);
                //TODO tsg : 发送失败之后的处理，message 回队列，消息回滚/重试
            }
        });

    }

    private void sendByTopic(String topic){

    }

    private boolean checkAndSend(String topic,Message message,CommitLog commitLog)throws Exception{
        boolean hasData = true;
        long idx = commitLog.get(0);
        if (idx == -1) {
            hasData = false;
        } else {
            CompletableFuture.runAsync(() -> {
                String text = commitLog.getString(idx);
                message.setText(text);
                message.setIdx(idx);
                message.setSendCount(message.getSendCount() + 1);
                message.setCreateTime(System.currentTimeMillis());
                message.setUuid(UUID.randomUUID().toString().replaceAll("-", ""));
                consumeServiceMap.get(topic).listen(message);
            }).thenRunAsync(() -> commit(message), pool
            ).exceptionally(e -> {
                logger.error("consume message error",e);
                retry(message);
                return null;
            });
        }
        return hasData;
    }

    private boolean needRun() {
        //TODO tsg : 长时间没有消息处理时，暂停线程？
//        return !waitingRunnableList.isEmpty() ||
//                !waitingQueueNameList.isEmpty() ||
//                queuesHasData.values().stream().filter(item -> item.getUuid() == null).collect(Collectors.toList()).size() > 0;
        return true;
    }

    public void commit(Message message){
        try {
            CommitLog commitLog = messageStore.getQueueFile(message.getTopic());
            if (commitLog == null) {
                return;
            }
            commitLog.commit(message.getIdx());
        } catch (IOException e) {
            logger.error("exception in commit localqueue data = " + e.getMessage());
        }finally {
            message.reset();
            pullRequestQueueMap.get(message.getTopic()).offer(message);
        }
    }

    public void rollback(Message message){
        try {
            CommitLog commitLog = messageStore.getQueueFile(message.getTopic());
            if (commitLog == null) {
                return;
            }
            commitLog.rollback(message.getIdx());
        } catch (IOException e) {
            logger.error("exception in rollback localqueue data = " + e.getMessage());
        }finally {
            message.reset();
            pullRequestQueueMap.get(message.getTopic()).offer(message);
        }
    }

    public void retry(Message message){
        if (message.getSendCount() > message.getMaxSendCount()){
            rollback(message);
        }else {
            message.setSendCount(message.getSendCount() + 1);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    pullRequestQueueMap.get(message.getTopic()).offer(message);
                }
            },message.getRetryInterval());
        }
    }

    public void register(String topic,ConsumeService consumer){
        this.consumeServiceMap.put(topic,consumer);

        int concurrentNum = consumer.getConcurrentNum();
        LinkedBlockingQueue<Message> blockingQueue = new LinkedBlockingQueue<>();
        for (int i = 0;i <concurrentNum; i++){
            Message message = new Message(topic,consumer.getRetryTimes(),consumer.getRetryInterval());
            blockingQueue.offer(message);
        }
        this.pullRequestQueueMap.put(topic,blockingQueue);
    }

}
