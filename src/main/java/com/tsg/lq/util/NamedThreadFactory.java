package com.tsg.lq.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 命名方便
 * Created by tsguang on 2019/11/27.
 */
public class NamedThreadFactory implements ThreadFactory {

    private AtomicInteger threadNumber;

//    private SecurityManager securityManager;

    private ThreadGroup group;

    private String namePrefix;

    public NamedThreadFactory(String namePrefix) {
        this.threadNumber = new AtomicInteger(1);
//        this.securityManager = System.getSecurityManager();
        SecurityManager securityManager = System.getSecurityManager();
        this.group  = securityManager == null ? Thread.currentThread().getThreadGroup() : securityManager.getThreadGroup();
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(group, r, namePrefix + "-thread-" + threadNumber.getAndIncrement(), 0);
        if (thread.isDaemon())
            thread.setDaemon(false);
        if (thread.getPriority() != Thread.NORM_PRIORITY)
            thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
    }
}

