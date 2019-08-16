package com.tsg.lq.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created by tsguang on 2019/8/16.
 */
public class UnMapTool {

    private static final Logger logger = LoggerFactory.getLogger(UnMapTool.class);
    private static Method GetCleanerMethod;
    private static Method cleanMethod;

    UnMapTool() {
    }

    /**
     * 清空 MappedByteBuffer
     * @param buffer
     */
    public static void unmap(final MappedByteBuffer buffer) {
        if (buffer != null && GetCleanerMethod != null && cleanMethod != null) {
            synchronized (buffer) {
                AccessController.doPrivileged(new PrivilegedAction<Object>() {
                    public Object run() {
                        try {
                            Object cleaner = UnMapTool.GetCleanerMethod.invoke(buffer);
                            UnMapTool.cleanMethod.invoke(cleaner);
                        } catch (Exception var2) {
                            UnMapTool.logger.error("unmap MappedByteBuffer error", var2);
                        }
                        return null;
                    }
                });
            }
        }
    }

    static {
        try {
            if (GetCleanerMethod == null) {
                Method getCleanerMethod = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                GetCleanerMethod = getCleanerMethod;
                cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");
            }
        } catch (Exception e) {
            GetCleanerMethod = null;
            cleanMethod = null;
        }
    }

}
