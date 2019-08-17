package com.tsg.lq.store;

import com.sun.xml.internal.fastinfoset.tools.FI_DOM_Or_XML_DOM_SAX_SAXEvent;
import org.junit.Test;

/**
 * Created by tsguang on 2019/8/16.
 */
public class MappedFileTest {

    private static final String DATA_DIR = "E:/test/";

//    private static final String filename = "E:/test/file1";
    private static final String filename = "queue_testTopic_data_1";

    @Test
    public void testWrite(){
        try {
            MappedFile mappedFile = MappedFile.createNew(DATA_DIR,filename);
            for (int i = 0; i < 100;i++){
                String str= "第"+ i +"条消息";
                int write = mappedFile.write(str.getBytes());
                System.out.println(write);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testRead()throws Exception{
        MappedFile mappedFile = MappedFile.load(DATA_DIR,filename);
        int i = mappedFile.get();
        while (i >= 0){
            System.out.println(i);
            byte[] item = mappedFile.item(i);
            System.out.println(new String(item));
            i = mappedFile.get();
        }

    }

}
