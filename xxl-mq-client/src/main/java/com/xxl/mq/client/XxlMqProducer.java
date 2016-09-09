package com.xxl.mq.client;

import com.xxl.mq.client.message.Message;
import com.xxl.mq.client.rpc.util.JacksonUtil;

import java.util.Date;
import java.util.Map;

/**
 * Created by xuxueli on 16/8/28.
 */
public class XxlMqProducer {

    public static void produce(String name, Map<String, String> dataMap, Date delayTime, int retryCount){

        // name
        if (name==null || name.length()>255) {
            throw  new IllegalArgumentException("消息标题长度不合法");
        }
        // data
        String dataJson = null;
        if (dataMap!=null) {
            dataJson = JacksonUtil.writeValueAsString(dataMap);
            if (dataJson.length()>1024) {
                throw  new IllegalArgumentException("消息数据长度超长");
            }
        }
        // delayTime
        if (delayTime==null) {
            delayTime = new Date();
        }
        // retryCount
        if (retryCount<0) {
            retryCount = 0;
        }

        // package message
        Message message = new Message();
        message.setName(name);
        message.setData(dataJson);
        message.setDelayTime(delayTime);
        message.setStatus(Message.Status.NEW.name());
        message.setRetryCount(retryCount);

        XxlMqClient.getXxlMqService().saveMessage(message);
    }

    public static void produce(String name, Map<String, String> dataMap){
        produce(name, dataMap, null, 0);
    }

}