package com.cmc.redisc.configure;

import com.cmc.redisc.service.RedisService;
import lombok.Data;

import java.io.Serializable;

/**
 * 队列事件
 */
@Data
public class QueueEvent implements Serializable {
    private static final long serialVersionUID = -1681130165215741845L;

    private long num;

    private Object data;

    private String name;

    private RedisService redisService;
    public QueueEvent(RedisService redisService){
        this.redisService = redisService;
    }
    /**
     * 发送事件
     */
    public void puEvent(){
        if(isSuccess()){
            redisService.pu(name,data);
        }else{
            throw new RuntimeException("数据进入消息队列失败，请检查redis配置");
        }
    }

    /**
     * 是否成功
     */
    public boolean isSuccess(){
        return num != 0;
    }
}
