package com.cmc.redisc.configure;

import cn.hutool.extra.spring.SpringUtil;
import com.cmc.redisc.service.RedisService;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
public class RedisReceiver {
    /**
     * 监听任务队列
     */
    public static final Map<String, Function<TaskSynData<?>,?>> LISTEN_QUEUE = new ConcurrentHashMap<>();

    public void receiveMessage(TaskSynData<?> taskSynData ) {
        if(LISTEN_QUEUE.containsKey(taskSynData.getName())){
            // 存在监听者，进行调用
            try{
                Object obj = LISTEN_QUEUE.get(taskSynData.getName()).apply(taskSynData);
                if(obj != null){
                    // 进行事件反发布
                    RedisService redisService = SpringUtil.getBean(RedisService.class);
                    redisService.pu(taskSynData.getKey(),obj);
                }
            }catch (Exception e){
                log.error("redis事件执行出现异常，请进行捕捉处理");
            }
        }
    }
}