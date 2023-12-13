package cmc.redisc.configure;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Data
public class TaskSynData<T> implements Serializable {
    private static final long serialVersionUID = -1681130765215741845L;

    /**
     * 唯一标识
     */
    private String key = UUID.randomUUID().toString();

    /**
     * 任务名字
     */
    private String name;

    /**
     * 任务类型
     */
    private String type;

    /**
     * 任务数据
     */
    private T data;

    /**
     * 获取数据
     */
    public <V> V get(Class<V> tClass){
        if(data instanceof Map && ((Map<?,?>)data).containsKey(tClass.getName())){
                return JSONObject.parseObject(JSONObject.toJSONString(((Map<?,?>)data).get(tClass.getName())),tClass);
        }
        return JSONObject.parseObject(JSONObject.toJSONString(data),tClass);
    }
    public T get(){
        return data;
    }

    /**
     * 获取单个数据
     */
    public T first(Class<T> tClass){
        return JSONObject.parseObject(JSONObject.toJSONString(data),tClass);
    }
    /**
     * 获取指定数据
     */
    public <V> V get(String name, Class<V> tClass){
        if(data instanceof Map){
            return JSONObject.parseObject(JSONObject.toJSONString(((Map<?, ?>) data).get(name)),tClass);
        }
        return null;
    }

    /**
     * 转化为数组
     */
    public <V> List<V> toList(Class<V> vClass){
        if(data instanceof List){
            return ((List<?>) data).stream().map(e-> JSONObject.parseObject(JSONObject.toJSONString(e),vClass)).collect(Collectors.toList());
        }
        throw new RuntimeException("数据类型错误，无法转化为List");
    }
}