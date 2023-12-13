package cmc.redisc.service;

import cn.hutool.core.thread.ThreadUtil;
import cmc.redisc.configure.RedisReceiver;
import cmc.redisc.configure.TaskSynData;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * spring redis 工具类
 *
 * @author 程梦城
 * @version 1.0.0
 * &#064;date  2023/12/12
 */
@SuppressWarnings(value = {"unchecked", "rawtypes"})
@Component
public class RedisService {
    @Resource
    public RedisTemplate redisTemplate;

    /**
     * 缓存基本的对象，Integer、String、实体类等
     *
     * @param key   缓存的键值
     * @param value 缓存的值
     */
    public <T> void setCacheObject(final String key, final T value) {
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * 缓存基本的对象，Integer、String、实体类等
     *
     * @param key      缓存的键值
     * @param value    缓存的值
     * @param timeout  时间
     * @param timeUnit 时间颗粒度
     */
    public <T> void setCacheObject(final String key, final T value, final Long timeout, final TimeUnit timeUnit) {
        redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
    }

    /**
     * 设置有效时间
     *
     * @param key     Redis键
     * @param timeout 超时时间
     * @return true=设置成功；false=设置失败
     */
    public boolean expire(final String key, final long timeout) {
        return expire(key, timeout, TimeUnit.SECONDS);
    }

    /**
     * 设置有效时间
     *
     * @param key     Redis键
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return true=设置成功；false=设置失败
     */
    public boolean expire(final String key, final long timeout, final TimeUnit unit) {
        return redisTemplate.expire(key, timeout, unit);
    }

    /**
     * 获取有效时间
     *
     * @param key Redis键
     * @return 有效时间
     */
    public long getExpire(final String key) {
        return redisTemplate.getExpire(key);
    }

    /**
     * 判断 key是否存在
     *
     * @param key 键
     * @return true 存在 false不存在
     */
    public Boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }

    /**
     * 获得缓存的基本对象。
     *
     * @param key 缓存键值
     * @return 缓存键值对应的数据
     */
    public <T> T getCacheObject(final String key) {
        ValueOperations<String, T> operation = redisTemplate.opsForValue();
        return operation.get(key);
    }

    /**
     * 删除单个对象
     *
     * @param key
     */
    public boolean deleteObject(final String key) {
        return redisTemplate.delete(key);
    }

    /**
     * 删除集合对象
     *
     * @param collection 多个对象
     * @return
     */
    public long deleteObject(final Collection collection) {
        return redisTemplate.delete(collection);
    }

    /**
     * 缓存List数据
     *
     * @param key      缓存的键值
     * @param dataList 待缓存的List数据
     * @return 缓存的对象
     */
    public <T> long setCacheList(final String key, final List<T> dataList) {
        Long count = redisTemplate.opsForList().rightPushAll(key, dataList);
        return count == null ? 0 : count;
    }

    /**
     * 获得缓存的list对象
     *
     * @param key 缓存的键值
     * @return 缓存键值对应的数据
     */
    public <T> List<T> getCacheList(final String key) {
        return redisTemplate.opsForList().range(key, 0, -1);
    }

    /**
     * 缓存Set
     *
     * @param key     缓存键值
     * @param dataSet 缓存的数据
     * @return 缓存数据的对象
     */
    public <T> BoundSetOperations<String, T> setCacheSet(final String key, final Set<T> dataSet) {
        BoundSetOperations<String, T> setOperation = redisTemplate.boundSetOps(key);
        Iterator<T> it = dataSet.iterator();
        while (it.hasNext()) {
            setOperation.add(it.next());
        }
        return setOperation;
    }

    /**
     * 获得缓存的set
     *
     * @param key
     * @return
     */
    public <T> Set<T> getCacheSet(final String key) {
        return redisTemplate.opsForSet().members(key);
    }

    /**
     * 缓存Map
     *
     * @param key
     * @param dataMap
     */
    public <T> void setCacheMap(final String key, final Map<String, T> dataMap) {
        if (dataMap != null) {
            redisTemplate.opsForHash().putAll(key, dataMap);
        }
    }

    /**
     * 缓存Map是否存在对应的值
     *
     * @param key
     * @param hKey
     */
    public Boolean CacheMapHasKey(final String key, final String hKey) {
        return redisTemplate.opsForHash().hasKey(key, hKey);
    }

    /**
     * 获得缓存的Map
     *
     * @param key
     * @return
     */
    public <T> Map<String, T> getCacheMap(final String key) {
        return redisTemplate.opsForHash().entries(key);
    }

    /**
     * 往Hash中存入数据
     *
     * @param key   Redis键
     * @param hKey  Hash键
     * @param value 值
     */
    public <T> void setCacheMapValue(final String key, final String hKey, final T value) {
        redisTemplate.opsForHash().put(key, hKey, value);
    }

    /**
     * 往Hash中删除
     *
     * @param key  Redis键
     * @param hKey Hash键
     */
    public void deleteCacheMapKey(final String key, final String hKey) {
        redisTemplate.opsForHash().delete(key, hKey);
    }

    /**
     * 获取Hash中的数据
     *
     * @param key  Redis键
     * @param hKey Hash键
     * @return Hash中的对象
     */
    public <T> T getCacheMapValue(final String key, final String hKey) {
        HashOperations<String, String, T> opsForHash = redisTemplate.opsForHash();
        return opsForHash.get(key, hKey);
    }

    /**
     * 获取多个Hash中的数据
     *
     * @param key   Redis键
     * @param hKeys Hash键集合
     * @return Hash对象集合
     */
    public <T> List<T> getMultiCacheMapValue(final String key, final Collection<Object> hKeys) {
        return redisTemplate.opsForHash().multiGet(key, hKeys);
    }

    /**
     * 递增
     *
     * @param key   键
     * @param delta 要增加几(大于0)
     * @return
     */
    public long incr(String key, long delta) {
        if (delta <= 0) {
            throw new RuntimeException("递增因子必须大于0");
        }
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /**
     * 获得缓存的基本对象列表
     *
     * @param pattern 字符串前缀
     * @return 对象列表
     */
    public Collection<String> keys(final String pattern) {
        return redisTemplate.keys(pattern);
    }

    /**
     * 不存在则设置值
     *
     * @param key
     * @param value
     * @param timeout
     * @param timeUnit
     * @param <T>
     * @return
     */
    public <T> boolean setIfAbsent(final String key, final T value, final Long timeout, final TimeUnit timeUnit) {
        return redisTemplate.opsForValue().setIfAbsent(key, value, timeout, timeUnit);
    }


    /**
     * 事件发布
     */
    public void pu(String name, Object... args) {
        TaskSynData taskSynData = new TaskSynData();
        taskSynData.setName(name);
        genParameter(taskSynData, args);
        redisTemplate.convertAndSend("onDoor", taskSynData);
    }


    /**
     * 事件发布有返回值的方法
     */
    public Optional<TaskSynData<?>> pur(String name, Object map) {
        String returnName = name + "_return_" + UUID.randomUUID();
        AtomicReference<TaskSynData<?>> result = new AtomicReference<>();
        // 先提前订阅
        this.on(returnName, e -> {
            // e是这次订阅返回的数据
            result.set(e);
            return null;
        });
        TaskSynData taskSynData = new TaskSynData();
        taskSynData.setKey(returnName);
        taskSynData.setName(name);
        taskSynData.setData(map);
        // 发送事件
        redisTemplate.convertAndSend("onDoor", taskSynData);
        int i = 0;
        // 获取返回值 最多等待20秒钟
        while (result.get() == null && i < 200) {
            // 暂停100毫秒
            ThreadUtil.sleep(100);
            i++;
        }
        // 无论是否获取到数据都取消订阅
        cancel(returnName);
        return Optional.ofNullable(result.get());
    }

    /**
     * 事件监听
     */
    public void on(String name, Function<TaskSynData<?>, ?> fun) {
        RedisReceiver.LISTEN_QUEUE.put(name, fun);
    }

    /**
     * 取消订阅
     */
    public void cancel(String name) {
        RedisReceiver.LISTEN_QUEUE.remove(name);
    }


    public static void genParameter(TaskSynData taskSynData, Object[] args) {
        if (args.length == 1) {
            taskSynData.setData(args[0]);
        } else {
            Map<String, Object> map = new HashMap<>();
            for (Object arg : args) {
                map.put(arg.getClass().getName(), arg);
            }
            taskSynData.setData(map);
        }
    }
}
