package cmc.redisc.configure;

import cmc.redisc.service.RedisService;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Redis队列 实现类
 */
@Slf4j
public class RedisQueue {

    private final StringRedisTemplate redisTemplate;
    private final RedisService redisService;

    private static final String QUESUE_LOCK_KEY_SUFFIX = "_gsoft_lock";

    public RedisQueue(StringRedisTemplate redisTemplate, RedisService redisService) {
        if (redisTemplate == null) {
            throw new RuntimeException("redisTemplate can not be null ");
        }
        this.redisTemplate = redisTemplate;
        this.redisService = redisService;
    }

    public QueueEvent push(final String queueName, Object... args) {
        QueueEvent queueEvent = new QueueEvent(redisService);
        TaskSynData<Object> taskSynData = new TaskSynData<>();
        RedisService.genParameter(taskSynData, args);
        queueEvent.setNum(rPush(queueName, JSON.toJSONString(taskSynData)));
        queueEvent.setName(queueName);
        queueEvent.setData(taskSynData);
        return queueEvent;
    }

    public long rPush(final String queueName, final String value) {
        RedisConnection redisConnection = getRedisConnection();
        return Optional.ofNullable(redisConnection.rPush(getByte(queueName), getByte(value))).orElse(-1L);
    }

    public long rPush(final String queueName, final List<String> values) {
        if (values != null && values.size() > 0) {
            RedisConnection redisConnection = getRedisConnection();
            List<byte[]> valuesByte = new ArrayList<>();
            for (String value : values) {
                if (value != null) {
                    valuesByte.add(redisTemplate.getStringSerializer().serialize(value));
                }
            }
            return Optional.ofNullable(redisConnection.rPush(getByte(queueName),
                    valuesByte.toArray(new byte[valuesByte.size()][]))).orElse(-1L);
        }
        return 0L;
    }

    /**
     * 获取指定的数据格式
     */
    public TaskSynData<?> pop(final String queueName) {
        return JSON.parseObject(lPop(queueName), TaskSynData.class);
    }


    public String lPop(final String queueName) {
        RedisConnection redisConnection = getRedisConnection();
        byte[] value = redisConnection.lPop(Objects.requireNonNull(redisTemplate.getStringSerializer().serialize((queueName))));
        return redisTemplate.getStringSerializer().deserialize(value);
    }

    public List<String> lPop(final String queueName, final int length) {
        RedisConnection connection = getRedisConnection();
        List<String> values = new ArrayList<>();
        byte[] key = getByte(queueName);
        byte[] lockKey = getByte(queueName + QUESUE_LOCK_KEY_SUFFIX);
        try {
            // 队列加锁
            while (Boolean.FALSE.equals(connection.setNX(lockKey, key))) {
                // 等待10毫秒
                ThreadUtil.sleep(10);
            }
            connection.expire(lockKey, 5);// 设计锁的超时时间
            List<byte[]> valuesByte = connection.lRange(key, 0, length - 1);
            assert valuesByte != null;
            connection.lTrim(key, valuesByte.size(), -1);
            for (byte[] valueByte : valuesByte) {
                values.add(redisTemplate.getStringSerializer().deserialize(valueByte));
            }
        } finally {
            connection.del(lockKey);
        }
        return values;
    }

    public List<String> lRange(final String queueName, final int length) {
        RedisConnection connection = getRedisConnection();
        List<String> values = new ArrayList<>();
        byte[] key = getByte(queueName);
        List<byte[]> valuesByte = connection.lRange(key, 0, length - 1);
        assert valuesByte != null;
        for (byte[] valueByte : valuesByte) {
            values.add(redisTemplate.getStringSerializer().deserialize(valueByte));
        }
        return values;
    }

    public void lRem(final String queueName, final int length) {
        RedisConnection connection = getRedisConnection();
        byte[] key = getByte((queueName));
        connection.lTrim(key, length, -1);
    }

    public long lLen(final String queueName) {
        RedisConnection connection = getRedisConnection();
        byte[] key = getByte((queueName));
        return Optional.ofNullable(connection.lLen(key)).orElse(-1L);
    }


    private byte[] getByte(String queueName) {
        return redisTemplate.getStringSerializer().serialize((queueName));
    }

    private RedisConnection getRedisConnection() {
        return redisTemplate.execute((RedisCallback<RedisConnection>) connection -> connection);
    }
}
