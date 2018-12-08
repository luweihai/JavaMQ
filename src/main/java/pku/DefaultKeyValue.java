package pku;

import java.util.HashMap;
import java.util.Set;

/**
 * 一个Key-Value的实现
 */
public class DefaultKeyValue implements KeyValue{
    private final HashMap<String, Object> kvs = new HashMap<>();

    public Object getObj(String key) {
        return kvs.get(key);
    }

    public HashMap<String, Object> getMap(){
        return kvs;
    }

    public DefaultKeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    public int getInt(String key) {
<<<<<<< HEAD
        return (Integer) kvs.getOrDefault(key, 0);
=======
        return (Integer) kvs.getOrDefault(key, 0);    // 如果 key存在则返回相应的 value，否则返回 0 这个默认值 
>>>>>>> JavaMQ
    }

    public long getLong(String key) {
        return (Long) kvs.getOrDefault(key, 0L);
    }

    public double getDouble(String key) {
        return (Double) kvs.getOrDefault(key, 0.0d);
    }

<<<<<<< HEAD
    public String getString(String key) {
        return (String) kvs.getOrDefault(key, null);
    }

    public Set<String> keySet() {
        return kvs.keySet();
=======
    public String getString(String key) {       // 返回的是 
        return (String) kvs.getOrDefault(key, null);    // 如果 key存在则返回相应的 value，否则返回 null 这个默认值 
    }

    public Set<String> keySet() {   // 返回 keySet，也就是返回 已经出现过的 那个固定的头的key。 所谓已经出现，是对于一个 生产者而言的
        return kvs.keySet();          //  一个生产者 20 * 50 条消息  一共4个生产者    ？   有问题
>>>>>>> JavaMQ
    }

    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}
