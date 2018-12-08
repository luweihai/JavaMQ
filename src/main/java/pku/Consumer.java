package pku;

<<<<<<< HEAD
=======
import java.io.IOException;
>>>>>>> JavaMQ
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 消费者
 */

public class Consumer {
    List<String> topics = new LinkedList<>();
<<<<<<< HEAD
    int readPos = 0;
    String queue;

    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
=======
    int readPos = 0;     // 从 0 位置开始读取 
    String queue;

    //将消费者订阅的topic进行绑定
    public void attachQueue(String queueName, Collection<String> t) throws Exception {    // 传入参数是 queue 和  topics
>>>>>>> JavaMQ
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }

<<<<<<< HEAD

    //每次消费读取一个message
    public ByteMessage poll() {
        ByteMessage re = null;
        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {
            int index = (i + readPos) % topics.size();
            re = DemoMessageStore.store.pull(queue, topics.get(index));
=======
    //每次消费读取一个message
    public ByteMessage poll() throws IOException {
        ByteMessage re = null;
        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {     // 遍历整个 topics 
            int index = (i + readPos) % topics.size();
            re = MessageStore.store.pull(queue, topics.get(index));   // 取消息也是从 Store（磁盘）中取 
>>>>>>> JavaMQ
            if (re != null) {
                readPos = index + 1;
                break;
            }
        }
        return re;
    }

}
