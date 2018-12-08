package pku;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();

	// 消息存储
<<<<<<< HEAD
	HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();
	// 遍历指针
	HashMap<String, Integer> readPos = new HashMap<>();

=======
	HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();  // key 是  单独的 topic ， value 是 消息出现的次数-1，也就是序号
	// 遍历指针
	HashMap<String, Integer> readPos = new HashMap<>();   // value是 读的位置，也就是 topic 索引 
                                                  // key 是 queue + topic      此处是表示已经poll出的位置 
>>>>>>> JavaMQ

	// 加锁保证线程安全
	/**
	 * @param msg
	 * @param topic
<<<<<<< HEAD
	 */
	public synchronized void push(ByteMessage msg, String topic) {
		if (msg == null) {
			return;
		}
		if (!msgs.containsKey(topic)) {
			msgs.put(topic, new ArrayList<>());
		}
		// 加入消息
		msgs.get(topic).add(msg);
=======
	 */ 
	/*
	 * 存的时候是按照 topic进行存储的，但是取的时候是按照 queue + topic。还要注意是多线程并发取 
	 */
	public synchronized void push(ByteMessage msg, String topic) {   // 存的时候有  ByteMessage 
		if (msg == null) {               
			return;
		}
		if (!msgs.containsKey(topic)) {
			msgs.put(topic, new ArrayList<>());  // 新建  list 
		}
		// 加入消息
		msgs.get(topic).add(msg);  // 此处是添加新的消息到 value 对应的 list 
>>>>>>> JavaMQ

	}

	// 加锁保证线程安全
<<<<<<< HEAD
	public synchronized ByteMessage pull(String queue, String topic) {
		String k = queue + " " + topic;
		if (!readPos.containsKey(k)) {
			readPos.put(k, 0);
		}
		int pos = readPos.get(k);
=======
	public synchronized ByteMessage pull(String queue, String topic) {   // 取的时候有 queue
		String k = queue + " " + topic;       // 注意此处是 queue + topic
		if (!readPos.containsKey(k)) {      // 第一次出现 
			readPos.put(k, 0);
		}
		int pos = readPos.get(k);     // 得到 位置索引 
>>>>>>> JavaMQ
		if (!msgs.containsKey(topic)) {
			return null;
		}

<<<<<<< HEAD
		ArrayList<ByteMessage> list = msgs.get(topic);
		if (list.size() <= pos) {
=======
		ArrayList<ByteMessage> list = msgs.get(topic);  // value 是 ByteMessage 的序列      此处是得到 已经存储的对应 topic 的消息  
		if (list.size() <= pos) {   // 序列的大小  一定 大于 读取的位置 ， 最后等于的时候就表示读取完了 此topic的消息 
>>>>>>> JavaMQ
			return null;
		} else {
			ByteMessage msg = list.get(pos);
			// 将键k的值+1，表示当前读到第pos个msg，下一次应该读+1个
			readPos.put(k, pos + 1);
			return msg;
		}
	}

	
}
