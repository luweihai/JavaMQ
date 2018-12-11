package pku;

import pku.ByteMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;


public class MessageStore {
	static final MessageStore store = new MessageStore();    // 这里有静态，保证了所有操作都可以保留

	// 消息存储
	HashMap<String, DataOutputStream> map_Out = new HashMap<>();          //  输出流
	//volatile int producerNum = 0;
	HashMap<String, DataInputStream> map_In = new HashMap<>();         //  输入流 
	HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();
	HashMap<String, Integer> readPos = new HashMap<>();
	
	public synchronized void flush()  throws IOException{
		Producer.count --;
		int count = Producer.count ;
		if( count != 0)
			return ;
		
		for(DataOutputStream  proFlush : map_Out.values() ){
			proFlush.close();
		}
		
	}

	// push
	public void push(ByteMessage msg, String topic) throws Exception {
		if (msg == null) {
			return;
		}
		DataOutputStream temp_Out = null ;     // 输出流 
		
		synchronized (map_Out) {     // 锁 ， 一个线程调用此代码块时， map_Out 会加锁，另外的线程就不能调用此代码块
			if (!map_Out.containsKey(topic)) {    // topic 无 对应的输出流 
				temp_Out = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream("./data/" + topic )));   
				map_Out.put(topic, temp_Out);          // 把新的输出流 加入 输出流的 map 中 
				//System.out.println("aaa");  测试用

			} else {
				temp_Out = map_Out.get(topic);         // 取得输出流 
				//System.out.println("bbb");   测试用 
			}
		}

		Write.write(temp_Out , msg , topic );

	}

	public ByteMessage pull(String queue, String topic) throws Exception {
		
		String queue_Topic = queue + " " + topic;                     
		
		if (!readPos.containsKey(topic)) {
			readPos.put(topic , 0);
		}
		if (!msgs.containsKey(topic)) {

			DataInputStream temp_In;                             
			
			if (!map_In.containsKey(queue_Topic)) {                  
				try {
					temp_In = new DataInputStream(new BufferedInputStream(new FileInputStream("./data/" + topic)));    // 建立 输入流 
				} catch (FileNotFoundException e) {
					return null;
				}
				synchronized (map_In) {             // 加锁，只能让一个线程调用  输入流的 put   尽可能减少锁的范围 
					map_In.put(queue_Topic , temp_In);            
				}
			} else {               
				temp_In = map_In.get(queue_Topic);                // 根据  queue + topic  作为 map 的 key 得到输入流 
			}
			
			return Read.read(  temp_In , queue,  topic );

			
		}
		int pos = readPos.get(topic);
		ArrayList<ByteMessage> list = msgs.get(topic);
		if (list.size() <= pos) {
			return null;
		} else {
			ByteMessage msg = list.get(pos);
			// 将键k的值+1，表示当前读到第pos个msg，下一次应该读+1个
			readPos.put(topic , pos + 1);
			return msg;
		}

	}

}
