package pku;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Producer {

	private KeyValue properties;                 //  key -value 内容 
	private String storePath;                    // 存储路径 
	private Map<String, MappedWriter> bufferHashMap = new HashMap<>();     //   key 是 topic 
	private MessageStore messageStore = MessageStore.getInstance();
	
	
	private Map<String, MappedWriter> buffertopics = new ConcurrentHashMap<>();   // key 是 topic
	
	
	public Producer() {          
		this.properties = new DefaultKeyValue();
		properties.put("STORE_PATH", "data");
		storePath = properties.getString("STORE_PATH");
	}

	public ByteMessage createBytesMessageToTopic(String topic, byte[] body) {
		DefaultMessage defaultBytesMessage = new DefaultMessage(body);
		defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
		return defaultBytesMessage;
	}

	
	
	public synchronized void  send(ByteMessage message) throws Exception {
		if (message == null) {
			return ;
		}
		String topic = message.headers().getString(MessageHeader.TOPIC);
		MappedWriter mw;
		if (!bufferHashMap.containsKey(topic)) {  // 如果该 生产者 发送过该 topic的消息 
			
			
			mw = messageStore.getMappedWriter(storePath, topic);   // 生成新的  writer的路径 
			
			
			bufferHashMap.put(topic, mw);
		} else {
			mw = bufferHashMap.get(topic);    // 否则就直接得到刚刚的路径  
		}
		mw.send(message);
	}

	public void flush() throws IOException {
		
	}
}
