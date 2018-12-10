package pku;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Producer {

	private KeyValue properties;                 //  key -value 内容 
	private String storePath;                    // 存储路径 
	private Map<String, MappedWriter> bufferHashMap = new HashMap<>();     //   key 是 topic 


	private Map<String, MappedWriter> buffertopics = new HashMap<>();   // key 是 topic
	
	
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
			
			
			String fileName = storePath + "/" + topic;
	  		if (!buffertopics.containsKey(topic)) {     // 如果包含 topic 
	  			mw = new MappedWriter(fileName);           // 根据 存储路径 + topic 得到 mapperwriter       是根据 fileName得到 mw 的
	  			buffertopics.put(topic, mw);   // 如果存在就返回当前值，不存在就 put 
	  		} else {
	  			mw =  buffertopics.get(topic);     // 否则直接取  
	  		}
			
			
			
			
			
			
			
			bufferHashMap.put(topic, mw);
		} else {
			mw = bufferHashMap.get(topic);    // 否则就直接得到刚刚的路径  
		}
		mw.send(message);
	}

	public void flush() throws IOException {
		
	}
}
