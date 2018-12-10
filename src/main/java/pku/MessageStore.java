package pku;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {

	private static final MessageStore INSTANCE = new MessageStore();
	private Map<String, MappedWriter> buffertopics = new ConcurrentHashMap<>();   // key 是 topic

  	public static MessageStore getInstance() {    // 得到实例  
  		return INSTANCE;
  	}

	  public synchronized MappedWriter getMappedWriter(String storePath, String topic) throws Exception {    // 此处有些不懂  ？？？？？？
		  String fileName = storePath + "/" + topic;
		  MappedWriter mw ;
		  if (!buffertopics.containsKey(topic)) {     // 如果包含 topic 
			  mw = new MappedWriter(fileName);           // 根据 存储路径 + topic 得到 mapperwriter       是根据 fileName得到 mw 的
			  buffertopics.put(topic, mw);   // 如果存在就返回当前值，不存在就 put 
			  return mw ;
		  } else {
			  return buffertopics.get(topic);     // 否则直接取  
		  }
	  }
}
