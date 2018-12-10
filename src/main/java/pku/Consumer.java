package pku;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {

	private String queue;
	String storePath ;
	int readPos = 0;
	
	LinkedList<String> nonConsumeFiles = new LinkedList<>();   // 里面存的是 topic的序列  
	private Map<String, MappedReader> bufferBuckets = new ConcurrentHashMap<>();  // key 是 queue + topic   value 是  mr
	
	public Consumer() {
		
		this.storePath = "data";
		
	}

	public ByteMessage poll()throws Exception {
		
		ByteMessage message=null;
	    
	    
	    
	    
	    for (int i = 0; i < nonConsumeFiles.size(); i++) {    // 遍历整个链表 
		    int index = (i + readPos) % nonConsumeFiles.size();         // 此处可以提高速度  
		    String topic = nonConsumeFiles.get(index);
		      
		    String key = queue + " " + topic;
			MappedReader  mr;
			if (!bufferBuckets.containsKey(key)) {
				mr = new MappedReader( storePath , topic);
				bufferBuckets.put( key , mr);
				  
			} else {
				mr= bufferBuckets.get(key);
			}
			message =  mr.poll();
		      
			if (message != null) {
				readPos = index + 1;
				break;
			}
	    }
	    
	    
	    return message;

	}


	public void attachQueue(String queueName, Collection<String> topics) throws Exception {
		
		queue = queueName;
		nonConsumeFiles.addAll(topics); 
	}
}
