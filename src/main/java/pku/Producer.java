package pku;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 生产者
 */
public class Producer {
	
	//private final HashMap<String , String > uesed_Key = new HashMap<>();    // 存储已经出现过的固定头部 , set中存储 
	private String topic = null ;
	
	public static byte count = 0;
	public Producer(){
		this.count ++;
	}
	
	//生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        this.topic = topic ;         // 因为生成 message后立马要 send ，所以topic 还是相同可以全局用 
        
    	ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC  ,   topic);  // topic在此处是 value的位置
        return msg;
    }
    //将message发送出去
    public void send(ByteMessage defaultMessage) throws IOException{  // 一个生产者用到多个 defaultMessage，因为多次 send，所以这个
    	if(defaultMessage == null)
        	return ;
        //String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        MessageStore.store.push(defaultMessage , topic);     // 所谓发送消息就是写在 Store（磁盘） 
    }
    //处理将缓存区的剩余部分
    public void flush()throws Exception{
        MessageStore.store.flush();
    }
}
