package pku;

import java.io.DataOutputStream;
import java.util.HashMap;



public class Write {
	

	public  static void write(DataOutputStream temp_Out , ByteMessage msg, String topic ) throws Exception{
		
		
		byte[] body = null;
		byte head_Num = (byte) msg.headers().keySet().size();   // 此处就是得到已经出现过的消息的 固定的头 组成的Set 集合的 大小     
		byte isCompress;
		if (msg.getBody().length > 1000) {     // 消息的 body的 byte数组 大于 200    调参数的过程  
			body = Compress.compressGZ(msg.getBody());   // 对 body 压缩
			isCompress = 1;       // 记录被压缩了 
		}
		else {
			body=msg.getBody();      // 不压缩了
			isCompress = 0;
		}
		synchronized (temp_Out) {        //  加锁，只能一个线程访问 temp_Out 对象 
			temp_Out.writeByte(head_Num);                    //  写入  已经出现过的消息的 固定的头 组成的Set 集合的 大小 
			
			
			
			
			for( HashMap.Entry<String , Object > entry :  msg.headers().getMap().entrySet()  ){
				String key = entry.getKey();
				temp_Out.writeByte( stringToClassByte.get( key ));         // 得到 某个 固定头部 对应的  byte类型的索引    并且写入 
				
		
				
				switch ( stringToClass.get( key )) {             // 此处 得到 short 类型的 数字化 类型 
				case 1:
					temp_Out.writeLong( (long)entry.getValue() );               // 写入  long 类型的 topic 
					break;
				case 2:
					temp_Out.writeDouble((double)entry.getValue());               // 写入 double 类型的 topic
					break;
				case 3:
					temp_Out.writeInt((int)entry.getValue());                  // 写入 int 类型的 topic
					break;
				case 4:
					temp_Out.writeUTF( (String) entry.getValue());            // 写入 String 类型的 topic     此处是用输出流的 writeUTF
					break;
				}
			}
			temp_Out.writeByte(isCompress);           //  isCompress = 0 或者 1  表明 是否被压缩 
			temp_Out.writeShort(body.length);         // 写入 body，也就是 数据部分 的长度  用 short是为了节约 
			temp_Out.write(body);			          // 写入全部的 body 数据
		}
	}
	
	public static final HashMap<String, Byte> stringToClass = new HashMap<String, Byte>() {   
		{                                          // 此处的 value 是 数字，表示是属于哪个类型  
			put("MessageId", (byte) 3);             // int
			put("Topic", (byte) 4);                 // String 
			put("BornTimestamp", (byte) 1);        // long  
			put("BornHost", (byte) 4);              // String 
			put("StoreTimestamp", (byte) 1);        // long 
			put("StoreHost", (byte) 4);             // String 
			put("StartTime", (byte) 1);            // long  
			put("StopTime", (byte) 1);            // long  
			put("Timeout", (byte) 3);               // int 
			put("Priority", (byte) 3);                // int 
			put("Reliability", (byte) 3);              // int  
			put("SearchKey", (byte) 4);                // String 
			put("ScheduleExpression", (byte) 4);        // String 
			put("ShardingKey", (byte) 2);            // double 
			put("ShardingPartition", (byte) 2);          // double 
			put("TraceId", (byte) 4);                  // String  
		}
	};
	public static final HashMap<String, Byte> stringToClassByte = new HashMap<String, Byte>() {     // value 表明索引
		{
			put("MessageId", (byte) 1);                      
			put("Topic", (byte) 2);                           
			put("BornTimestamp", (byte) 3);
			put("BornHost", (byte) 4);
			put("StoreTimestamp", (byte) 5);
			put("StoreHost", (byte) 6);
			put("StartTime", (byte) 7);
			put("StopTime", (byte) 8);
			put("Timeout", (byte) 9);
			put("Priority", (byte) 10);
			put("Reliability", (byte) 11);
			put("SearchKey", (byte) 12);
			put("ScheduleExpression", (byte) 13);
			put("ShardingKey", (byte) 14);
			put("ShardingPartition", (byte) 15);
			put("TraceId", (byte) 16);
		}
	};	

}
