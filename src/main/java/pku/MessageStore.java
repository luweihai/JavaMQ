package pku;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;



public class MessageStore {
	
	protected static final MessageStore store = new MessageStore();
	
	HashMap<String , DataOutputStream >  map_Out = new HashMap<>();
	HashMap<String  , DataInputStream  >  map_In = new HashMap<>();
	
	private static class change_Headers_Key{
		/*
		 * 下面的匿名内部类是初始化 索引-固定头部-分类的 映射关系
		 */
		
		public static final HashMap<Byte, String> indexToString = new HashMap<Byte, String>() {    // key是索引，value是固定头部 ？ 
			{
				put((byte) 1, "MessageId");
				put((byte) 2, "Topic");
				put((byte) 3, "BornTimestamp");
				put((byte) 4, "BornHost");
				put((byte) 5, "StoreTimestamp");
				put((byte) 6, "StoreHost");
				put((byte) 7, "StartTime");
				put((byte) 8, "StopTime");
				put((byte) 9, "Timeout");
				put((byte) 10, "Priority");
				put((byte) 11, "Reliability");
				put((byte) 12, "SearchKey");
				put((byte) 13, "ScheduleExpression");
				put((byte) 14, "ShardingKey");
				put((byte) 15, "ShardingPartition");
				put((byte) 16, "TraceId");
			}
		};
		public static final HashMap<String, Short> stringToClass = new HashMap<String, Short>() {   
			{                                          // 此处的 value 是 数字，表示是属于哪个类型  
				put("MessageId", (short) 3);             // int
				put("Topic", (short) 4);                 // String 
				put("BornTimestamp", (short) 1);        // long  
				put("BornHost", (short) 4);              // String 
				put("StoreTimestamp", (short) 1);        // long 
				put("StoreHost", (short) 4);             // String 
				put("StartTime", (short) 1);            // long  
				put("StopTime", (short) 1);            // long  
				put("Timeout", (short) 3);               // int 
				put("Priority", (short) 3);                // int 
				put("Reliability", (short) 3);              // int  
				put("SearchKey", (short) 4);                // String 
				put("ScheduleExpression", (short) 4);        // String 
				put("ShardingKey", (short) 2);            // double 
				put("ShardingPartition", (short) 2);          // double 
				put("TraceId", (short) 4);                  // String  
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
		
		public static final HashMap<Byte, Byte> indexToClass = new HashMap<Byte, Byte>() {   //  索引  到  类型 的  key-value 对
			{
				put((byte) 1, (byte) 3);                            // int
				put((byte) 2, (byte) 4);                            // String 
				put((byte) 3, (byte) 1);                            // long 
				put((byte) 4, (byte) 4);                            // String 
				put((byte) 5, (byte) 1);                            // long 
				put((byte) 6, (byte) 4);                            // String 
				put((byte) 7, (byte) 1);                            // long 
				put((byte) 8, (byte) 1);                            // long 
				put((byte) 9, (byte) 3);                            // int 
				put((byte) 10, (byte) 3);                           // int 
				put((byte) 11, (byte) 3);                           // int 
				put((byte) 12, (byte) 4);                           // String 
				put((byte) 13, (byte) 4);                           // String 
				put((byte) 14, (byte) 2);                           // double 
				put((byte) 15, (byte) 2);                           // double
				put((byte) 16, (byte) 4);                           // String
			}
		};
	}
	
	
	public void flush() throws IOException{
		Producer.count --;
		int count = Producer.count ;
		if( count != 0)
			return ;
		
		for(String head : map_Out.keySet() ){
			map_Out.get(head).close();
		}
		
	}
	
	public synchronized void push( ByteMessage msg , String topic ) throws IOException{
		if(msg == null )
			return ;
		DataOutputStream temp_Out = null;
		if( map_Out.containsKey( topic ) == false ){
			FileOutputStream fs = new FileOutputStream("./data/" + topic, true);
			temp_Out = new DataOutputStream( fs );
			map_Out.put(topic, temp_Out);
		}
		else{
			temp_Out = map_Out.get(topic);
		}
		
		
		
		byte[] body = null;
		byte head_Num = (byte)msg.headers().keySet().size();
		byte is_Compress ;
		
		if (msg.getBody().length > 1024) {     // 消息的 body的 byte数组 大于 1024 
			body = compress(msg.getBody());   // 对 body 压缩
			is_Compress = 1;       // 记录被压缩了 
		}
		else {
			body  =  msg.getBody();      // 不压缩了
			is_Compress = 0;
		}

		
		temp_Out.writeByte(head_Num);
		for( String headers_Key :  msg.headers().keySet() ){
			
			temp_Out.writeByte(change_Headers_Key.stringToClassByte.get(headers_Key)); 
			
		//	System.out.println( change_Headers_Key.stringToClassByte.get(headers_Key) + "&&&&" );    // 这里都是正确的 
			
			switch (change_Headers_Key.stringToClass.get(headers_Key)) {             // 此处 得到 short 类型的 数字化 类型 
			case 1:
				temp_Out.writeLong(msg.headers().getLong(headers_Key));               // 写入  long 类型的 topic 
				break;
			case 2:
				temp_Out.writeDouble(msg.headers().getDouble(headers_Key));               // 写入 double 类型的 topic
				break;
			case 3:
				temp_Out.writeInt(msg.headers().getInt(headers_Key));                  // 写入 int 类型的 topic
				break;
			case 4:
				temp_Out.writeUTF(msg.headers().getString(headers_Key));            // 写入 String 类型的 topic     此处是用输出流的 writeUTF
				break;
			}
		}
		temp_Out.writeByte(is_Compress);           //  is_Compress = 0 或者 1  表明 是否被压缩 
		temp_Out.writeShort(body.length);         // 写入 body，也就是 数据部分 的长度  用 short是为了节约 
		temp_Out.write(body);	
	}
	
	
	public synchronized  ByteMessage pull(String queue  , String topic) throws IOException{
		
		String queue_Topic = queue + " " + topic;
		DataInputStream temp_In = null;
				
		
		if (!map_In.containsKey(queue_Topic)) {
			try {
				FileInputStream fs = new FileInputStream("./data/" + topic );
				temp_In = new DataInputStream( fs );;
			} catch (FileNotFoundException e) {
				// e.printStackTrace();
				return null;
			}
			synchronized (map_In) {
				map_In.put(queue_Topic, temp_In);
			}
		} else {
			temp_In= map_In.get(queue_Topic);
		}  
		
		
		
		if( temp_In.available() == 0 )
			return null;
		ByteMessage msg = new DefaultMessage( null );
		
		byte head_Type ;
		byte head_Num = temp_In.readByte();  // 读出的第一个是 head_Num  
	//	System.out.println(head_Num + "$$");            // 这里就有问题了 
		for(int i = 0  ; i < head_Num ; i ++){
			
			head_Type = temp_In.readByte();
			
	//		System.out.println(head_Type + "asdasd");     
			
			switch (change_Headers_Key.indexToClass.get(head_Type)) {     //  short 类型的 分类 
			case  1:
				msg.putHeaders(change_Headers_Key.indexToString.get(head_Type), temp_In.readLong());  // 生成 message头部，key 是 short的分类，value 是 topic 
				break;                                                                         // 为什么 key 是分类？ 
			case  2:
				msg.putHeaders(change_Headers_Key.indexToString.get(head_Type), temp_In.readDouble());
				break;
			case  3:
				msg.putHeaders(change_Headers_Key.indexToString.get(head_Type), temp_In.readInt());
				break;
			case  4:
				msg.putHeaders(change_Headers_Key.indexToString.get(head_Type), temp_In.readUTF());   // 此处是用输入流的 readUTF() 
				break;
			}
		}
		
		msg.putHeaders("Topic", topic);
		byte is_Compress = temp_In.readByte();
		short length = temp_In.readShort();
		byte[] data = new byte[length];
		temp_In.read(data);
		
		if (is_Compress == 1) {                     // 如果压缩了
			msg.setBody(uncompress(data));      // 解压，并且 setBody
		} else {                         
			msg.setBody(data);                        // 直接 setBody
		}
		
		return msg;
	}
	
	
	public static byte[] compress(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			gzip.finish();
			gzip.close();
			b = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
	
	public static byte[] uncompress(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			GZIPInputStream gzip = new GZIPInputStream(bis);
			byte[] buf = new byte[1024];
			int num = -1;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((num = gzip.read(buf, 0, buf.length)) != -1) {
				baos.write(buf, 0, num);
			}
			b = baos.toByteArray();
			baos.flush();
			baos.close();
			gzip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
	
}
