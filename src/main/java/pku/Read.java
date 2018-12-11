package pku;

import java.io.DataInputStream;
import java.util.HashMap;


public class Read {
	

	public static ByteMessage read(DataInputStream  temp_In , String queue, String topic ) throws Exception{
		
		
		if (temp_In.available() != 0) {                  // 此方法是返回流中实际可以读取的字节数目  

			ByteMessage msg = new DefaultMessage(null);        
			byte head_Type ;
			byte head_Num = temp_In.readByte();              
			for (int i = 0; i < head_Num; i++) {

				                                          //  接下来就是读取依此出现过的 固定头部 
				head_Type = temp_In.readByte();                //  在循环中获取 固定头部的  byte类型的 下标 
				switch (indexToClass.get(head_Type)) {    
				case  1:
					msg.putHeaders(indexToString.get(head_Type), temp_In.readLong());   
					break;                                                                          
				case  2:
					msg.putHeaders(indexToString.get(head_Type), temp_In.readDouble());
					break;
				case  3:
					msg.putHeaders(indexToString.get(head_Type), temp_In.readInt());
					break;
				case  4:
					msg.putHeaders(indexToString.get(head_Type), temp_In.readUTF());   // 此处是用输入流的 readUTF() 
					break;
				}
			}
			
			byte is_Compress = temp_In.readByte();           // 读一个byte 表示 是否被压缩了                
			short length = temp_In.readShort();             //  读一个 short ，代表 data 的长度 
			byte[] data = new byte[length];                 // 用 byte数组 存储 data 
			temp_In.read(data);             
			if (is_Compress == 1) {                     // 如果压缩了
				try {
					msg.setBody(Compress.uncompressGZ(data));
				} catch (Exception e) {
					e.printStackTrace();
				}    
			} else {                         
				msg.setBody(data);                        // 直接 setBody
			}
			return msg;           
		} else {                                   // 没有可读的消息了 
			return null;
		}
	}
	public static final  HashMap<Byte, String> indexToString = new HashMap<Byte, String>() {    
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
