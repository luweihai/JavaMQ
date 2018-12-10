package pku;

import java.util.HashMap;

public class IndexStringClass {
  
	  public  final int tSTRING= 1; 
	  public  final int tDOUBLE= 2; 
	  public  final int tLONG= 3; 
	  public  final int tINT= 4;    

	  public  int getKey(String str){
		  return stringToIndex.get(str);
		  
	  }
	  public  String getStrKey(int i){
		  return indexToString.get(i);
	  }
	  
	  //public int getType(  )
	  
	  public  final HashMap<String, Integer> stringToIndex = new HashMap<String, Integer>() {     // value 表明索引
			{
				put(MessageHeader.TOPIC , 0);                      
				put(MessageHeader.BORN_HOST ,  1);                           
				put(MessageHeader.BORN_TIMESTAMP ,  2);
				put(MessageHeader.MESSAGE_ID , 3);
				put(MessageHeader.STORE_TIMESTAMP ,  4);
				put(MessageHeader.STORE_HOST , 5);
				put(MessageHeader.START_TIME ,  6);
				put(MessageHeader.STOP_TIME , 7);
				put(MessageHeader.TIMEOUT ,  8);
				put(MessageHeader.PRIORITY , 9);
				put(MessageHeader.RELIABILITY ,  10);
				put(MessageHeader.SEARCH_KEY ,  11);
				put(MessageHeader.SCHEDULE_EXPRESSION ,  12);
				put(MessageHeader.SHARDING_KEY , 13);
				put(MessageHeader.SHARDING_PARTITION , 14);
				put(MessageHeader.TRACE_ID ,  15);
			}
		};
	  
	  
	  
	  
	  
	  public  final HashMap<String, Integer> stringToClass = new HashMap<String, Integer>() {   
			{                                          // 此处的 value 是 数字，表示是属于哪个类型  
				
				put(MessageHeader.TOPIC , 4 );                      
				put(MessageHeader.BORN_HOST ,  4 );                           
				put(MessageHeader.BORN_TIMESTAMP ,  1);
				put(MessageHeader.MESSAGE_ID , 3);
				put(MessageHeader.STORE_TIMESTAMP ,  1);
				put(MessageHeader.STORE_HOST , 4);
				put(MessageHeader.START_TIME ,  1);
				put(MessageHeader.STOP_TIME , 1 );
				put(MessageHeader.TIMEOUT ,  3 );
				put(MessageHeader.PRIORITY , 3 );
				put(MessageHeader.RELIABILITY ,  3);
				put(MessageHeader.SEARCH_KEY ,  4);
				put(MessageHeader.SCHEDULE_EXPRESSION ,  4);
				put(MessageHeader.SHARDING_KEY , 2);
				put(MessageHeader.SHARDING_PARTITION , 2);
				put(MessageHeader.TRACE_ID ,  4);
				
			
			}
		};
		public  final  HashMap<Integer, String> indexToString = new HashMap<Integer, String>() {    
			{
				put(0, MessageHeader.TOPIC );                      
				put(1, MessageHeader.BORN_HOST );                           
				put(2, MessageHeader.BORN_TIMESTAMP );
				put(3,  MessageHeader.MESSAGE_ID );
				put(4,  MessageHeader.STORE_TIMESTAMP );
				put(5,  MessageHeader.STORE_HOST );
				put(6,  MessageHeader.START_TIME );
				put(7,  MessageHeader.STOP_TIME );
				put(8,  MessageHeader.TIMEOUT );
				put(9,  MessageHeader.PRIORITY );
				put(10, MessageHeader.RELIABILITY );
				put(11, MessageHeader.SEARCH_KEY );
				put(12, MessageHeader.SCHEDULE_EXPRESSION );
				put(13, MessageHeader.SHARDING_KEY );
				put(14, MessageHeader.SHARDING_PARTITION );
				put(15, MessageHeader.TRACE_ID );
			}
		};
	  
	  
}
