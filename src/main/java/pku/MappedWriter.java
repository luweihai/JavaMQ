package pku;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class MappedWriter {

	private static final long SIZE = 1024 * 1024  ;
	private static final long MAX_MESSAGE_SIZE = 256 * 1024;
	private FileChannel fc;
	private MappedByteBuffer buf;
	private long offset;
	private int headNum;
	private String filePathName;
	
	IndexStringClass flag = new IndexStringClass() ;
	

	public MappedWriter(String filePathName) throws Exception {
		this.filePathName = filePathName;
		init();
	}
//RAS方式
  	private synchronized void init() throws Exception{
  		fc = new RandomAccessFile(filePathName, "rw").getChannel();         //  根据整个 fileName 得到 流 
		offset = 0;
		map(offset);
  	}
  	private  synchronized void map(long offset) throws Exception {    // 设置起始偏移量 
  		
  		buf = fc.map(FileChannel.MapMode.READ_WRITE, offset, SIZE);
  	}
  	public synchronized void send(ByteMessage message) throws Exception{
  		if (MAX_MESSAGE_SIZE > buf.remaining()) {     // 如果最大的消息  > buffer的剩余容量 
  			offset  = offset +  buf.position();   //   不是很懂这里 ？          
  			map(offset);
  		}
  		buf.putInt(-1);          // 惊为天人！！！！！！！
  		headNum = message.headers().keySet().size();
  		buf.putInt(headNum);                     //  写入那些固定头部的数目 
  		for (String key : message.headers().keySet()) {
  			StringBuilder sb = new StringBuilder();         // 注意是每次分开吖 
  			int type = typeJudge(message.headers().getObj(key));     // 得到是什么类型 
  			
  			sb.append(flag.getKey(key)).append(",").append(type);   // 此处的 getKey 就是 得到那些的索引 
  			writeString(sb.toString());                           //    写入了  索引+ ， + 分类   , type 也就是 1、2、3、4    
  			writeHeadValue(type, message.headers().getObj(key));     // 写入   分类  + value
  		}
  		int isCompress = 0;
  		byte[] body = null;
  		if (message.getBody().length > 200) {     // 消息的 body的 byte数组 大于 200    调参数的过程  
			body = compress(message.getBody());   // 对 body 压缩
			isCompress = 1;       // 记录被压缩了 
		}
		else {
			body=message.getBody();      // 不压缩了
			isCompress = 0;
		}
  		buf.putInt(isCompress);
  		buf.putInt(body.length);
  		buf.put(body);

  	}
  	private synchronized void  writeString(String str){
  		byte[] data = str.getBytes();
  		buf.putInt(data.length);
  		buf.put(data);
  	}
  	private void writeHeadValue(int type, Object obj) {
  		
  		
  		if (type == flag.tSTRING) {   //  1 、 2 、 3 、 4 
  			writeString((String) obj);
  		}
  		if (type == flag.tLONG) {
  			buf.putLong(((Long) obj));
  		}
  		if (type == flag.tDOUBLE) {
  			buf.putDouble(((Double) obj));
  		}
  		if (type == flag.tINT) {
  			buf.putInt(((Integer) obj));
  		}
  	}
  	private int typeJudge(Object obj) {
  		if (obj instanceof Long) {
  			return flag.tLONG;    // 也就是得到 1 、2、3、4 
  		}
  		if (obj instanceof Double) {
  			return flag.tDOUBLE;
  		}
  		if (obj instanceof Integer) {
  			return flag.tINT;
  		}
  		return flag.tSTRING;
  	}
  	public void close() throws Exception {
  		fc.close();
  	}
  	
  	
  	
  	public static byte[] compress(byte[] data) {
		byte[] new_Data = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			gzip.finish();
			gzip.close();
			new_Data = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return new_Data;
	}

	
}
