package pku;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;


public class MappedReader {
	private FileChannel fc;
	private MappedByteBuffer buf;
	private String filename, storePath;
	public MappedReader(String storePath, String filename) throws Exception {    // 根据   storePath + fileName 初始化
		this.filename = filename;
		this.storePath = storePath;
		init();
	}
	//RAS方式
	private synchronized void init() throws Exception {
		fc = new RandomAccessFile(storePath + "/" + filename, "rw").getChannel();
		buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size() );   // 0 是起始偏移量         fc.size() 这是什么操作？？？
	}
	public ByteMessage poll() {
		ByteMessage msg =null;
		try {                         // 这个 try 很重要 不然一直出现异常     
			if ((buf.getInt() == -1) && (buf.hasRemaining())) {
				int headNum = buf.getInt();
				msg = new DefaultMessage();
				for (int i = 0; i < headNum; i++) {
					String str[] = readString().split(",");
					int headKey = Integer.valueOf(str[0]);
					int valueType = Integer.valueOf(str[1]);
					setHead(valueType, headKey, msg);
				}
				int compre = buf.getInt();
				
				
				int bodyLen = buf.getInt();
				byte[] body = new byte[bodyLen];
				buf.get(body);
				
				if (compre == 1) {                     // 如果压缩了
					try {
						msg.setBody(uncompress(body));
					} catch (Exception e) {
						e.printStackTrace();
					}      // 解压，并且 setBody
				} else {                         
					msg.setBody(body);                        // 直接 setBody
				}
				
				
				
				
			}
		}catch (BufferUnderflowException e){
			return msg;
		}
		return msg;
	}

	private void setHead(int valueType, int headKey, ByteMessage msg)throws BufferUnderflowException {
		
		IndexStringClass flag = new IndexStringClass();
		if (valueType == flag.tSTRING) {
			msg.putHeaders(flag.getStrKey(headKey), readString());
		}
		if (valueType == flag.tDOUBLE) {

			msg.putHeaders(flag.getStrKey(headKey), buf.getDouble());
		}
		if (valueType == flag.tINT) {
			
			msg.putHeaders(flag.getStrKey(headKey), buf.getInt());
		}
		if (valueType == flag.tLONG) {

			msg.putHeaders(flag.getStrKey(headKey), buf.getLong());
		}
  }
	private String readString() throws BufferUnderflowException{
		int length = buf.getInt();
		byte[] data = new byte[length];
		buf.get(data);
		return new String(data);
	}
	public void close() throws Exception {
		fc.close();
	}
	
	public static byte[] uncompress(byte[] data) {
		byte[] new_Data = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			GZIPInputStream gzip = new GZIPInputStream(bis);
			byte[] buf = new byte[1000];
			int num = -1;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((num = gzip.read(buf, 0, buf.length)) != -1) {
				baos.write(buf, 0, num);
			}
			new_Data = baos.toByteArray();
			baos.flush();
			baos.close();
			gzip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return new_Data;
	}
}
