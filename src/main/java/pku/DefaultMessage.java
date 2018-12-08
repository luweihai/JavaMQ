package pku;

/**
 *消息的实现
 */
public class DefaultMessage implements ByteMessage{

    private KeyValue headers = new DefaultKeyValue();
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public DefaultMessage(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public KeyValue headers() {
        return headers;
    }
<<<<<<< HEAD


    public DefaultMessage putHeaders(String key, int value) {
=======
    

    public DefaultMessage putHeaders(String key, int value) {   // value 是 topic ， key是那些静态常量
>>>>>>> JavaMQ
        headers.put(key, value);
        return this;
    }

<<<<<<< HEAD
    public DefaultMessage putHeaders(String key, long value) {
=======
    public DefaultMessage putHeaders(String key, long value) {   //  int long double是为了让我们简化？ 
>>>>>>> JavaMQ
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

<<<<<<< HEAD
    public DefaultMessage putHeaders(String key, String value) {
=======
    public DefaultMessage putHeaders(String key, String value) {   // 基本上只用 String 类型的
>>>>>>> JavaMQ
        headers.put(key, value);
        return this;
    }

<<<<<<< HEAD
=======
    
    
    
>>>>>>> JavaMQ
}
