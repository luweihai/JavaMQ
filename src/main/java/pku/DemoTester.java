package pku;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * 这个程序演示了测评程序的基本逻辑
 * 正式的测评程序会比这个更复杂
 */
public class DemoTester {
    //每个pusher向每个topic发送的消息数目
    static int PUSH_COUNT = 1000;
    //发送消息的线程数
    static int PUSH_THREAD_COUNT = 4;
    //发送线程往n个topic发消息
    static int PUSH_TOPIC_COUNT = 10;
    //消费消息的线程数
    static int PULL_THREAD_COUNT = 4;
    //每个消费者消费的topic数量
    static int PULL_TOPIC_COUNT = 10;
    //topic数量
    static int TOPIC_COUNT = 20;
    //每个queue绑定的topic数量
    static int ATTACH_COUNT = 2;
    //统计push/pull消息的数量
    static AtomicInteger pushCount = new AtomicInteger();
    static AtomicInteger pullCount = new AtomicInteger();

    static class PushTester implements Runnable {
        //随机向以下topic发送消息
        List<String> topics = new ArrayList<>();    // 存储 多个topic的List
        Producer producer = new Producer();     // 一个 PushTester对应一个 Producer和一个 topics列，以及一个 id 
        int id;
        PushTester(List<String> t, int id) {   // id 是表示这是第 i 个线程，也就是第 i 个PushTester
            topics.addAll(t);
            this.id = id;                             //  id 应该也是表示  第 id 个线程 ，也就是对应一个 PushTester 和  producer
            StringBuilder sb=new StringBuilder();
            sb.append(String.format("producer%d push to:",id));
            for (int i = 0; i <t.size() ; i++) {
                sb.append(t.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {     // 每个 PushTester 一个 topics列 ，然后一个 topic 对应 PUSH_COUNT个 msg 
            try {
                for (int i = 0; i < topics.size(); i++) {
                    String topic = topics.get(i);
                    for (int j = 0; j < PUSH_COUNT; j++) {
                        //topic加j作为数据部分
                        //j是序号, 在consumer中会用来校验顺序                            注意 data里面有 topic，Header 里面的value 也是 topic
                        byte[] data = (topic +" "+id + " " + j).getBytes();    // data 是 topic 、 id 、 序号j 的组合
                        ByteMessage msg = producer.createBytesMessageToTopic(topics.get(i), data);
                        //设置一个header
                        msg.putHeaders(MessageHeader.SEARCH_KEY, "hello");   // 为msg设置 Headers， value 是一个标志？ 但是为什么是 hello？
                        //发送消息                                                                                                                                     hello 见 122行   实际测评的时候就不只用 hello ，甚至还有 int long 
                        producer.send(msg);        // 发出 msg  
                        pushCount.incrementAndGet();
                    }
                }
                producer.flush();   // 刷新出剩余未发送出去的数据，是对于一个 PushTester 发出大部分后剩余一下部分 
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    static class PullTester implements Runnable {
        //拉消息
        String queue;    //  应该是每个 PullTester对应一个 queue,以及一个 consumer
        List<String> topics = new ArrayList<>();
        Consumer consumer = new Consumer();
        int pc=0;                           //  pc 是什么意思呢？ 应该是表示 取出的消息的序号  
        public PullTester(String s, ArrayList<String> tops) throws Exception {    // tops 也就是 topics
            queue = s;
            topics.addAll(tops);             // 从构造方法中传入的 topic 序列形成  tops
            consumer.attachQueue(s, tops);   // 绑定 queue 和 tops。经过实验，此处换成 topics也是一样的结果  

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("queue%s attach:",s));  // 一个 queue 绑定 多个 topic 
            for (int i = 0; i <topics.size() ; i++) {
                sb.append(topics.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {
            try {
                //检查顺序, 保存每个topic-producer对应的序号, 新获得的序号必须严格+1
                HashMap<String, Integer> posTable = new HashMap<>();   // key 是  "topic" + "id" ,value 是 以此 topic 
                while (true) {                                          //  所对应的 第 n 个消息 ，最多为 PUSH_COUNT
                    ByteMessage msg = consumer.poll();    // 只依赖于 consumer的 poll()方法
                    if (msg == null) {    // 当一个线程（queue） 读完了它全部的消息 
                        System.out.println(String.format("thread pull %s",pc));
                        return;
                    } else {
                        byte[] data = msg.getBody();
                        String str = new String(data);
                        String[] strs = str.split(" ");  // 把 data用 split分割开 ,因为之前的data 是用" " 连接的
                        String topic = strs[0];
                        String prod = strs[1];            // prod 就是前面的 id  
                        int j = Integer.parseInt(strs[2]);   // 第3部分是内层循环 j 
                        String mapkey=topic+" "+prod;
                        if (!posTable.containsKey(mapkey)) {
                            posTable.put(mapkey, 0);
                        }
                        if (j != posTable.get(mapkey)) {     // 取出的序号与存入的序号相同 
                            System.out.println(String.format("数据错误 topic %s 序号:%d", topic, j));
                            System.exit(0);   // 因为一个 topic可以有多个消息 ，内存循环是 PUSH_COUNT,所以有此步骤 
                        }
                        if (!msg.headers().getString(MessageHeader.SEARCH_KEY).equals("hello")) {  // 验证 topic是否相同
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            System.exit(0);   
                        }
                        posTable.put(mapkey, posTable.get(mapkey) + 1);
                        pullCount.incrementAndGet();
                        pc++;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }
    static Random rand = new Random(100);

    static void testPush()throws Exception{
        //topic的名字是topic+序号的形式
        System.out.println("开始push");
        long time1 = System.currentTimeMillis();
        ArrayList<Thread> pushers = new ArrayList<>();      // pushers 是pusher形成的 List
        for (int i = 0; i < PUSH_THREAD_COUNT; i++) {      // 遍历 push 线程数
            //随机选择连续的topic
            ArrayList<String> tops = new ArrayList<>();   // topic序列 tops
            int start = rand.nextInt(TOPIC_COUNT);       // 随机以TOPIC_COUNT 为上届取随机数 start 
            for (int j = 0; j < PUSH_TOPIC_COUNT; j++) {
                int v = (start+j)  %  TOPIC_COUNT;     // 从 随机数start 开始为 topic 序号 
                tops.add("topic"+Integer.toString(v));    // 生成真正的 topic序列 
            }
            Thread t = new Thread(new PushTester(tops, i));   // 第二个参数是 第 i 个线程，也就是 id 
            t.start();
            pushers.add(t);                                // 当前线程t 添加进 pushers这个 List
        }
        for (int i = 0; i < pushers.size(); i++) {       // 遍历 pushers 
            pushers.get(i).join();                      //   执行完 调用join()的线程后再执行当前线程 
        }
        long time2 = System.currentTimeMillis();
        System.out.println(String.format("push 结束 time cost %d push count %d", time2 - time1, pushCount.get()));  // 计算 push 耗时
    }

    static void testPull() throws Exception{
        long time2=System.currentTimeMillis();
        System.out.println("开始pull");
        int queue = 0;                               
        ArrayList<Thread> pullers = new ArrayList<>();     // pullers 是 puller形成的 List
        for (int i = 0; i < PULL_THREAD_COUNT; i++) {           // 遍历取出线程数目 
            //随机选择topic
            ArrayList<String> tops = new ArrayList<>();
            int start = rand.nextInt(TOPIC_COUNT);           // 随机取 start 
            for (int j = 0; j < PULL_TOPIC_COUNT; j++) {
                int v =(start + j ) % TOPIC_COUNT;          // 取出topic 并从 start开始为序号
                tops.add("topic" + Integer.toString(v));       // 生成真正的 topic序列 tops
            }
            Thread t = new Thread(new PullTester(Integer.toString(queue), tops));   // 第一个参数是 queue
            queue++;           // queue 更新
            t.start();
            pullers.add(t);
        }
        for (int i = 0; i < pullers.size(); i++) {   // 如果没有这个，main，以及 pull 结束 都可能会乱套 
            pullers.get(i).join();
        }
        long time3 = System.currentTimeMillis();
        System.out.println(String.format("pull 结束 time cost %d pull count %d", time3 - time2, pullCount.get()));
    }

    public static void main(String args[]) {
        try {
                testPush();
                testPull();
        } catch (Exception e) {
            e.printStackTrace();
        }
        

    }
}
