package threah;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachedThreadExecutorTest {
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        //创建一个可重用固定线程数的线程池
        ExecutorService pool= Executors.newCachedThreadPool();

        //创建实现了Runnable接口对象，Thread对象当然也实现了Runnable接口;


        Thread t1=new MyThread();

        Thread t2=new MyThread();

        Thread t3=new MyThread();

        Thread t4=new MyThread();

        Thread t5=new MyThread();

        //将线程放到池中执行；

        pool.execute(t1);

        pool.execute(t2);

        pool.execute(t3);

        pool.execute(t4);

        pool.execute(t5);

        //关闭线程池

        pool.shutdown();

    }
}
