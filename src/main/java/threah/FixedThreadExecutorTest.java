package threah;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FixedThreadExecutorTest {
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        Thread t1 = new MyThread2();
        Thread t2 = new MyThread2();
        Thread t3 = new MyThread2();
        Thread t4 = new MyThread2();
        Thread t5 = new MyThread2();

        pool.execute(t1);
        pool.execute(t2);
        pool.execute(t3);
        pool.execute(t4);
        pool.execute(t5);

        pool.shutdown();
    }
}

class MyThread2 extends Thread {

    @Override
    public void run() {
        // TODO Auto-generated method stub
//        super.run();
        System.out.println(Thread.currentThread().getName()+"正在执行....");
    }
}