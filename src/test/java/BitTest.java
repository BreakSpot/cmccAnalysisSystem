import org.junit.Test;

import java.util.Date;


public class BitTest {

    @Test
    public void test2(){
        int nun = 1000000;
        int arr[] = new int[nun];
        int arr2[] = new int[nun];
        int arr3[] = new int[nun];
        int h = 0;
        while(true){
            h ++;
            for(int i = 0; i < nun; i++){
                arr[i] = i;
                arr3[i] = i;
                arr2[i] = (int)Math.random()*nun;
            }

            long startTime = System.currentTimeMillis();
            for(int i = 0; i < nun; i++){
                arr[i] = arr3[i];
            }
            long endTime=System.currentTimeMillis();
            long t1 = endTime-startTime;


            long startTime2 = System.currentTimeMillis();
            for(int i = 0; i < nun; i++){
                arr[i] = arr2[i];
            }
            long endTime2=System.currentTimeMillis();
            long t2 = endTime2-startTime2;
            if(h < nun){
                System.out.println(t1-t2);
              //  break;
            }

        }

    }
}
