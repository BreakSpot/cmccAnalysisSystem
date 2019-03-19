package other;

import java.util.*;

public class MaxInterval {

    public static void main(String[] args) {
        ArrayList<Node> list = new ArrayList<Node>();
        double[] arr = new double[]{3,4,7,83, 54, 22, 34, 36};
        int n = arr.length;
        double Max = -1<<30;
        double Min = 1<<30;
        for(int i = 0; i < n; i++){
            Max = Math.max(Max, arr[i]);
            Min = Math.min(Min, arr[i]);
        }
        double len = (Max-Min)/(n-1);

        Node node = new Node();
        node.left = Min;
        node.right = Min+len;
        list.add(node);

        for(int i = 1; i < len-1; i++){
            Node node2 = new Node();
            node2.left = list.get(i-1).right;
            node2.right = node2.left+len;
            list.add(node2);
        }

        for(int i = 0; i < n; i++){
            double temp = Math.floor((arr[i] - Min)/len);
            int temp1 = (int) temp;
            if(temp1 >= n-1)
                temp1 = n -2;
            list.get(temp1).index++;
            if(arr[i] > list.get(temp1).max)
                list.get(temp1).max = arr[i];
            if(arr[i] < list.get(temp1).min)
                list.get(temp1).min = arr[i];
        }
        double max = Min;
        for(int i = 1; i < n-1; i++){
            if(list.get(i).min == Max)
                list.get(i).min = list.get(i-1).max;
            if(max < list.get(i).max- list.get(i).min)
                max = list.get(i).max- list.get(i).min;
            if(list.get(i).max == Min)
                list.get(i).max = list.get(i).min;
        }
        System.out.println(max);
    }
}

class Node{
    double left, right;
    double max = -1<<30, min = 1<<30;
    int index = 0;
}