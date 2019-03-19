package sort;

import java.util.Arrays;

public class HeapSort {
    public static void main(String[] args) {
        int[] arr = new int[]{2, 1, 4, 3, 6, 5, 8, 7};
        sort(arr);
        System.out.println(Arrays.toString(arr));
    }

    public static void sort(int[] arr){
        int len = arr.length;
        for(int i = len/2-1; i>=0; i--){
            adjustHeap(arr, i, len);
        }

        for(int j = len-1; j > 0; j--){
            swap(arr, 0, j);
            adjustHeap(arr, 0, j);
        }
    }

    public static void adjustHeap(int[] arr, int i, int len){
        int temp = arr[i];
        for(int k = 2*i+1; k < len; k = 2*k+1){
            if(k+1 < len && arr[k] < arr[k+1])
                k++;

            if(arr[k] > temp){
                swap(arr, i, k);
                i = k;
            }else{
                break;
            }
        }
    }

    public static void swap(int[] arr, int a, int b){
        int temp = arr[a];
        arr[a] = arr[b];
        arr[b] = temp;
    }
}
