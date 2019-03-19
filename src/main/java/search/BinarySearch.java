package search;

public class BinarySearch {
    public static void main(String[] args) {
        int arr[] = new int[]{1,3,4,5,7,8};
        int pos = binarySearch(arr, 4);
        System.out.println(pos);
    }
    public static int binarySearch(int arr[], int key){
        int len = arr.length;
        int left = 0, right = len-1;
        while(left <= right){
            int min = (left + right)/2;
            if(arr[min] > key)
                right = min-1;
            if(arr[min] < key)
                left = min+1;
            if(arr[min] == key)
                return min;
        }
        return -1;
    }
}
