package sort;

public class MergeSort {
    public static void main(String[] args) {
        Integer arr[] = new Integer[]{21,3,30,5,27,8,2,54,65,322,5,0,7,34,66,11,30,83,74,95,5,75,35,26};
        sort(arr);
        for (int i : arr) {
            System.out.print(i+" ");
        }
    }

    private static void merge(Comparable[] src, Comparable[] dst, int lo, int mid, int hi) {
        int indexI = lo;
        int indexJ = mid + 1;
        for (int indexK = lo; indexK <= hi; indexK++) {
            if (indexI > mid) {
                dst[indexK] = src[indexJ++];
            } else if (indexJ > hi) {
                dst[indexK] = src[indexI++];
            } else if (less(src[indexJ], src[indexI])) {
                dst[indexK] = src[indexJ++];
            } else {
                dst[indexK] = src[indexI++];
            }
        }
    }

    // 将数组 arr 排序到数组 aux
    private static void sort(Comparable[] src, Comparable[] dst, int lo, int hi) {
        // 优化方案②：应该在子数组长度为 7 的时候切换到插入排序
        if (hi <= lo) {
            return;
        }
        int mid = lo + ((hi - lo) >> 1);

        // 优化方案④：在每个层次交换输入数组和辅助数组的角色
        sort(dst, src, lo, mid);
        sort(dst, src, mid + 1, hi);

        //优化方案③：判断测试数组是否已经有序
        if (!less(src[mid + 1], src[mid])) {
            System.arraycopy(src, lo, dst, lo, hi - lo + 1);
            return;
        }

        // 优化方案④：merge() 方法中不将元素复制到辅助数组
        merge(src, dst, lo, mid, hi);
    }

    public static void sort(Comparable[] arr) {
        // 优化方案①：直接将辅助数组作为参数传入
        Comparable[] aux = arr.clone();
        sort(aux, arr, 0, arr.length - 1);
    }

    private static boolean less(Comparable comparableA, Comparable comparableB) {
        return comparableA.compareTo(comparableB) < 0;
    }
}
