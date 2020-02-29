package com.tom.test;

/**
 * ClassName: Test
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/28 17:01
 */
public class Test {
    public static void main(String[] args) {
        int[] arr = {1, 2, 5, 73, 4, 6, 10, 5, 2, 4};
        quickSort(arr, 0, arr.length);
        for (int i : arr) {
            System.out.println(i);
        }
    }

    /*快排*/
    public static void quickSort(int[] arr, int bgn, int end) {

        //基准数左面的 最大 索引
        int lindex = bgn;
        //基准数右面的 最小 索引
        int rindex = end - 1;
        //基准数
        int std = arr[lindex];
        while (lindex < rindex) {
            while (lindex < rindex) {
                if (arr[rindex] < std) {
                    arr[lindex++] = arr[rindex];
                    break;
                }
                --rindex;
            }
            while (lindex < rindex) {
                if (arr[lindex] >= std) {
                    arr[rindex--] = arr[lindex];
                    break;
                }
                ++lindex;
            }
        }

        arr[lindex] = std;
        quickSort(arr, bgn, lindex);
        quickSort(arr, rindex + 1, end);
    }


    /*选择排序*/
    public static void selectSort(int[] arr, int start, int end) {
        for (int i = start; i < end; ++i) {
            int minIndex = i;
            for (int j = i + 1; j < end; ++j) {
                if (arr[j] < arr[minIndex])
                    minIndex = j;
            }
            if (minIndex != i) {
                int temp = 0;
                temp = arr[i];
                arr[i] = arr[minIndex];
                arr[minIndex] = temp;
            }
        }
    }
}
