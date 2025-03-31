package javaSort;

/**
 * @author stalwarthuang
 * @since 2025-03-29 星期六 11:28:47
 */
public class qsort {
    public static void main(String[] args) {
        int[] nums = new int[]{4, 6, 3, 2, 1, 9, 5, 88};
        int n = nums.length;
        bubbleSort(nums);
        for(int i:nums) {
            System.out.print(i + ", ");
        }
    }

    static void bubbleSort(int[] nums) {
        int n = nums.length;
        for(int round = 1; round < n; round++) {
            int times = n - round;
            boolean change = false;
            for(int i = 0; i < times; i++) {
                if(nums[i] > nums[i + 1]) {
                    int tmp = nums[i];
                    nums[i] = nums[i + 1];
                    nums[i + 1] = tmp;
                    change = true;
                }
            }
            if(!change) {
                break;
            }
        }
    }
}



