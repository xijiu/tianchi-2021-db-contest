package com.aliyun.adb.contest.utils;

public class PubTools {

  public static void myQuickSort(long[] arr, int low, int high) {
    int i, j;
    long temp, t;
    if (low > high) {
      return;
    }
    i = low;
    j = high;
    //temp就是基准位
    temp = arr[low];

    while (i < j) {
      //先看右边，依次往左递减
      while (temp <= arr[j] && i < j) {
        j--;
      }
      //再看左边，依次往右递增
      while (temp >= arr[i] && i < j) {
        i++;
      }
      //如果满足条件则交换
      if (i < j) {
        t = arr[j];
        arr[j] = arr[i];
        arr[i] = t;
      }

    }
    //最后将基准为与i和j相等位置的数字交换
    arr[low] = arr[i];
    arr[i] = temp;
    //递归调用左半数组
    myQuickSort(arr, low, j - 1);
    //递归调用右半数组
    myQuickSort(arr, j + 1, high);
  }


  public static void quickSort(long[] a, int left, int right, boolean leftmost) {
    int length = right - left + 1;

    // Use insertion sort on tiny arrays
    if (length < 47) {
      if (leftmost) {
        /*
         * Traditional (without sentinel) insertion sort,
         * optimized for server VM, is used in case of
         * the leftmost part.
         */
        for (int i = left, j = i; i < right; j = ++i) {
          long ai = a[i + 1];
          while (ai < a[j]) {
            a[j + 1] = a[j];
            if (j-- == left) {
              break;
            }
          }
          a[j + 1] = ai;
        }
      } else {
        /*
         * Skip the longest ascending sequence.
         */
        do {
          if (left >= right) {
            return;
          }
        } while (a[++left] >= a[left - 1]);

        /*
         * Every element from adjoining part plays the role
         * of sentinel, therefore this allows us to avoid the
         * left range check on each iteration. Moreover, we use
         * the more optimized algorithm, so called pair insertion
         * sort, which is faster (in the context of Quicksort)
         * than traditional implementation of insertion sort.
         */
        for (int k = left; ++left <= right; k = ++left) {
          long a1 = a[k], a2 = a[left];

          if (a1 < a2) {
            a2 = a1; a1 = a[left];
          }
          while (a1 < a[--k]) {
            a[k + 2] = a[k];
          }
          a[++k + 1] = a1;

          while (a2 < a[--k]) {
            a[k + 1] = a[k];
          }
          a[k + 1] = a2;
        }
        long last = a[right];

        while (last < a[--right]) {
          a[right + 1] = a[right];
        }
        a[right + 1] = last;
      }
      return;
    }

    // Inexpensive approximation of length / 7
    int seventh = (length >> 3) + (length >> 6) + 1;

    /*
     * Sort five evenly spaced elements around (and including) the
     * center element in the range. These elements will be used for
     * pivot selection as described below. The choice for spacing
     * these elements was empirically determined to work well on
     * a wide variety of inputs.
     */
    int e3 = (left + right) >>> 1; // The midpoint
    int e2 = e3 - seventh;
    int e1 = e2 - seventh;
    int e4 = e3 + seventh;
    int e5 = e4 + seventh;

    // Sort these elements using insertion sort
    if (a[e2] < a[e1]) { long t = a[e2]; a[e2] = a[e1]; a[e1] = t; }

    if (a[e3] < a[e2]) { long t = a[e3]; a[e3] = a[e2]; a[e2] = t;
      if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
    }
    if (a[e4] < a[e3]) { long t = a[e4]; a[e4] = a[e3]; a[e3] = t;
      if (t < a[e2]) { a[e3] = a[e2]; a[e2] = t;
        if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
      }
    }
    if (a[e5] < a[e4]) { long t = a[e5]; a[e5] = a[e4]; a[e4] = t;
      if (t < a[e3]) { a[e4] = a[e3]; a[e3] = t;
        if (t < a[e2]) { a[e3] = a[e2]; a[e2] = t;
          if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
        }
      }
    }

    // Pointers
    int less  = left;  // The index of the first element of center part
    int great = right; // The index before the first element of right part

    if (a[e1] != a[e2] && a[e2] != a[e3] && a[e3] != a[e4] && a[e4] != a[e5]) {
      /*
       * Use the second and fourth of the five sorted elements as pivots.
       * These values are inexpensive approximations of the first and
       * second terciles of the array. Note that pivot1 <= pivot2.
       */
      long pivot1 = a[e2];
      long pivot2 = a[e4];

      /*
       * The first and the last elements to be sorted are moved to the
       * locations formerly occupied by the pivots. When partitioning
       * is complete, the pivots are swapped back into their final
       * positions, and excluded from subsequent sorting.
       */
      a[e2] = a[left];
      a[e4] = a[right];

      /*
       * Skip elements, which are less or greater than pivot values.
       */
      while (a[++less] < pivot1);
      while (a[--great] > pivot2);

      /*
       * Partitioning:
       *
       *   left part           center part                   right part
       * +--------------------------------------------------------------+
       * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
       * +--------------------------------------------------------------+
       *               ^                          ^       ^
       *               |                          |       |
       *              less                        k     great
       *
       * Invariants:
       *
       *              all in (left, less)   < pivot1
       *    pivot1 <= all in [less, k)     <= pivot2
       *              all in (great, right) > pivot2
       *
       * Pointer k is the first index of ?-part.
       */
      outer:
      for (int k = less - 1; ++k <= great; ) {
        long ak = a[k];
        if (ak < pivot1) { // Move a[k] to left part
          a[k] = a[less];
          /*
           * Here and below we use "a[i] = b; i++;" instead
           * of "a[i++] = b;" due to performance issue.
           */
          a[less] = ak;
          ++less;
        } else if (ak > pivot2) { // Move a[k] to right part
          while (a[great] > pivot2) {
            if (great-- == k) {
              break outer;
            }
          }
          if (a[great] < pivot1) { // a[great] <= pivot2
            a[k] = a[less];
            a[less] = a[great];
            ++less;
          } else { // pivot1 <= a[great] <= pivot2
            a[k] = a[great];
          }
          /*
           * Here and below we use "a[i] = b; i--;" instead
           * of "a[i--] = b;" due to performance issue.
           */
          a[great] = ak;
          --great;
        }
      }

      // Swap pivots into their final positions
      a[left]  = a[less  - 1]; a[less  - 1] = pivot1;
      a[right] = a[great + 1]; a[great + 1] = pivot2;

      // Sort left and right parts recursively, excluding known pivots
      quickSort(a, left, less - 2, leftmost);
      quickSort(a, great + 2, right, false);

      /*
       * If center part is too large (comprises > 4/7 of the array),
       * swap internal pivot values to ends.
       */
      if (less < e1 && e5 < great) {
        /*
         * Skip elements, which are equal to pivot values.
         */
        while (a[less] == pivot1) {
          ++less;
        }

        while (a[great] == pivot2) {
          --great;
        }

        /*
         * Partitioning:
         *
         *   left part         center part                  right part
         * +----------------------------------------------------------+
         * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
         * +----------------------------------------------------------+
         *              ^                        ^       ^
         *              |                        |       |
         *             less                      k     great
         *
         * Invariants:
         *
         *              all in (*,  less) == pivot1
         *     pivot1 < all in [less,  k)  < pivot2
         *              all in (great, *) == pivot2
         *
         * Pointer k is the first index of ?-part.
         */
        outer:
        for (int k = less - 1; ++k <= great; ) {
          long ak = a[k];
          if (ak == pivot1) { // Move a[k] to left part
            a[k] = a[less];
            a[less] = ak;
            ++less;
          } else if (ak == pivot2) { // Move a[k] to right part
            while (a[great] == pivot2) {
              if (great-- == k) {
                break outer;
              }
            }
            if (a[great] == pivot1) { // a[great] < pivot2
              a[k] = a[less];
              /*
               * Even though a[great] equals to pivot1, the
               * assignment a[less] = pivot1 may be incorrect,
               * if a[great] and pivot1 are floating-point zeros
               * of different signs. Therefore in float and
               * double sorting methods we have to use more
               * accurate assignment a[less] = a[great].
               */
              a[less] = pivot1;
              ++less;
            } else { // pivot1 < a[great] < pivot2
              a[k] = a[great];
            }
            a[great] = ak;
            --great;
          }
        }
      }

      // Sort center part recursively
      quickSort(a, less, great, false);

    } else { // Partitioning with one pivot
      /*
       * Use the third of the five sorted elements as pivot.
       * This value is inexpensive approximation of the median.
       */
      long pivot = a[e3];

      /*
       * Partitioning degenerates to the traditional 3-way
       * (or "Dutch National Flag") schema:
       *
       *   left part    center part              right part
       * +-------------------------------------------------+
       * |  < pivot  |   == pivot   |     ?    |  > pivot  |
       * +-------------------------------------------------+
       *              ^              ^        ^
       *              |              |        |
       *             less            k      great
       *
       * Invariants:
       *
       *   all in (left, less)   < pivot
       *   all in [less, k)     == pivot
       *   all in (great, right) > pivot
       *
       * Pointer k is the first index of ?-part.
       */
      for (int k = less; k <= great; ++k) {
        if (a[k] == pivot) {
          continue;
        }
        long ak = a[k];
        if (ak < pivot) { // Move a[k] to left part
          a[k] = a[less];
          a[less] = ak;
          ++less;
        } else { // a[k] > pivot - Move a[k] to right part
          while (a[great] > pivot) {
            --great;
          }
          if (a[great] < pivot) { // a[great] <= pivot
            a[k] = a[less];
            a[less] = a[great];
            ++less;
          } else { // a[great] == pivot
            /*
             * Even though a[great] equals to pivot, the
             * assignment a[k] = pivot may be incorrect,
             * if a[great] and pivot are floating-point
             * zeros of different signs. Therefore in float
             * and double sorting methods we have to use
             * more accurate assignment a[k] = a[great].
             */
            a[k] = pivot;
          }
          a[great] = ak;
          --great;
        }
      }

      /*
       * Sort left and right parts recursively.
       * All elements from center part are equal
       * and, therefore, already sorted.
       */
      quickSort(a, left, less - 1, leftmost);
      quickSort(a, great + 1, right, false);
    }
  }


  public static void mergeSort(long[] a, int left, int right,
                               long[] work, int[] run) {

    int workBase = 0;
    int count = run.length - 1;

    if (run[count] == right++) { // The last run contains one element
      run[++count] = right;
    } else if (count == 1) { // The array is already sorted
      return;
    }

    // Determine alternation base for merge
    byte odd = 0;
    for (int n = 1; (n <<= 1) < count; odd ^= 1);

    // Use or create temporary array b for merging
    long[] b;                 // temp array; alternates with a
    int ao, bo;              // array offsets from 'left'
    int blen = right - left; // space needed for b
    if (odd == 0) {
      System.arraycopy(a, left, work, workBase, blen);
      b = a;
      bo = 0;
      a = work;
      ao = workBase - left;
    } else {
      b = work;
      ao = 0;
      bo = workBase - left;
    }

    // Merging
    for (int last; count > 1; count = last) {
      for (int k = (last = 0) + 2; k <= count; k += 2) {
        int hi = run[k], mi = run[k - 1];
        for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
          if (q >= hi || p < mi && a[p + ao] <= a[q + ao]) {
            b[i + bo] = a[p++ + ao];
          } else {
            b[i + bo] = a[q++ + ao];
          }
        }
        run[++last] = hi;
      }
      if ((count & 1) != 0) {
        for (int i = right, lo = run[count - 1]; --i >= lo;
             b[i + bo] = a[i + ao]
        );
        run[++last] = right;
      }
      long[] t = a; a = b; b = t;
      int o = ao; ao = bo; bo = o;
    }
  }

  private static final int MAX_RUN_COUNT = 67;

  private static final int MAX_RUN_LENGTH = 33;



  public static void sort(long[] a, int left, int right,
                   long[] work, int workBase, int workLen) {

    /*
     * Index run[i] is the start of i-th run
     * (ascending or descending sequence).
     */
    int[] run = new int[MAX_RUN_COUNT + 1];
    int count = 0; run[0] = left;

    // Check if the array is nearly sorted
    for (int k = left; k < right; run[count] = k) {
      if (a[k] < a[k + 1]) { // ascending
        while (++k <= right && a[k - 1] <= a[k]);
      } else if (a[k] > a[k + 1]) { // descending
        while (++k <= right && a[k - 1] >= a[k]);
        for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
          long t = a[lo]; a[lo] = a[hi]; a[hi] = t;
        }
      } else { // equal
        for (int m = MAX_RUN_LENGTH; ++k <= right && a[k - 1] == a[k]; ) {
          if (--m == 0) {
            System.out.println("error1");
            return;
          }
        }
      }

      /*
       * The array is not highly structured,
       * use Quicksort instead of merge sort.
       */
      if (++count == MAX_RUN_COUNT) {
        System.out.println("error2");
        System.out.println("left is " + left);
        System.out.println("right is " + right);
        for (int i = left; i < right; i++) {
          System.out.println(a[i] + "      ----------- " + i);
        }
        System.exit(1);
        return;
      }
    }




    // Check special cases
    // Implementation note: variable "right" is increased by 1.
    if (run[count] == right++) { // The last run contains one element
      run[++count] = right;
    } else if (count == 1) { // The array is already sorted
      return;
    }

    // Determine alternation base for merge
    byte odd = 0;
    for (int n = 1; (n <<= 1) < count; odd ^= 1);

    // Use or create temporary array b for merging
    long[] b;                 // temp array; alternates with a
    int ao, bo;              // array offsets from 'left'
    int blen = right - left; // space needed for b
    if (work == null || workLen < blen || workBase + blen > work.length) {
      work = new long[blen];
      workBase = 0;
    }
    if (odd == 0) {
      System.arraycopy(a, left, work, workBase, blen);
      b = a;
      bo = 0;
      a = work;
      ao = workBase - left;
    } else {
      b = work;
      ao = 0;
      bo = workBase - left;
    }


    // Merging
    for (int last; count > 1; count = last) {
      for (int k = (last = 0) + 2; k <= count; k += 2) {
        int hi = run[k], mi = run[k - 1];
        for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
          if (q >= hi || p < mi && a[p + ao] <= a[q + ao]) {
            b[i + bo] = a[p++ + ao];
          } else {
            b[i + bo] = a[q++ + ao];
          }
        }
        run[++last] = hi;
      }
      if ((count & 1) != 0) {
        for (int i = right, lo = run[count - 1]; --i >= lo;
             b[i + bo] = a[i + ao]
        );
        run[++last] = right;
      }
      long[] t = a; a = b; b = t;
      int o = ao; ao = bo; bo = o;
    }
  }


  /**
   * 交换元素通用处理
   *
   * @param arr
   * @param a
   * @param b
   */
  private static void swap(long[] arr, int a, int b) {
    long temp = arr[a];
    arr[a] = arr[b];
    arr[b] = temp;
  }

  private static int partition(long[] arr, int l, int r) {

    // 随机在arr[l...r]的范围中, 选择一个数值作为标定点pivot
//    swap(arr, l, (int) (Math.random() * (r - l + 1)) + l);

    long v = arr[l];

    int j = l; // arr[l+1...j] < v ; arr[j+1...i) > v
    for (int i = l + 1; i <= r; i++) {
      if (arr[i] < v) {
        j++;
        swap(arr, j, i);
      }
    }
    swap(arr, l, j);

    return j;
  }

  // 求出nums[l...r]范围里第k小的数
  public static long solve(long[] nums, int l, int r, int k) {
    if (l == r) {
      return nums[l];
    }
    int p = partition(nums, l, r);

    if (k == p) {
      return nums[p];
    } else if (k < p) {
      return solve(nums, l, p - 1, k);
    } else {
      return solve(nums, p + 1, r, k);
    }
  }

//  public static long solve(long[] nums, int k) {
//    return solve(nums, 0, nums.length - 1, k);
//  }

  public static long quickSelect(long[] nums, int start, int end, int k) {
    if (start == end) {
      return nums[start];
    }
    int left = start;
    int right = end;
    long pivot = nums[(start + end) / 2];
    while (left <= right) {
      while (left <= right && nums[left] > pivot) {
        left++;
      }
      while (left <= right && nums[right] < pivot) {
        right--;
      }
      if (left <= right) {
        long temp = nums[left];
        nums[left] = nums[right];
        nums[right] = temp;
        left++;
        right--;
      }
    }
    if (start + k - 1 <= right) {
      return quickSelect(nums, start, right, k);
    }
    if (start + k - 1 >= left) {
      return quickSelect(nums, left, end, k - (left - start));
    }
    return nums[right + 1];
  }
}
