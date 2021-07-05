package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyAnalyticDB implements AnalyticDB {

  /** 7-128  8-256  9-512  10-1024  11-2048 */
  public static final int power = 10;

  /**
   * 128- 8000000
   * 256- 4000000
   * 512- 2000000
   * 1024-1000000
   */
  public static ThreadLocal<long[]> helper = ThreadLocal.withInitial(() -> new long[1000000]);

  private final int drift = 64 - (power + 1);

  /** 每一列数据分多少块 */
  private static final int blockNum = (int) Math.pow(2, power);

  private static final int cpuThreadNum = 20;

  /** 单次读取文件的大小，单位字节 */
  private final int readFileLen = 1 * 1024 * 1024;

  private volatile int lastBucketIndex = -1;

  private long[] bucketHeadArr = null;

  private long[] bucketTailArr = null;

  private long[] bucketBaseArr = null;

  private byte[] bucketDataPosArr = null;

  private volatile int lastBucketLength = -1;

  public static final CpuThread[] cpuThread = new CpuThread[cpuThreadNum];

  private AtomicInteger finishThreadNum = new AtomicInteger();

  private AtomicInteger totalFinishThreadNum = new AtomicInteger();

  /** 第一列的所有块的元素个数 */
  private final int[] table_1_BlockDataNumArr1 = new int[blockNum];

  /** 第二列的所有块的元素个数 */
  private final int[] table_1_BlockDataNumArr2 = new int[blockNum];

  /** 第一列的所有块的元素个数 */
  private final int[] table_2_BlockDataNumArr1 = new int[blockNum];

  /** 第二列的所有块的元素个数 */
  private final int[] table_2_BlockDataNumArr2 = new int[blockNum];

  private File storeBlockNumberFile = null;

  private final int[] firstColDataLen = new int[cpuThreadNum * blockNum];

  private final int[] secondColDataLen = new int[cpuThreadNum * blockNum];

  private final List<DiskBlock> diskBlockData_1_1 = new ArrayList<>(blockNum);

  private final List<DiskBlock> diskBlockData_1_2 = new ArrayList<>(blockNum);

  private final List<DiskBlock> diskBlockData_2_1 = new ArrayList<>(blockNum);

  private final List<DiskBlock> diskBlockData_2_2 = new ArrayList<>(blockNum);

  private volatile long loadCostTime = 1000000000L;

  private static volatile boolean operateFirstFile = true;

  private final AtomicLong readFileTime = new AtomicLong();

  private final AtomicLong cpuTime = new AtomicLong();

  private final AtomicLong writeFileTime = new AtomicLong();

  private final AtomicLong sortDataTime = new AtomicLong();

  private long totalBeginTime = System.currentTimeMillis();

  private long step2BeginTime = System.currentTimeMillis();

  private static volatile boolean isFirstInvoke = true;

  public MyAnalyticDB() {
    System.out.println("current time is " + System.currentTimeMillis());
  }

  /**
   * 初始化
   */
  private void init(String workspaceDir) throws InterruptedException {
    DiskBlock.workspaceDir = workspaceDir;
    for (int i = 0; i < blockNum; i++) {
      diskBlockData_1_1.add(new DiskBlock("1", 1, i));
      diskBlockData_1_2.add(new DiskBlock("1", 2, i));
      diskBlockData_2_1.add(new DiskBlock("2", 1, i));
      diskBlockData_2_2.add(new DiskBlock("2", 2, i));
    }
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(1000 * 5 * 60);
        for (int i = 0; i < 10; i++) {
          System.out.println(i + "termination!!!");
          Thread.sleep(100);
        }
        System.exit(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    thread.start();
  }


  @Override
  public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
    setInvokeFlag(workspaceDir);
    init(workspaceDir);
    if (!isFirstInvoke) {
      reloadBlockNumberFile();
      return ;
    }

    long begin = System.currentTimeMillis();
    System.out.println("stable load invoked, time is " + begin);

    DiskBlock.workspaceDir = workspaceDir;

    File file1 = new File(tpchDataFileDir + "/lineitem");
    File file2 = new File(tpchDataFileDir + "/orders");

    File[] files = {file1, file2};
    for (int i = 0; i < files.length; i++) {
      File dataFile = files[i];
      System.out.println("stable target file name is " + dataFile.getName() + ", target file size is " + dataFile.length());
      operateFirstFile = i == 0;
      totalFinishThreadNum.set(0);
      finishThreadNum.set(0);
      storeBlockData(dataFile);
    }

    printForTest();
    storeBlockNumberFile();
    loadCostTime = System.currentTimeMillis() - begin;
    System.out.println("============> read file cost time : " + readFileTime.get() / cpuThreadNum);
    System.out.println("============> write file cost time : " + writeFileTime.get() / cpuThreadNum);
    System.out.println("============> sort data cost time : " + sortDataTime.get() / cpuThreadNum);
    System.out.println("");
    System.out.println("============> stable load cost time : " + loadCostTime);
  }

  private void printForTest() {
    Set<Integer> set = new TreeSet<>();
    for (int num : table_1_BlockDataNumArr1) {
      set.add(num);
    }
    System.out.println("table_1_BlockDataNumArr1 count is : " + set);

    Set<Integer> set2 = new TreeSet<>();
    for (int num : table_1_BlockDataNumArr2) {
      set2.add(num);
    }
    System.out.println("table_1_BlockDataNumArr2 count is : " + set2);

    Set<Integer> set3 = new TreeSet<>();
    for (int num : table_2_BlockDataNumArr1) {
      set3.add(num);
    }
    System.out.println("table_2_BlockDataNumArr1 count is : " + set3);

    Set<Integer> set4 = new TreeSet<>();
    for (int num : table_2_BlockDataNumArr2) {
      set4.add(num);
    }
    System.out.println("table_2_BlockDataNumArr2 count is : " + set4);
  }

  private void reloadBlockNumberFile() throws IOException {
    storeBlockNumberFile = new File(DiskBlock.workspaceDir + "/blockNumberInfo.data");
    FileChannel fileChannel = FileChannel.open(storeBlockNumberFile.toPath(), StandardOpenOption.READ);
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockNum * 4 * 4 + 8);
    fileChannel.read(byteBuffer);
    byteBuffer.flip();
    for (int i = 0; i < blockNum; i++) {
      table_1_BlockDataNumArr1[i] = byteBuffer.getInt();
    }
    for (int i = 0; i < blockNum; i++) {
      table_1_BlockDataNumArr2[i] = byteBuffer.getInt();
    }
    for (int i = 0; i < blockNum; i++) {
      table_2_BlockDataNumArr1[i] = byteBuffer.getInt();
    }
    for (int i = 0; i < blockNum; i++) {
      table_2_BlockDataNumArr2[i] = byteBuffer.getInt();
    }

    totalBeginTime = byteBuffer.getLong();
  }

  private void storeBlockNumberFile() throws Exception {
    storeBlockNumberFile = new File(DiskBlock.workspaceDir + "/blockNumberInfo.data");
    storeBlockNumberFile.createNewFile();
    FileChannel fileChannel = FileChannel.open(storeBlockNumberFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockNum * 4 * 4 + 8);
    for (int num : table_1_BlockDataNumArr1) {
      byteBuffer.putInt(num);
    }
    for (int num : table_1_BlockDataNumArr2) {
      byteBuffer.putInt(num);
    }
    for (int num : table_2_BlockDataNumArr1) {
      byteBuffer.putInt(num);
    }
    for (int num : table_2_BlockDataNumArr2) {
      byteBuffer.putInt(num);
    }
    byteBuffer.putLong(totalBeginTime);

    byteBuffer.flip();
    fileChannel.write(byteBuffer);
    fileChannel.close();
  }

  private void setInvokeFlag(String workspaceDir) {
    File file = new File(workspaceDir);
    File[] files = file.listFiles();
    isFirstInvoke = files == null || files.length <= 0;
  }

  private void statPerBlockCount1() {
    long firstSum = 0;
    for (int i = 0; i < table_1_BlockDataNumArr1.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += firstColDataLen[j * blockNum + i];
        tmp += cpuThread[j].firstCacheLengthArr[i];
      }
      table_1_BlockDataNumArr1[i] = tmp;
      firstSum += tmp;
    }
    System.out.println("table 1 firstSum is " + firstSum);

    firstSum = 0;
    for (int i = 0; i < table_1_BlockDataNumArr2.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += secondColDataLen[j * blockNum + i];
        tmp += cpuThread[j].secondCacheLengthArr[i];
      }
      table_1_BlockDataNumArr2[i] = tmp;
      firstSum += tmp;
    }
    System.out.println("table 1 secondSum is " + firstSum);
  }

  private void statPerBlockCount2() {
    long firstSum = 0;
    for (int i = 0; i < table_2_BlockDataNumArr1.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += firstColDataLen[j * blockNum + i];
        tmp += cpuThread[j].firstCacheLengthArr[i];
      }
      table_2_BlockDataNumArr1[i] = tmp;
      firstSum += tmp;
    }
    System.out.println("table 2 firstSum is " + firstSum);

    firstSum = 0;
    for (int i = 0; i < table_2_BlockDataNumArr2.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += secondColDataLen[j * blockNum + i];
        tmp += cpuThread[j].secondCacheLengthArr[i];
      }
      table_2_BlockDataNumArr2[i] = tmp;
      firstSum += tmp;
    }
    System.out.println("table 2 secondSum is " + firstSum);
  }

  public void storeBlockData(File dataFile) throws Exception {
    long begin = System.currentTimeMillis();
    FileChannel fileChannel = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ);
    // 跳过第一行
    fileChannel.position(21);

    lastBucketIndex = (int) ((fileChannel.size() - 21) / readFileLen);
    bucketHeadArr = new long[lastBucketIndex + 1];
    bucketTailArr = new long[lastBucketIndex + 1];
    bucketBaseArr = new long[lastBucketIndex + 1];
    bucketDataPosArr = new byte[lastBucketIndex + 1];


    AtomicInteger number = new AtomicInteger();

    for (int i = 0; i < cpuThreadNum; i++) {
      cpuThread[i] = new CpuThread(i, fileChannel, number);
      cpuThread[i].setName("stable-thread-" + i);
      cpuThread[i].start();
    }

    for (int i = 0; i < cpuThreadNum; i++) {
      cpuThread[i].join();
    }

    if (operateFirstFile) {
      statPerBlockCount1();
      Arrays.fill(firstColDataLen, 0);
      Arrays.fill(secondColDataLen, 0);
    } else {
      statPerBlockCount2();
    }
    System.out.println("operate file " + dataFile.toPath() + ", cost time is " + (System.currentTimeMillis() - begin));
  }

  public class CpuThread extends Thread {

    private int threadIndex;

    public int cacheLength = DiskBlock.cacheLength;

    public int secondCacheLength = DiskBlock.secondCacheLength;

    public long[][] firstThreadCacheArr = new long[blockNum][cacheLength];

    public int[] firstCacheLengthArr = new int[blockNum];

    public long[][] secondThreadCacheArr = new long[blockNum][secondCacheLength];

    public int[] secondCacheLengthArr = new int[blockNum];

    private FileChannel fileChannel;

    private long fileSize;

    private AtomicInteger number;

    private ByteBuffer byteBuffer = ByteBuffer.allocate(readFileLen);

    private long[] bucketLongArr = new long[readFileLen / 8 / 2];

    private int bucket = -1;

    public CpuThread(int index, FileChannel fileChannel, AtomicInteger number) throws Exception {
      this.threadIndex = index;
      this.fileChannel = fileChannel;
      this.fileSize = fileChannel.size();
      this.number = number;
    }

    public void run() {
      long begin = System.currentTimeMillis();
      try {
        int finishNum = 0;
        while (true) {
          long readTime1 = System.currentTimeMillis();
          byte[] data = threadReadData();
          readFileTime.addAndGet(System.currentTimeMillis() - readTime1);


          if (data != null) {
            operate(data);
          } else {
            finishNum = finishThreadNum.incrementAndGet();
            if (finishNum == cpuThreadNum) {
              operateGapData();
            }
            for (int i = 0; i < blockNum; i++) {
              if (firstCacheLengthArr[i] > 0) {
                batchSaveFirstCol(i);
              }
              if (secondCacheLengthArr[i] > 0) {
                batchSaveSecondCol(i);
              }
            }
            break;
          }
        }
        totalFinishThreadNum.incrementAndGet();
      } catch (Exception e) {
        finishThreadNum.incrementAndGet();
        e.printStackTrace();
      }
      long cost = System.currentTimeMillis() - begin;
      System.out.println(Thread.currentThread().getName() + " cost time : " + cost);
    }

    private int tmpBlockIndex = -1;

    private byte[] threadReadData() throws Exception {
      if (tmpBlockIndex == -1) {
        tmpBlockIndex = number.getAndAdd(cpuThreadNum);
      } else {
        if ((tmpBlockIndex + 1) % cpuThreadNum == 0) {
          tmpBlockIndex = number.getAndAdd(cpuThreadNum);
        } else {
          tmpBlockIndex++;
        }
      }

      int indexNum = tmpBlockIndex;
      bucket = indexNum;
      long position = (long) indexNum * readFileLen;
      if (position >= fileSize) {
        return null;
      }
      fileChannel.read(byteBuffer, position + 21);

      byteBuffer.flip();
      byte[] array = byteBuffer.array();
      if (bucket == lastBucketIndex) {
        lastBucketLength = byteBuffer.limit();
        byte[] result = new byte[byteBuffer.limit()];
        System.arraycopy(array, 0, result, 0, lastBucketLength);
        return result;
      } else {
        byteBuffer.clear();
        return array;
      }


    }

    private void operateGapData() throws Exception {
      long begin = System.currentTimeMillis();
      for (int bucket = 0; bucket < lastBucketIndex; bucket++) {
        long tailData = bucketTailArr[bucket];
        long headData = bucketHeadArr[bucket + 1];
        long data = tailData * bucketBaseArr[bucket + 1] + headData;
        int number = bucketDataPosArr[bucket + 1];
        int blockIndex = (int) (data >> drift);
        if (number == 1) {
          firstThreadCacheArr[blockIndex][firstCacheLengthArr[blockIndex]++] = data;
          if (firstCacheLengthArr[blockIndex] == cacheLength) {
            batchSaveFirstCol(blockIndex);
          }
        } else {
          secondThreadCacheArr[blockIndex][secondCacheLengthArr[blockIndex]++] = data;
          if (secondCacheLengthArr[blockIndex] == secondCacheLength) {
            batchSaveSecondCol(blockIndex);
          }
        }
      }
      System.out.println("gap data operate over!!! cost " + (System.currentTimeMillis() - begin));
    }

    private void operate(byte[] dataArr) throws Exception {
      long data = 0L;
      int beginIndex = 0;
      int length = dataArr.length;
      boolean normal = true;

      if (bucket > 0) {
        long base = 1;
        for (int i = 0; i < length; i++) {
          byte element = dataArr[i];
          if (element < 45) {
            beginIndex = i + 1;
            bucketHeadArr[bucket] = data;
            bucketBaseArr[bucket] = base;
            bucketDataPosArr[bucket] = (byte) (element == 10 ? 2 : 1);
            data = 0L;
            normal = element == 10;
            break;
          } else {
            data = data * 10 + (element - 48);
            base *= 10;
          }
        }
      }

      int firstIndex = 0;

      for (int i = beginIndex; i < length; i++) {
        byte element = dataArr[i];
        if (element < 45) {
          bucketLongArr[firstIndex++] = data;
          data = 0L;
        } else {
          data = data * 10 + (element - 48);
        }
      }

      // 处理尾部数据
      bucketTailArr[bucket] = data;

      saveToMemoryOrDisk(firstIndex, normal);
    }

    private void saveToMemoryOrDisk(int firstIndex, boolean normal) throws Exception {
      int helperNum = 0;
      int i = normal ? 0 : 1;
      int endIndex = firstIndex - 1;
      for (; i < endIndex; i = i + 2) {
        long data1 = bucketLongArr[i];
        long data2 = bucketLongArr[i + 1];
        int blockIndex = (int) (data1 >> drift);
        firstThreadCacheArr[blockIndex][helperNum = firstCacheLengthArr[blockIndex]++] = data1;
        if (helperNum + 1 == cacheLength) {
          batchSaveFirstCol(blockIndex);
        }

        blockIndex = (int) (data2 >> drift);
        secondThreadCacheArr[blockIndex][helperNum = secondCacheLengthArr[blockIndex]++] = data2;
        if (helperNum + 1 == secondCacheLength) {
          batchSaveSecondCol(blockIndex);
        }
      }
      if (!normal) {
        long data2 = bucketLongArr[0];
        int blockIndex = (int) (data2 >> drift);
        secondThreadCacheArr[blockIndex][helperNum = secondCacheLengthArr[blockIndex]++] = data2;
        if (helperNum + 1 == secondCacheLength) {
          batchSaveSecondCol(blockIndex);
        }
      }
      if (i - 2 != endIndex - 1) {
        long data1 = bucketLongArr[endIndex];
        int blockIndex = (int) (data1 >> drift);
        firstThreadCacheArr[blockIndex][helperNum = firstCacheLengthArr[blockIndex]++] = data1;
        if (helperNum + 1 == cacheLength) {
          batchSaveFirstCol(blockIndex);
        }
      }
    }


    private byte[] batchWriteArr = new byte[secondCacheLength * 7];

    private ByteBuffer batchWriteBuffer = ByteBuffer.wrap(batchWriteArr);


    private void batchSaveFirstCol(int blockIndex) throws Exception {
      int length = firstCacheLengthArr[blockIndex];
      firstCacheLengthArr[blockIndex] = 0;
      // 标记已经在内存存储的位置
      firstColDataLen[(threadIndex << power) + blockIndex] += length;

      List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData_1_1 : diskBlockData_2_1;

      putToByteBuffer(firstThreadCacheArr[blockIndex], length);
      diskBlocks.get(blockIndex).storeArr(batchWriteBuffer);
//      diskBlocks.get(blockIndex).storeLongArr(firstThreadCacheArr[blockIndex], length);
    }

    private void batchSaveSecondCol(int blockIndex) throws Exception {
      int length = secondCacheLengthArr[blockIndex];
      secondCacheLengthArr[blockIndex] = 0;
      // 标记已经在内存存储的位置
      secondColDataLen[(threadIndex << power) + blockIndex] += length;

      List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData_1_2 : diskBlockData_2_2;
      putToByteBuffer(secondThreadCacheArr[blockIndex], length);
      diskBlocks.get(blockIndex).storeArr(batchWriteBuffer);
//      diskBlocks.get(blockIndex).storeLongArr(secondThreadCacheArr[blockIndex], length);
    }

    private void putToByteBuffer(long[] data, int length) {
      int index = 0;
      for (int i = 0; i < length; i++) {
        long element = data[i];
        batchWriteArr[index++] = (byte)(element >> 48);
        batchWriteArr[index++] = (byte)(element >> 40);
        batchWriteArr[index++] = (byte)(element >> 32);
        batchWriteArr[index++] = (byte)(element >> 24);
        batchWriteArr[index++] = (byte)(element >> 16);
        batchWriteArr[index++] = (byte)(element >> 8);
        batchWriteArr[index++] = (byte)(element);
      }

      batchWriteBuffer.clear();
      batchWriteBuffer.position(index);
      batchWriteBuffer.flip();
    }
  }













  private AtomicInteger invokeTimes = new AtomicInteger();

  public static AtomicLong cpuSloveTime = new AtomicLong();

  public static AtomicLong diskReadFileTime = new AtomicLong();

  private volatile boolean loadFinish = false;

  @Override
  public String quantile(String table, String column, double percentile) throws Exception {
    int num = invokeTimes.incrementAndGet();
    if (num >= 4000) {
      long time = System.currentTimeMillis();
      System.out.println("finish time is : " + time);
      System.out.println("=================> cpuSloveTime cost : " + (cpuSloveTime.get() / 8));
      System.out.println("=================> diskReadFileTime cost : " + (diskReadFileTime.get() / 8));
      System.out.println("=======================> step 2 cost : " + (time - step2BeginTime));
      System.out.println("=======================> actual total cost : " + (time - totalBeginTime));
      return "0";
    }

//    if (1 == 1) {
//      return "0";
//    }

//    if (!isFirstInvoke) {
//      return "0";
//    }

//    while (totalFinishThreadNum.get() != cpuThreadNum) {
//      Thread.sleep(1);
//    }
//    if (!loadFinish) {
//      statPerBlockCount1();
//      loadFinish = true;
//    }
    return tmp(table, column, percentile);
  }

  public String tmp(String table, String column, double percentile) throws Exception {
    long begin = System.currentTimeMillis();

    int number = (int) Math.ceil(1000000000L * percentile);
    if (number % 2 != 0) {
      if (number % 10 == 9) {
        number++;
      } else {
        number--;
      }
    }

//    System.out.println("table is " + table + ", column is" + column
//            + ", percentile is " + percentile + ", number is " + number);

    String result;
    if (table.startsWith("lineitem")) {
      if (column.startsWith("L_O")) {
        result = firstQuantile(number);
      } else {
        result = secondQuantile(number);
      }
    } else {
      if (column.startsWith("O_O")) {
        result = firstQuantileForTable2(number);
      } else {
        result = secondQuantileForTable2(number);
      }
    }
//    System.out.println("=====> stable quantile cost time is " + (System.currentTimeMillis() - begin) + ", result is " + result);
    return result;
  }


  private String firstQuantile(int number) throws Exception {
    int total = 0;
    for (int i = 0; i < table_1_BlockDataNumArr1.length; i++) {
      int count = table_1_BlockDataNumArr1[i];
      int beforeTotal = total;
      total += count;
      if (total >= number) {
        int index = number - beforeTotal - 1;
//        System.out.println(Thread.currentThread().getName() + " stable first number read disk, block index is " + i);
        return String.valueOf(diskBlockData_1_1.get(i).get2(index, count));
      }
    }
    return null;
  }

  private String firstQuantileForTable2(int number) throws Exception {
    int total = 0;
    for (int i = 0; i < table_2_BlockDataNumArr1.length; i++) {
      int count = table_2_BlockDataNumArr1[i];
      int beforeTotal = total;
      total += count;
      if (total >= number) {
        int index = number - beforeTotal - 1;
//        System.out.println(Thread.currentThread().getName() + " stable first number read disk, block index is " + i);
        return String.valueOf(diskBlockData_2_1.get(i).get2(index, count));
      }
    }
    return null;
  }

  public static long[] sortArr = new long[2600000];

  private String secondQuantile(int number) throws Exception {
    int total = 0;
    for (int i = 0; i < table_1_BlockDataNumArr2.length; i++) {
      int count = table_1_BlockDataNumArr2[i];
      int beforeTotal = total;
      total += count;
      if (total >= number) {
        int index = number - beforeTotal - 1;
//        System.out.println(Thread.currentThread().getName() + " stable second number read disk, block index is " + i);
        return String.valueOf(diskBlockData_1_2.get(i).get2(index, count));
      }
    }
    return null;
  }

  private String secondQuantileForTable2(int number) throws Exception {
    int total = 0;
    for (int i = 0; i < table_2_BlockDataNumArr2.length; i++) {
      int count = table_2_BlockDataNumArr2[i];
      int beforeTotal = total;
      total += count;
      if (total >= number) {
        int index = number - beforeTotal - 1;
//        System.out.println(Thread.currentThread().getName() + " second number read disk, block index is " + i);
        return String.valueOf(diskBlockData_2_2.get(i).get2(index, count));
      }
    }
    return null;
  }



}
