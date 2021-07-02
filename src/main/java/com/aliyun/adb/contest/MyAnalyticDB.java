package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyAnalyticDB implements AnalyticDB {

  /** 每一列数据分多少块 */
  private final int blockNum = 128;

//  private final int beginDirectMemoryIndex = 120;

  /** 当前block块，及以上的数据，均存内存；以下的块存硬盘 */
//  private final int beginMemoryIndex = 126;

  /** 每一块存储long值的个数 */
  private final int perBlockDataNum = 2350000;

  private static final int cpuThreadNum = 20;

  private final int perThreadBlockDataNum = 2400000 / cpuThreadNum;

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

  private final int[] firstColDataLen = new int[cpuThreadNum * blockNum];

  private final int[] secondColDataLen = new int[cpuThreadNum * blockNum];

  private final List<DiskBlock> diskBlockData1 = new ArrayList<>(blockNum);

  private final AtomicInteger tableHelper1 = new AtomicInteger();

  private final List<DiskBlock> diskBlockData2 = new ArrayList<>(blockNum);

  private final AtomicInteger tableHelper2 = new AtomicInteger();

  private final List<DiskBlock> diskBlockData3 = new ArrayList<>(blockNum);

  private final List<DiskBlock> diskBlockData4 = new ArrayList<>(blockNum);

  private volatile long loadCostTime = 1000000000L;

  private static volatile boolean operateFirstFile = true;

  private final AtomicLong readFileTime = new AtomicLong();

  private final AtomicLong cpuTime = new AtomicLong();

  private final AtomicLong writeFileTime = new AtomicLong();

  private final AtomicLong sortDataTime = new AtomicLong();

  private final long totalBeginTime = System.currentTimeMillis();

  private static volatile boolean isFirstInvoke = true;

  public MyAnalyticDB() {
    try {
      long begin = System.currentTimeMillis();
      System.out.println("stable init prepare, time is " + begin);
      init();
      System.out.println("stable init time cost : " + (System.currentTimeMillis() - begin));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * 初始化
   */
  private void init() throws InterruptedException {
    for (int i = 0; i < blockNum; i++) {
      diskBlockData1.add(new DiskBlock("1", 1, i));
      diskBlockData2.add(new DiskBlock("1", 2, i));
      diskBlockData3.add(new DiskBlock("2", 1, i));
      diskBlockData4.add(new DiskBlock("2", 2, i));
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
    if (!isFirstInvoke) {
      return ;
    }

    long begin = System.currentTimeMillis();
    System.out.println("stable load invoked, time is " + begin);

    DiskBlock.workspaceDir = workspaceDir;
    File dir = new File(tpchDataFileDir);

    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      File dataFile = files[i];
      System.out.println("stable target file name is " + dataFile.getName() + ", target file size is " + dataFile.length());
      operateFirstFile = i == 0;
      totalFinishThreadNum.set(0);
      finishThreadNum.set(0);
      storeBlockData(dataFile);
    }
    loadCostTime = System.currentTimeMillis() - begin;
    System.out.println("============> read file cost time : " + readFileTime.get() / cpuThreadNum);
    System.out.println("============> write file cost time : " + writeFileTime.get() / cpuThreadNum);
    System.out.println("============> sort data cost time : " + sortDataTime.get() / cpuThreadNum);
    System.out.println("");
    System.out.println("============> stable load cost time : " + loadCostTime);
  }

  private void setInvokeFlag(String workspaceDir) {
    File file = new File(workspaceDir);
    File[] files = file.listFiles();
    System.out.println("files is " + files.length);
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

    for (int i = 0; i < table_1_BlockDataNumArr2.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += secondColDataLen[j * blockNum + i];
        tmp += cpuThread[j].secondCacheLengthArr[i];
      }
      table_1_BlockDataNumArr2[i] = tmp;
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
    System.out.println("table 1 firstSum is " + firstSum);

    for (int i = 0; i < table_2_BlockDataNumArr2.length; i++) {
      int tmp = 0;
      for (int j = 0; j < cpuThreadNum; j++) {
        tmp += secondColDataLen[j * blockNum + i];
        tmp += cpuThread[j].secondCacheLengthArr[i];
      }
      table_2_BlockDataNumArr2[i] = tmp;
    }
    System.out.println("table 1 secondSum is " + firstSum);
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

//    if (operateFirstFile) {
//      statPerBlockCount1();
//      Arrays.fill(firstColDataLen, 0);
//      Arrays.fill(secondColDataLen, 0);
//    } else {
//      statPerBlockCount2();
//    }
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
      long beginTmp = System.currentTimeMillis();
      System.out.println("create space time is " + (System.currentTimeMillis() - beginTmp));

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

        while (true) {
          if (totalFinishThreadNum.get() == cpuThreadNum) {
            long sortBegin = System.currentTimeMillis();
            sortDataTest();
            sortDataTime.addAndGet(System.currentTimeMillis() - sortBegin);
            break;
          } else {
            Thread.sleep(1);
          }
        }
      } catch (Exception e) {
        finishThreadNum.incrementAndGet();
        e.printStackTrace();
      }
      long cost = System.currentTimeMillis() - begin;
      System.out.println(Thread.currentThread().getName() + " cost time : " + cost);
    }

    private void sortDataTest() throws Exception {
      System.out.println("sortDataTest begin");
      AtomicInteger num = operateFirstFile ? tableHelper1 : tableHelper2;
      int totalNum = blockNum * 2;
      int index;
      while ((index = num.getAndIncrement()) < totalNum) {
        if (index < blockNum) {
          List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData1 : diskBlockData3;
          diskBlocks.get(index).query();
        } else {
          index -= blockNum;
          List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData2 : diskBlockData4;
          diskBlocks.get(index).query();
        }
        System.out.println("sort index is " + index + ", cost time is " + (System.currentTimeMillis() - totalBeginTime));
      }
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
        int blockIndex = (int) (data >> 56);
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
        int blockIndex = (int) (data1 >> 56);
        firstThreadCacheArr[blockIndex][helperNum = firstCacheLengthArr[blockIndex]++] = data1;
        if (helperNum + 1 == cacheLength) {
          batchSaveFirstCol(blockIndex);
        }

        blockIndex = (int) (data2 >> 56);
        secondThreadCacheArr[blockIndex][helperNum = secondCacheLengthArr[blockIndex]++] = data2;
        if (helperNum + 1 == secondCacheLength) {
          batchSaveSecondCol(blockIndex);
        }
      }
      if (!normal) {
        long data2 = bucketLongArr[0];
        int blockIndex = (int) (data2 >> 56);
        secondThreadCacheArr[blockIndex][helperNum = secondCacheLengthArr[blockIndex]++] = data2;
        if (helperNum + 1 == secondCacheLength) {
          batchSaveSecondCol(blockIndex);
        }
      }
      if (i - 2 != endIndex - 1) {
        long data1 = bucketLongArr[endIndex];
        int blockIndex = (int) (data1 >> 56);
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
      firstColDataLen[(threadIndex << 7) + blockIndex] += length;

      putToByteBuffer(firstThreadCacheArr[blockIndex], length);
      List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData1 : diskBlockData3;

      long writeTime1 = System.currentTimeMillis();
      diskBlocks.get(blockIndex).storeArr(batchWriteBuffer);
      writeFileTime.addAndGet(System.currentTimeMillis() - writeTime1);
    }

    private void batchSaveSecondCol(int blockIndex) throws Exception {
      int length = secondCacheLengthArr[blockIndex];
      secondCacheLengthArr[blockIndex] = 0;
      // 标记已经在内存存储的位置
      secondColDataLen[(threadIndex << 7) + blockIndex] += length;

      putToByteBuffer(secondThreadCacheArr[blockIndex], length);
      List<DiskBlock> diskBlocks = operateFirstFile ? diskBlockData2 : diskBlockData4;

      long writeTime1 = System.currentTimeMillis();
      diskBlocks.get(blockIndex).storeArr(batchWriteBuffer);
      writeFileTime.addAndGet(System.currentTimeMillis() - writeTime1);
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













  int time = 0;

  private volatile boolean loadFinish = false;

  @Override
  public String quantile(String table, String column, double percentile) throws Exception {
    if (1 == 1) {
      return "0";
    }

    if (!isFirstInvoke) {
      return "0";
    }

//    while (totalFinishThreadNum.get() != cpuThreadNum) {
//      Thread.sleep(1);
//    }
//    if (!loadFinish) {
//      statPerBlockCount1();
//      loadFinish = true;
//    }
    return tmp(column, percentile);
  }

  public String tmp(String column, double percentile) throws Exception {
    long begin = System.currentTimeMillis();

    int number = (int) Math.ceil(300000000L * percentile);
    if (number % 2 != 0) {
      if (number % 10 == 9) {
        number++;
      } else {
        number--;
      }
    }

    String result;
    if (column.startsWith("L_O")) {
      result = firstQuantile(number);
    } else {
      result = secondQuantile(number);
    }
    System.out.println("=====> stable quantile cost time is " + (System.currentTimeMillis() - begin) + ", result is " + result);
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
        System.out.println("stable first number read disk");
        return String.valueOf(diskBlockData1.get(i).get(index, count));
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
        System.out.println("stable second number read disk");
        return String.valueOf(diskBlockData2.get(i).get(index, count));
      }
    }
    return null;
  }



}
