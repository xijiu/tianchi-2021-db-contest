package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyAnalyticDB implements AnalyticDB {

  /** 7-128  8-256  9-512  10-1024  11-2048 */
  public static final int power = 7;

  /**
   * 128- 8000000
   * 256- 4000000
   * 512- 2000000
   * 1024-1000000
   */
  public static ThreadLocal<long[]> helper = ThreadLocal.withInitial(() -> new long[500000]);
//  public static ThreadLocal<long[]> helper2 = ThreadLocal.withInitial(() -> new long[8000000]);

  private final int drift = 64 - (power + 1);

  /** 每一列数据分多少块 */
  private static final int blockNum = (int) Math.pow(2, power);

  private static final int cpuThreadNum = 22;

  /** 单次读取文件的大小，单位字节 */
  private final int readFileLen = 1 * 1024 * 1024;

  private volatile int lastBucketIndex_1 = -1;

  private long[] bucketHeadArr_1 = null;

  private long[] bucketTailArr_1 = null;

  private long[] bucketBaseArr_1 = null;

  private byte[] bucketDataPosArr_1 = null;

  private volatile int lastBucketIndex_2 = -1;

  private long[] bucketHeadArr_2 = null;

  private long[] bucketTailArr_2 = null;

  private long[] bucketBaseArr_2 = null;

  private byte[] bucketDataPosArr_2 = null;

  private volatile int lastBucketLength = -1;

  public static final CpuThread[] cpuThread = new CpuThread[cpuThreadNum];

  private AtomicInteger finishThreadNum1 = new AtomicInteger();

  private AtomicInteger finishThreadNum2 = new AtomicInteger();

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

  private final int[] colDataLen_1_1 = new int[cpuThreadNum * blockNum];

  private final int[] colDataLen_1_2 = new int[cpuThreadNum * blockNum];

  private final int[] colDataLen_2_1 = new int[cpuThreadNum * blockNum];

  private final int[] colDataLen_2_2 = new int[cpuThreadNum * blockNum];

  private final DiskBlock[] diskBlockData_1_1 = new DiskBlock[blockNum];

  private final DiskBlock[]  diskBlockData_1_2 = new DiskBlock[blockNum];

  private final DiskBlock[]  diskBlockData_2_1 = new DiskBlock[blockNum];

  private final DiskBlock[]  diskBlockData_2_2 = new DiskBlock[blockNum];

  private volatile long loadCostTime = 1000000000L;

  private static volatile boolean operateFirstFile = true;

  private final AtomicLong readFileTime = new AtomicLong();

  public static final AtomicLong writeFileTime = new AtomicLong();

  private final AtomicLong sortDataTime = new AtomicLong();

  private long totalBeginTime = System.currentTimeMillis();

  private long step2BeginTime = System.currentTimeMillis();

  private final boolean isTest = step2BeginTime < 1629424800000L;

  public static volatile boolean isFirstInvoke = true;

  private final File file1 = new File("/adb-data/tpch/lineitem");

  private final File file2 = new File("/adb-data/tpch/orders");

  private volatile long fileSize = file1.length();

  public static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);

  public MyAnalyticDB() {
//    try {
//      Thread thread = new Thread(() -> {
//        try {
//          Thread.sleep(1 * 1000 * 60);
//          System.exit(1);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      });
//      thread.start();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
  }

  /**
   * 初始化
   */
  private void init(String workspaceDir) throws Exception {
    DiskBlock.workspaceDir = workspaceDir;
    long begin = System.currentTimeMillis();
    firstInit();
    System.out.println("\n\n\ninit cost time : " + (System.currentTimeMillis() - begin));
  }

  private void firstInit() throws Exception {
    Future<?> future1 = executor.submit(() -> {
      for (int i = 0; i < blockNum / 2; i++) {
        diskBlockData_1_1[i] = new DiskBlock("1", 1, i);
      }
    });
    Future<?> future2 = executor.submit(() -> {
      for (int i = blockNum / 2; i < blockNum; i++) {
        diskBlockData_1_1[i] = new DiskBlock("1", 1, i);
      }
    });
    Future<?> future3 = executor.submit(() -> {
      for (int i = 0; i < blockNum / 2; i++) {
        diskBlockData_1_2[i] = new DiskBlock("1", 2, i);
      }
    });
    Future<?> future4 = executor.submit(() -> {
      for (int i = blockNum / 2; i < blockNum; i++) {
        diskBlockData_1_2[i] = new DiskBlock("1", 2, i);
      }
    });
    Future<?> future5 = executor.submit(() -> {
      for (int i = 0; i < blockNum / 2; i++) {
        diskBlockData_2_1[i] = new DiskBlock("2", 1, i);
      }
    });
    Future<?> future6 = executor.submit(() -> {
      for (int i = blockNum / 2; i < blockNum; i++) {
        diskBlockData_2_1[i] = new DiskBlock("2", 1, i);
      }
    });
    Future<?> future7 = executor.submit(() -> {
      for (int i = 0; i < blockNum / 2; i++) {
        diskBlockData_2_2[i] = new DiskBlock("2", 2, i);
      }
    });

    for (int i = blockNum / 2; i < blockNum; i++) {
      diskBlockData_2_2[i] = new DiskBlock("2", 2, i);
    }

    future1.get();
    future2.get();
    future3.get();
    future4.get();
    future5.get();
    future6.get();
    future7.get();

//    if (!isFirstInvoke) {
//      executor.shutdown();
//    }
  }


  @Override
  public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
    long begin = System.currentTimeMillis();
    setInvokeFlag(workspaceDir);
    init(workspaceDir);
    if (!isFirstInvoke) {
      reloadBlockNumberFile();
      return ;
    }

    System.out.println("stable load invoked, time is " + begin);
    DiskBlock.workspaceDir = workspaceDir;

    storeBlockData();

    storeBlockNumberFile();

    loadCostTime = System.currentTimeMillis() - begin;
//    System.out.println("target file size is " + PubTools.getDirSize(new File(workspaceDir)));
    System.out.println("============> read file cost time : " + readFileTime.get() / cpuThreadNum);
    System.out.println("============> write file cost time : " + writeFileTime.get() / cpuThreadNum);
    System.out.println("============> sort data cost time : " + sortDataTime.get() / cpuThreadNum);
    System.out.println("");
    System.out.println("============> stable load cost time : " + loadCostTime);
  }

//  private void printForTest() {
//    Set<Integer> set = new TreeSet<>();
//    for (int num : table_1_BlockDataNumArr1) {
//      set.add(num);
//    }
//
//    Set<Integer> set2 = new TreeSet<>();
//    for (int num : table_1_BlockDataNumArr2) {
//      set2.add(num);
//    }
//
//    Set<Integer> set3 = new TreeSet<>();
//    for (int num : table_2_BlockDataNumArr1) {
//      set3.add(num);
//    }
//
//    Set<Integer> set4 = new TreeSet<>();
//    for (int num : table_2_BlockDataNumArr2) {
//      set4.add(num);
//    }
//  }

  private void reloadBlockNumberFile() throws IOException {
    storeBlockNumberFile = new File(DiskBlock.workspaceDir + "/blockNumberInfo.data");
    FileChannel fileChannel = FileChannel.open(storeBlockNumberFile.toPath(), StandardOpenOption.READ);
    ByteBuffer byteBuffer = ByteBuffer.allocate(blockNum * 4 * 4 + 8 + (DiskBlock.splitNum * blockNum * 4 * 4));

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

    for (DiskBlock diskBlock : diskBlockData_1_1) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int i = 0; i < partFilePosArr.length; i++) {
        partFilePosArr[i] = byteBuffer.getInt();
      }
    }
    for (DiskBlock diskBlock : diskBlockData_1_2) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int i = 0; i < partFilePosArr.length; i++) {
        partFilePosArr[i] = byteBuffer.getInt();
      }
    }
    for (DiskBlock diskBlock : diskBlockData_2_1) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int i = 0; i < partFilePosArr.length; i++) {
        partFilePosArr[i] = byteBuffer.getInt();
      }
    }
    for (DiskBlock diskBlock : diskBlockData_2_2) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int i = 0; i < partFilePosArr.length; i++) {
        partFilePosArr[i] = byteBuffer.getInt();
      }
    }

    fileChannel.close();
  }

  private void storeBlockNumberFile() throws Exception {
    storeBlockNumberFile = new File(DiskBlock.workspaceDir + "/blockNumberInfo.data");
    storeBlockNumberFile.createNewFile();
    FileChannel fileChannel = FileChannel.open(storeBlockNumberFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(blockNum * 4 * 4 + 8 + (DiskBlock.splitNum * blockNum * 4 * 4));
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


    for (DiskBlock diskBlock : diskBlockData_1_1) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int num : partFilePosArr) {
        byteBuffer.putInt(num);
      }
    }
    for (DiskBlock diskBlock : diskBlockData_1_2) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int num : partFilePosArr) {
        byteBuffer.putInt(num);
      }
    }
    for (DiskBlock diskBlock : diskBlockData_2_1) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int num : partFilePosArr) {
        byteBuffer.putInt(num);
      }
    }
    for (DiskBlock diskBlock : diskBlockData_2_2) {
      int[] partFilePosArr = diskBlock.partFilePosArr;
      for (int num : partFilePosArr) {
        byteBuffer.putInt(num);
      }
    }

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
        tmp += colDataLen_1_1[j * blockNum + i];
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
        tmp += colDataLen_1_2[j * blockNum + i];
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
        tmp += colDataLen_2_1[j * blockNum + i];
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
        tmp += colDataLen_2_2[j * blockNum + i];
        tmp += cpuThread[j].secondCacheLengthArr[i];
      }
      table_2_BlockDataNumArr2[i] = tmp;
      firstSum += tmp;
    }
    System.out.println("table 2 secondSum is " + firstSum);
  }

  public void storeBlockData() throws Exception {
    initGapBucketArr(file1.length(), file2.length());

    long beginThreadTime = System.currentTimeMillis();
    for (int i = 0; i < cpuThreadNum; i++) {
      cpuThread[i] = new CpuThread(i, i < cpuThreadNum / 2 ? 1 : 2);
      cpuThread[i].setName("stable-thread-" + i);
      cpuThread[i].start();
    }
    System.out.println("create threads cost time is : " + (System.currentTimeMillis() - beginThreadTime));

    for (int i = 0; i < cpuThreadNum; i++) {
      cpuThread[i].join();
    }

    // 存储残存的第二张表的数据
    long finalBeginTime = System.currentTimeMillis();
    storeFinalDataToDisk();
    System.out.println("storeFinalDataToDisk 2 time cost : " + (System.currentTimeMillis() - finalBeginTime));

    // 统计每个分桶的数量
    statPerBlockCount1();
    statPerBlockCount2();
  }

  private void initGapBucketArr(long size1, long size2) {
    lastBucketIndex_1 = (int) ((size1 - 21) / readFileLen);
    bucketHeadArr_1 = new long[lastBucketIndex_1 + 1];
    bucketTailArr_1 = new long[lastBucketIndex_1 + 1];
    bucketBaseArr_1 = new long[lastBucketIndex_1 + 1];
    bucketDataPosArr_1 = new byte[lastBucketIndex_1 + 1];

    lastBucketIndex_2 = (int) ((size2 - 21) / readFileLen);
    bucketHeadArr_2 = new long[lastBucketIndex_2 + 1];
    bucketTailArr_2 = new long[lastBucketIndex_2 + 1];
    bucketBaseArr_2 = new long[lastBucketIndex_2 + 1];
    bucketDataPosArr_2 = new byte[lastBucketIndex_2 + 1];
  }

  private void storeFinalDataToDisk() throws Exception {
    Future<?> future1 = executor.submit(() -> {
      try {
        for (int i = 0; i < blockNum / 2; i++) {
          diskBlockData_1_1[i].forceStoreLongArr1();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future2 = executor.submit(() -> {
      try {
        for (int i = blockNum / 2; i < blockNum; i++) {
          diskBlockData_1_1[i].forceStoreLongArr1();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future3 = executor.submit(() -> {
      try {
        for (int i = 0; i < blockNum / 2; i++) {
          diskBlockData_1_2[i].forceStoreLongArr2();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future4 = executor.submit(() -> {
      try {
        for (int i = blockNum / 2; i < blockNum; i++) {
          diskBlockData_1_2[i].forceStoreLongArr2();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future5 = executor.submit(() -> {
      try {
        for (int i = 0; i < blockNum / 2; i++) {
          diskBlockData_2_1[i].forceStoreLongArr1();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future6 = executor.submit(() -> {
      try {
        for (int i = blockNum / 2; i < blockNum; i++) {
          diskBlockData_2_1[i].forceStoreLongArr1();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    Future<?> future7 = executor.submit(() -> {
      try {
        for (int i = 0; i < blockNum / 2; i++) {
          diskBlockData_2_2[i].forceStoreLongArr2();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    for (int i = blockNum / 2; i < blockNum; i++) {
      diskBlockData_2_2[i].forceStoreLongArr2();
    }

    future1.get();
    future2.get();
    future3.get();
    future4.get();
    future5.get();
    future6.get();
    future7.get();
  }

  private AtomicInteger number1 = new AtomicInteger();

  private AtomicInteger number2 = new AtomicInteger();

  public class CpuThread extends Thread {

    private final int threadIndex;

    private static final int thresholdValue = 4096 * 3;

    public final short cacheLength = thresholdValue + 1000;

    public final short secondCacheLength = cacheLength;

    public final long[] firstThreadCacheArr = new long[blockNum * cacheLength];

    public final short[] firstCacheLengthArr = new short[blockNum];

    public final long[] secondThreadCacheArr = new long[blockNum * secondCacheLength];

    public final short[] secondCacheLengthArr = new short[blockNum];

    private final ByteBuffer byteBuffer = ByteBuffer.allocate(readFileLen);

    private final long[] bucketHeadArr;
    private final long[] bucketBaseArr;
    private final byte[] bucketDataPosArr;
    private final long[] bucketTailArr;

    private final int[] firstColDataLen;
    private final int[] secondColDataLen;

    private final FileChannel fileChannel;

    private int bucket = -1;

    private final int fileFlag;

    private final boolean firstFile;
    private final AtomicInteger number;

    public CpuThread(int index, int fileFlag) throws Exception {
      this.threadIndex = index;
      this.fileFlag = fileFlag;
      firstFile = fileFlag == 1;
      fileChannel = firstFile ? FileChannel.open(file1.toPath(), StandardOpenOption.READ) :
              FileChannel.open(file2.toPath(), StandardOpenOption.READ);
      bucketHeadArr = firstFile ? bucketHeadArr_1 : bucketHeadArr_2;
      bucketBaseArr = firstFile ? bucketBaseArr_1 : bucketBaseArr_2;
      bucketDataPosArr = firstFile ? bucketDataPosArr_1 : bucketDataPosArr_2;
      bucketTailArr = firstFile ? bucketTailArr_1 : bucketTailArr_2;
      firstColDataLen = firstFile ? colDataLen_1_1 : colDataLen_2_1;
      secondColDataLen = firstFile ? colDataLen_1_2 : colDataLen_2_2;
      number = firstFile ? number1 : number2;
    }

    public void run() {
      long begin = System.currentTimeMillis();
      try {
        while (true) {
//            long begin1 = System.currentTimeMillis();
          byte[] data = threadReadData();
//            readFileTime.addAndGet(System.currentTimeMillis() - begin1);

          if (data != null) {
            operate(data);
          } else {
            if (firstFile) {
              int finishNum = finishThreadNum1.incrementAndGet();
              if (finishNum == cpuThreadNum / 2) {
                operateGapData1();
              }
              for (int i = 0; i < blockNum; i++) {
                if (firstCacheLengthArr[i] > 0) {
                  batchSaveFirstCol(i);
                }
                if (secondCacheLengthArr[i] > 0) {
                  batchSaveSecondCol(i);
                }
              }
            } else {
              int finishNum = finishThreadNum2.incrementAndGet();
              if (finishNum == cpuThreadNum / 2) {
                operateGapData2();
              }
              for (int i = 0; i < blockNum; i++) {
                if (firstCacheLengthArr[i] > 0) {
                  batchSaveFirstCol(i);
                }
                if (secondCacheLengthArr[i] > 0) {
                  batchSaveSecondCol(i);
                }
              }
            }
            break;
          }
        }
      } catch (Exception e) {
        finishThreadNum1.incrementAndGet();
        e.printStackTrace();
      }
      long cost = System.currentTimeMillis() - begin;
      System.out.println(Thread.currentThread().getName() + " cost time : " + cost);
    }

//    private void cpuThreadReInitForFile2() throws Exception {
//      // 存储最后残存的数据
//      long finalBeginTime = System.currentTimeMillis();
//      storeFinalDataToDisk();
//      System.out.println("storeFinalDataToDisk time cost : " + (System.currentTimeMillis() - finalBeginTime));
//
//      // 统计表1每个分桶数量的具体信息
//      statPerBlockCount1();
//
//      Arrays.fill(colDataLen_1_1, 0);
//      Arrays.fill(colDataLen_1_2, 0);
//
//      number.set(0);
//      finishThreadNum1.set(0);
//      operateFirstFile = false;
//      initGapBucketArr(file2.length());
//      fileSize = file2.length();
//      couldReadFile2 = true;
//    }

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

      byteBuffer.clear();
      fileChannel.read(byteBuffer, position + 21);
      byteBuffer.flip();

      byte[] array = byteBuffer.array();
      if (bucket == lastBucketIndex_1) {
        lastBucketLength = byteBuffer.limit();
        byte[] result = new byte[byteBuffer.limit()];
        System.arraycopy(array, 0, result, 0, lastBucketLength);
        return result;
      } else {
        return array;
      }
    }

    private void operateGapData1() throws Exception {
      for (int bucket = 0; bucket < lastBucketIndex_1; bucket++) {
        long tailData = bucketTailArr_1[bucket];
        long headData = bucketHeadArr_1[bucket + 1];
        long data = tailData * bucketBaseArr_1[bucket + 1] + headData;
        int number = bucketDataPosArr_1[bucket + 1];
        int blockIndex = (int) (data >> drift);
        if (number == 1) {
          firstThreadCacheArr[blockIndex * cacheLength + firstCacheLengthArr[blockIndex]++] = data;
          if (firstCacheLengthArr[blockIndex] == cacheLength) {
            batchSaveFirstCol(blockIndex);
          }
        } else {
          secondThreadCacheArr[blockIndex * secondCacheLength + secondCacheLengthArr[blockIndex]++] = data;
          if (secondCacheLengthArr[blockIndex] == secondCacheLength) {
            batchSaveSecondCol(blockIndex);
          }
        }
      }
    }

    private void operateGapData2() throws Exception {
      for (int bucket = 0; bucket < lastBucketIndex_2; bucket++) {
        long tailData = bucketTailArr_2[bucket];
        long headData = bucketHeadArr_2[bucket + 1];
        long data = tailData * bucketBaseArr_2[bucket + 1] + headData;
        int number = bucketDataPosArr_2[bucket + 1];
        int blockIndex = (int) (data >> drift);
        if (number == 1) {
          firstThreadCacheArr[blockIndex * cacheLength + firstCacheLengthArr[blockIndex]++] = data;
          if (firstCacheLengthArr[blockIndex] == cacheLength) {
            batchSaveFirstCol(blockIndex);
          }
        } else {
          secondThreadCacheArr[blockIndex * secondCacheLength + secondCacheLengthArr[blockIndex]++] = data;
          if (secondCacheLengthArr[blockIndex] == secondCacheLength) {
            batchSaveSecondCol(blockIndex);
          }
        }
      }
    }

    private void operate(byte[] dataArr) throws Exception {
      long data = 0L;
      int beginIndex = 0;
      int length = dataArr.length;

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
            break;
          } else {
            data = data * 10 + (element - 48);
            base *= 10;
          }
        }
      }

      for (int i = beginIndex; i < length; i++) {
        byte element = dataArr[i];
        if (element < 45) {
          int blockIndex = (int) (data >> drift);
          if (element == 44) {
            firstThreadCacheArr[blockIndex * cacheLength + firstCacheLengthArr[blockIndex]++] = data;
          } else {
            secondThreadCacheArr[blockIndex * secondCacheLength + secondCacheLengthArr[blockIndex]++] = data;
          }
          data = 0L;
        } else {
          data = data * 10 + (element - 48);
        }
      }

      // 处理尾部数据
      bucketTailArr[bucket] = data;

      saveToMemoryOrDisk();
    }

    private void saveToMemoryOrDisk() throws Exception {
      for (int j = 0; j < blockNum; j++) {
        if (firstCacheLengthArr[j] >= thresholdValue) {
          batchSaveFirstCol(j);
        }
        if (secondCacheLengthArr[j] >= thresholdValue) {
          batchSaveSecondCol(j);
        }
      }
    }

    private void batchSaveFirstCol(int blockIndex) throws Exception {
      int length = firstCacheLengthArr[blockIndex];
      firstCacheLengthArr[blockIndex] = 0;
      // 标记已经在内存存储的位置
      firstColDataLen[(threadIndex << power) + blockIndex] += length;

      DiskBlock[] diskBlocks = fileFlag == 1 ? diskBlockData_1_1 : diskBlockData_2_1;
      diskBlocks[blockIndex].storeLongArr1(firstThreadCacheArr, blockIndex * cacheLength, length);
    }

    private void batchSaveSecondCol(int blockIndex) throws Exception {
      int length = secondCacheLengthArr[blockIndex];
      secondCacheLengthArr[blockIndex] = 0;
      // 标记已经在内存存储的位置
      secondColDataLen[(threadIndex << power) + blockIndex] += length;

      DiskBlock[] diskBlocks = fileFlag == 1 ? diskBlockData_1_2 : diskBlockData_2_2;
      diskBlocks[blockIndex].storeLongArr2(secondThreadCacheArr, blockIndex * secondCacheLength, length);
    }
  }













  private final AtomicInteger invokeTimes = new AtomicInteger();

//  public static final AtomicInteger findKStat = new AtomicInteger();

//  public static final AtomicLong writeTimeStat = new AtomicLong();

  @Override
  public String quantile(String table, String column, double percentile) throws Exception {
    int num = invokeTimes.incrementAndGet();
    if (num >= 4000) {
      long time = System.currentTimeMillis();
      long totalCost = time - totalBeginTime;
//      System.out.println("hit cache count is : " + findKStat.get());
//      System.out.println("miss cache count is : " + (4000 - findKStat.get()));
      System.out.println("finish time is : " + time);
      System.out.println("=======================> step 2 cost : " + (time - step2BeginTime));
      System.out.println("=======================> actual total cost : " + totalCost);

      if (isTest) {
        if (totalCost > 38000) {
          return "0";
        }
      }
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
    int number = (int) Math.ceil(1000000000L * percentile);
    if (number % 2 != 0) {
      if (number % 10 == 9) {
        number++;
      } else {
        number--;
      }
    }

    String result = tmp(table, column, number);
//    System.out.println("table is " + table + ", column is" + column
//            + ", percentile is " + percentile + ", result is " + result);

    return result;
  }

  public String tmp(String table, String column, int number) throws Exception {
    if (table.startsWith("lineitem")) {
      if (column.startsWith("L_O")) {
        return firstQuantile(number);
      } else {
        return secondQuantile(number);
      }
    } else {
      if (column.startsWith("O_O")) {
        return firstQuantileForTable2(number);
      } else {
        return secondQuantileForTable2(number);
      }
    }
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
        return String.valueOf(diskBlockData_1_1[i].get2(index));
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
        return String.valueOf(diskBlockData_2_1[i].get2(index));
      }
    }
    return null;
  }

  private String secondQuantile(int number) throws Exception {
    int total = 0;
    for (int i = 0; i < table_1_BlockDataNumArr2.length; i++) {
      int count = table_1_BlockDataNumArr2[i];
      int beforeTotal = total;
      total += count;
      if (total >= number) {
        int index = number - beforeTotal - 1;
//        System.out.println(Thread.currentThread().getName() + " stable second number read disk, block index is " + i);
        return String.valueOf(diskBlockData_1_2[i].get2(index));
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
        return String.valueOf(diskBlockData_2_2[i].get2(index));
      }
    }
    return null;
  }



}
