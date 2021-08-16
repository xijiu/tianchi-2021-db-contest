package com.aliyun.adb.contest;


import com.aliyun.adb.contest.utils.PubTools;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 硬盘存储块
 */
public class DiskBlock {

  public static volatile String workspaceDir = null;

  private int col;

  private int blockIndex;

  private byte bytePrev;

  private static final int thresholdValue = 4096 * 3;

  public static final short cacheLength = thresholdValue + 1000;

  public static final short secondCacheLength = (int) (cacheLength);

  private final String tableName;

  private static final int perReadSize = 7 * 1024 * 128;

  public static final int partFileSize = 4000000;

  public static final int splitNum = 16;

  public int[] partFilePosArr = new int[splitNum];

  public volatile FileChannel partFileChannel = null;

  private ByteBuffer[] dataCache1 = new ByteBuffer[splitNum];

  private long[] dataCacheLenBase1 = null;

  private long[] dataCacheLen1 = null;

  private ByteBuffer[] dataCache2 = new ByteBuffer[splitNum];

  private long[] dataCacheLenBase2 = null;

  private long[] dataCacheLen2 = null;

//  private final long address;

  private long addressHelper = 0;

  public static Unsafe unsafe = PubTools.unsafe();

  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) (blockIndex >> (MyAnalyticDB.power - 8 + 1));
    if (MyAnalyticDB.isFirstInvoke) {
//      batchWriteBuffer = ByteBuffer.allocateDirect((int) (secondCacheLength * 7 + 14));
//      address = ((DirectBuffer) batchWriteBuffer).address();


      if (col == 1) {
        dataCacheLen1 = new long[splitNum];
        dataCacheLenBase1 = new long[splitNum];
        for (int i = 0; i < splitNum; i++) {
          dataCache1[i] = ByteBuffer.allocateDirect(cacheLength * 7);
          dataCacheLen1[i] = ((DirectBuffer) dataCache1[i]).address();
          dataCacheLenBase1[i] = ((DirectBuffer) dataCache1[i]).address();
        }
      } else {
        dataCacheLen2 = new long[splitNum];
        dataCacheLenBase2 = new long[splitNum];
        for (int i = 0; i < splitNum; i++) {
          dataCache2[i] = ByteBuffer.allocateDirect(cacheLength * 7);
          dataCacheLen2[i] = ((DirectBuffer) dataCache2[i]).address();
          dataCacheLenBase2[i] = ((DirectBuffer) dataCache2[i]).address();
        }
      }
      for (int i = 0; i < splitNum; i++) {
        partFilePosArr[i] = i * partFileSize;
      }
    }
    this.initFileChannel();
  }

  public synchronized void storeLongArr1(long[] dataArr, int beginIndex, int length) throws Exception {
    int endIndex = beginIndex + length;
    for (int i = beginIndex; i < endIndex; i++) {
      long data = dataArr[i];
      // 2 part  : 36028797018963968L   >> 55
      // 4 part  : 54043195528445952L   >> 54
      // 8 part  : 63050394783186944L   >> 53
      // 16 part : 67553994410557440L   >> 52
      // 32 part : 69805794224242688L   >> 51
      int index = (int) ((data & 67553994410557440L) >> 52);

      unsafe.putLong(dataCacheLen1[index], data);
      dataCacheLen1[index] += 7;
    }

    for (int index = 0; index < splitNum; index++) {
      long size = dataCacheLen1[index] - dataCacheLenBase1[index];
      if (size >= thresholdValue * 7) {
        ByteBuffer byteBuffer = dataCache1[index];
        System.out.println("size 1 is " + size);
        byteBuffer.position((int) size);
        byteBuffer.flip();

//        long begin = System.currentTimeMillis();
        partFileChannel.write(byteBuffer, partFilePosArr[index]);
//        MyAnalyticDB.writeFileTime.addAndGet(System.currentTimeMillis() - begin);
        partFilePosArr[index] += byteBuffer.limit();
        dataCacheLen1[index] = dataCacheLenBase1[index];
      }
    }
  }

  public synchronized void storeLongArr2(long[] dataArr, int beginIndex, int length) throws Exception {
    int endIndex = beginIndex + length;
    for (int i = beginIndex; i < endIndex; i++) {
      long data = dataArr[i];
      int index = (int) ((data & 67553994410557440L) >> 52);

      unsafe.putLong(dataCacheLen2[index], data);
      dataCacheLen2[index] += 7;
    }

    for (int index = 0; index < splitNum; index++) {
      long size = dataCacheLen2[index] - dataCacheLenBase2[index];
      if (size >= thresholdValue * 7) {
        ByteBuffer byteBuffer = dataCache2[index];
        System.out.println("size 2 is " + size);
        byteBuffer.position((int) size);
        byteBuffer.flip();

//        long begin = System.currentTimeMillis();
        partFileChannel.write(byteBuffer, partFilePosArr[index]);
//        MyAnalyticDB.writeFileTime.addAndGet(System.currentTimeMillis() - begin);
        partFilePosArr[index] += byteBuffer.limit();
        dataCacheLen2[index] = dataCacheLenBase2[index];
      }
    }
  }


  public synchronized void forceStoreLongArr1() throws Exception {
    for (int i = 0; i < splitNum; i++) {
      long size = dataCacheLen1[i] - dataCacheLenBase1[i];
      if (size > 0) {
        ByteBuffer byteBuffer = dataCache1[i];
        byteBuffer.position((int) size);
        byteBuffer.flip();

//        long begin = System.currentTimeMillis();
        partFileChannel.write(byteBuffer, partFilePosArr[i]);
//        MyAnalyticDB.writeFileTime.addAndGet(System.currentTimeMillis() - begin);
        partFilePosArr[i] += byteBuffer.limit();
        dataCacheLen1[i] = dataCacheLenBase1[i];
      }
    }
  }

  public synchronized void forceStoreLongArr2() throws Exception {
    for (int i = 0; i < splitNum; i++) {
      long size = dataCacheLen2[i] - dataCacheLenBase2[i];
      if (size > 0) {
        ByteBuffer byteBuffer = dataCache2[i];
        byteBuffer.position((int) size);
        byteBuffer.flip();

//        long begin = System.currentTimeMillis();
        partFileChannel.write(byteBuffer, partFilePosArr[i]);
//        MyAnalyticDB.writeFileTime.addAndGet(System.currentTimeMillis() - begin);
        partFilePosArr[i] += byteBuffer.limit();
        dataCacheLen2[i] = dataCacheLenBase2[i];
      }
    }
  }


//  private void putToByteBuffer(int index, long[] dataArr, int length) {
//    batchWriteBuffer.clear();
//    addressHelper = address;
//    int actualLen = length % 2 == 0 ? length : (length - 1);
//    int beginIndex = index * cacheLength;
//    int endIndex = beginIndex + actualLen;
//    for (int i = beginIndex; i < endIndex; i += 2) {
//      long data1 = dataArr[i];
//      long data2 = dataArr[i + 1];
//
//      unsafe.putByte(addressHelper++, (byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60)));
//      unsafe.putInt(addressHelper, (int) (data1 << 16 >>> 32));
//      addressHelper += 4;
//      unsafe.putLong(addressHelper, data1 << 48 | (data2 << 16 >>> 16));
//      addressHelper += 8;
//    }
//
//    // 奇数
//    if (actualLen != length) {
//      if (temporaryArr[index] == 0) {
//        temporaryArr[index] = dataArr[beginIndex + length - 1];
//      } else {
//        long data1 = temporaryArr[index];
//        long data2 = dataArr[beginIndex + length - 1];
//
//        unsafe.putByte(addressHelper++, (byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60)));
//        unsafe.putInt(addressHelper, (int) (data1 << 16 >>> 32));
//        addressHelper += 4;
//        unsafe.putLong(addressHelper, data1 << 48 | (data2 << 16 >>> 16));
//        addressHelper += 8;
//
//        temporaryArr[index] = 0;
//      }
//    }
//  }











  private static ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(perReadSize));

  public long get2(int index) throws Exception {
    int lastTmpSize = 0;
    int tmpSize = 0;
    byte partNum = 0;
    byte partIndex = 0;
    int fileLen = 0;
    for (byte i = 0; i < splitNum; i++) {
      fileLen = (int) partFilePosArr[i] - (i * partFileSize);
      tmpSize += fileLen / 7;
      if (tmpSize > index) {
        partNum = i;
        partIndex = i;
        index = index - lastTmpSize;
        break;
      }
      lastTmpSize = tmpSize;
    }
    byte partNumFinal = (byte) (partNum << 4);

    int beginPos = partIndex * partFileSize;
    int endPos = partFilePosArr[partIndex];
    AtomicLong pos = new AtomicLong(beginPos);
    long[] data = MyAnalyticDB.helper.get();

    Future<?> future = MyAnalyticDB.executor.submit(() -> {
      readAndAssignValue(beginPos, endPos, pos, data, partNumFinal);
    });
//    Future<?> future2 = MyAnalyticDB.executor.submit(() -> {
//      readAndAssignValue(beginPos, endPos, pos, data, partNumFinal);
//    });
    readAndAssignValue(beginPos, endPos, pos, data, partNumFinal);
    future.get();
//    future2.get();

//    if (1 == 1) {
//      return 0;
//    }

    int totalLen = (endPos - beginPos) / 7;
    long solve = tryToQuickFindK(partNumFinal, data, totalLen, index);
    if (solve == -1) {
      solve = PubTools.solve(data, 0, totalLen - 1, index);
    }
    return (((long) bytePrev & 0xff) << 56) | solve;
//    return PubTools.quickSelect(data, 0, idx - 1, idx - index);
  }

  private void readAndAssignValue(int beginPos, int endPos, AtomicLong pos, long[] data, byte partNum) {
    ByteBuffer byteBuffer = threadLocal.get();

    int idx = 0;
    int length = 0;
    boolean over = false;
    while (true) {
      long address = ((DirectBuffer) byteBuffer).address();
      byteBuffer.clear();
      long readPos = pos.getAndAdd(perReadSize);
      idx = (int) ((readPos - beginPos) / 7);
      if (readPos >= endPos) {
        break;
      }
      int flag = 0;
      try {
        flag = partFileChannel.read(byteBuffer, readPos);
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (flag == -1) {
        break;
      }
      long currPos = readPos + perReadSize;
      length = byteBuffer.position();
      if (currPos > endPos) {
        length = (int) (perReadSize - (currPos - endPos));
        over = true;
      }
      byteBuffer.flip();
      int cycleTime = length / 7;
      for (int i = 0; i < cycleTime; i++) {
        data[idx++] = unsafe.getLong(address) & 0x00FFFFFFFFFFFFFFL;
        address += 7;
      }
      if (over) {
        break;
      }
    }
  }

  private long tryToQuickFindK(byte partNum, long[] data, int length, int index) {
    long min = ((long) partNum & 0xff) << 48;
    long max = min | 4503599627370495L;

    long solve = (long) ((index / (double)length) * (max - min)) + min;
    long leftSolve = (long) (solve - solve * 0.0003);
    long rightSolve = (long) (solve + solve * 0.0003);

//    System.out.println("min is " + min + ", max is " + max + ", solve is " + solve + ", index is " + index);

    int left = 0, middle = 0;

    int validIndex = 0;

    for (int i = 0; i < length; i++) {
      long ele = data[i];
      if (ele <= rightSolve) {
        if (ele < leftSolve) {
          left++;
        } else {
          middle++;
          data[i] = data[validIndex];
          data[validIndex++] = ele;
        }
      }
    }

    if (index > left && index < (left + middle)) {
//      System.out.println("hit, left is " + left + ", mid is " + middle + ", right is " + right + ", index is " + index);
//      MyAnalyticDB.findKStat.incrementAndGet();
      index = index - left;
      return PubTools.solve(data, 0, validIndex - 1, index);
    } else {
      return -1;
    }
  }


  public static long makeLong5(byte byteTmp, long longTmp) {
    return ((((long) byteTmp & 0xff) << 48) |
            (longTmp << 16 >>> 16));
  }

  public static long makeLong4(byte byteTmp, int intTmp, long longTmp) {
    return ((((long) byteTmp & 0xff) << 48) |
            (((long) intTmp & 0xffffffffL) << 16) |
            (((longTmp >>> 48) & 0xffff)));
  }

  public static long makeLong3(byte b6, short s, int i) {
    return ((((long) b6 & 0xff) << 48) |
            (((long) s & 0xffff) << 32) |
            (((long) i & (long)0xfffffff)));
  }

  public static long makeLong2(byte b6, byte b5, byte b4,
                               byte b3, byte b2, byte b1, byte b0){
    return ((((long)b6 & 0xff) << 48) |
            (((long)b5 & 0xff) << 40) |
            (((long)b4 & 0xff) << 32) |
            (((long)b3 & 0xff) << 24) |
            (((long)b2 & 0xff) << 16) |
            (((long)b1 & 0xff) <<  8) |
            (((long)b0 & 0xff)      ));
  }

  public static long makeLong(byte b7, byte b6, byte b5, byte b4,
                              byte b3, byte b2, byte b1, byte b0){
    return ((((long)b7       ) << 56) |
            (((long)b6 & 0xff) << 48) |
            (((long)b5 & 0xff) << 40) |
            (((long)b4 & 0xff) << 32) |
            (((long)b3 & 0xff) << 24) |
            (((long)b2 & 0xff) << 16) |
            (((long)b1 & 0xff) <<  8) |
            (((long)b0 & 0xff)      ));
  }

  private void initFileChannel() {
    try {
      File path = new File(workspaceDir + "/" + tableName);
      if (!path.exists()) {
        path.mkdirs();
      }

      File file = new File(workspaceDir + "/" + tableName + "/partFile_" + col + "_" + blockIndex + ".data");
      if (!file.exists()) {
        file.createNewFile();
      }
      partFileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }



}
