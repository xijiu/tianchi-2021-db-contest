package com.aliyun.adb.contest;


import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 硬盘存储块
 */
public class DiskBlock {

  public static volatile String workspaceDir = null;

  private int col;

  private int blockIndex;

  private byte bytePrev;

  private File file = null;

  private volatile FileChannel fileChannel = null;

  public static final short cacheLength = 4096 * 3;

  public static final short secondCacheLength = (int) (cacheLength);

  private final String tableName;

  private static final int perReadSize = 7 * 1024 * 128;

//  private static final int concurrentQueryThreadNum = 2;

//  private final long[] beginReadPosArr = new long[concurrentQueryThreadNum];
//
//  private final int[] readSizeArr = new int[concurrentQueryThreadNum];
//
//  private static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);

  private static final int splitNum = 16;

  private final FileChannel[] partFileChannels = new FileChannel[splitNum];

  private long[][] dataCache1 = new long[splitNum][cacheLength];

  private short[] dataCacheLen1 = new short[splitNum];

  private long[][] dataCache2 = new long[splitNum][secondCacheLength];

  private short[] dataCacheLen2 = new short[splitNum];

  private byte[] batchWriteArr = new byte[secondCacheLength * 7];

  private ByteBuffer batchWriteBuffer = ByteBuffer.wrap(batchWriteArr);


  public DiskBlock(String tableName, int col, int blockIndex) {
    this.tableName = tableName;
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) (blockIndex >> (MyAnalyticDB.power - 8 + 1));
    this.initFileChannel();
  }

  public void storeArr(ByteBuffer byteBuffer) throws Exception {
    fileChannel.write(byteBuffer);
  }

  public synchronized void storeLongArr1(long[] dataArr, int length) throws Exception {
    for (int i = 0; i < length; i++) {
      long data = dataArr[i];
      // 8 part  : 63050394783186944L   >> 53
      // 16 part : 67553994410557440L   >> 52
      int index = (int) ((data & 67553994410557440L) >> 52);
      short pos = dataCacheLen1[index]++;
      dataCache1[index][pos] = data;
      if (pos + 1 == cacheLength) {
        putToByteBuffer(dataCache1[index], cacheLength);
        partFileChannels[index].write(batchWriteBuffer);
        dataCacheLen1[index] = 0;
      }
    }
  }

  public synchronized void storeLongArr2(long[] dataArr, int length) throws Exception {
    for (int i = 0; i < length; i++) {
      long data = dataArr[i];
      int index = (int) ((data & 67553994410557440L) >> 52);
      short pos = dataCacheLen2[index]++;
      dataCache2[index][pos] = data;
      if (pos + 1 == secondCacheLength) {
        putToByteBuffer(dataCache2[index], secondCacheLength);
        partFileChannels[index].write(batchWriteBuffer);
        dataCacheLen2[index] = 0;
      }
    }
  }


  public synchronized void forceStoreLongArr1() throws Exception {
    for (int i = 0; i < dataCacheLen1.length; i++) {
      int len = dataCacheLen1[i];
      if (len > 0) {
        putToByteBuffer(dataCache1[i], len);
        partFileChannels[i].write(batchWriteBuffer);
        dataCacheLen1[i] = 0;
      }
    }
  }

  public synchronized void forceStoreLongArr2() throws Exception {
    for (int i = 0; i < dataCacheLen2.length; i++) {
      int len = dataCacheLen2[i];
      if (len > 0) {
        putToByteBuffer(dataCache2[i], len);
        partFileChannels[i].write(batchWriteBuffer);
        dataCacheLen2[i] = 0;
      }
    }
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


//  public long get2(int index, int count) throws Exception {
//    long[] data = MyAnalyticDB.helper.get();
//    fillThreadReadFileInfo();
//
//    Future[] futures = new Future[1];
//    for (int i = 0; i < futures.length; i++) {
//      int finalI = i;
//      futures[i] = executor.submit(() -> {
//        try {
//          ByteBuffer byteBuffer = threadLocal.get();
//          byte[] array = byteBuffer.array();
//          long pos = beginReadPosArr[finalI];
//          int idx = (int) (pos / 7);
//          int endIdx = idx + readSizeArr[finalI];
//          while (true) {
//            byteBuffer.clear();
//            int flag = fileChannel.read(byteBuffer, pos);
//            pos += perReadSize;
//            if (flag == -1) {
//              break;
//            }
//            int length = byteBuffer.position();
//            for (int j = 0; j < length; j += 7) {
//              data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
//                      array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
//            }
//            if (idx >= endIdx) {
//              break;
//            }
//          }
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
//      });
//    }
//
//    currentThreadRead();
//
//    for (Future future : futures) {
//      future.get();
//    }
//    return PubTools.solve(data, 0, (int) (file.length() / 7 - 1), index);
//  }
//
//  private void currentThreadRead() throws IOException {
//    int finalI = concurrentQueryThreadNum - 1;
//    long[] data = MyAnalyticDB.helper.get();
//    ByteBuffer byteBuffer = ByteBuffer.allocate(perReadSize);
//    byte[] array = byteBuffer.array();
//    long pos = beginReadPosArr[finalI];
//    int idx = (int) (pos / 7);
//    int endIdx = idx + readSizeArr[finalI];
//    while (true) {
//      byteBuffer.clear();
//      int flag = fileChannel.read(byteBuffer, pos);
//      pos += perReadSize;
//      if (flag == -1) {
//        break;
//      }
//      int length = byteBuffer.position();
//      for (int j = 0; j < length; j += 7) {
//        data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
//                array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
//      }
//      if (idx >= endIdx) {
//        break;
//      }
//    }
//  }
//
//  private void fillThreadReadFileInfo() {
//    long fileLen = file.length();
//    int count = (int) (fileLen / 7 / concurrentQueryThreadNum);
//    long pos = 0;
//    for (int i = 0; i < concurrentQueryThreadNum; i++) {
//      beginReadPosArr[i] = pos;
//      pos += count * 7L;
//      readSizeArr[i] = count;
//    }
//    readSizeArr[concurrentQueryThreadNum - 1] = (int) ((fileLen / 7) - (count * (concurrentQueryThreadNum - 1)));
//  }

  private static ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(perReadSize));

  public long get2(int index, int count) throws Exception {
    FileChannel partFileChannel = null;
    long lastTmpSize = 0;
    long tmpSize = 0;
    for (int i = 0; i < splitNum; i++) {
      tmpSize += partFileChannels[i].size();
      if (tmpSize > index * 7) {
        partFileChannel = partFileChannels[i];
        index = (int) (index - (lastTmpSize / 7));
        break;
      }
      lastTmpSize = tmpSize;
    }

    long[] data = MyAnalyticDB.helper.get();
    ByteBuffer byteBuffer = threadLocal.get();
    byte[] array = byteBuffer.array();
    int idx = 0;
    long pos = 0;
    while (true) {
      byteBuffer.clear();
      long begin1 = System.currentTimeMillis();
      int flag = partFileChannel.read(byteBuffer, pos);
      MyAnalyticDB.diskReadFileTime.addAndGet(System.currentTimeMillis() - begin1);

      pos += perReadSize;
      if (flag == -1) {
        break;
      }
      int length = byteBuffer.position();
      for (int i = 0; i < length; i += 7) {
        data[idx++] = makeLong(bytePrev, array[i], array[i + 1], array[i + 2],
                array[i + 3], array[i + 4], array[i + 5], array[i + 6]);
      }
    }

    return PubTools.solve(data, 0, idx - 1, index);
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
      if (fileChannel == null) {
        synchronized (this) {
          if (fileChannel == null) {
            File path = new File(workspaceDir + "/" + tableName);
            if (!path.exists()) {
              path.mkdirs();
            }

            file = new File(workspaceDir + "/" + tableName + "/" + col + "_" + blockIndex + ".data");
            if (!file.exists()) {
              file.createNewFile();
            }
            fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            for (int i = 0; i < splitNum; i++) {
              File file = new File(workspaceDir + "/" + tableName + "/partFile_" + i + "_" + col + "_" + blockIndex + ".data");
              if (!file.exists()) {
                file.createNewFile();
              }
              partFileChannels[i] = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }



}
