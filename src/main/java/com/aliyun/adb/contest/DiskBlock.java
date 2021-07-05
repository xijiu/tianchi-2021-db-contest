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

  public static final int cacheLength = 4096 * 2;

  public static final int secondCacheLength = (int) (cacheLength);

  private final String tableName;

  private static final int perReadSize = 7 * 1024 * 128;

  private static final int concurrentQueryThreadNum = 2;

  private final long[] beginReadPosArr = new long[concurrentQueryThreadNum];

  private final int[] readSizeArr = new int[concurrentQueryThreadNum];

  private static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);




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


  public long get2(int index, int count) throws Exception {
    long[] data = MyAnalyticDB.helper.get();
    fillThreadReadFileInfo();

    Future[] futures = new Future[1];
    for (int i = 0; i < 2; i++) {
      int finalI = i;
      futures[i] = executor.submit(() -> {
        try {
          ByteBuffer byteBuffer = threadLocal.get();
          byte[] array = byteBuffer.array();
          long pos = beginReadPosArr[finalI];
          int idx = (int) (pos / 7);
          int endIdx = idx + readSizeArr[finalI];
          while (true) {
            byteBuffer.clear();
            int flag = fileChannel.read(byteBuffer, pos);
            pos += perReadSize;
            if (flag == -1) {
              break;
            }
            int length = byteBuffer.position();
            for (int j = 0; j < length; j += 7) {
              data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
                      array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
            }
            if (idx >= endIdx) {
              break;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }

    currentThreadRead();

    for (Future future : futures) {
      future.get();
    }
    return PubTools.solve(data, 0, (int) (file.length() / 7 - 1), index);
  }

  private void currentThreadRead() throws IOException {
    int finalI = concurrentQueryThreadNum - 1;
    long[] data = MyAnalyticDB.helper.get();
    ByteBuffer byteBuffer = ByteBuffer.allocate(perReadSize);
    byte[] array = byteBuffer.array();
    long pos = beginReadPosArr[finalI];
    int idx = (int) (pos / 7);
    int endIdx = idx + readSizeArr[finalI];
    while (true) {
      byteBuffer.clear();
      int flag = fileChannel.read(byteBuffer, pos);
      pos += perReadSize;
      if (flag == -1) {
        break;
      }
      int length = byteBuffer.position();
      for (int j = 0; j < length; j += 7) {
        data[idx++] = makeLong(bytePrev, array[j], array[j + 1], array[j + 2],
                array[j + 3], array[j + 4], array[j + 5], array[j + 6]);
      }
      if (idx >= endIdx) {
        break;
      }
    }
  }

  private void fillThreadReadFileInfo() {
    long fileLen = file.length();
    int count = (int) (fileLen / 7 / concurrentQueryThreadNum);
    long pos = 0;
    for (int i = 0; i < concurrentQueryThreadNum; i++) {
      beginReadPosArr[i] = pos;
      pos += count * 7L;
      readSizeArr[i] = count;
    }
    readSizeArr[concurrentQueryThreadNum - 1] = (int) ((fileLen / 7) - (count * (concurrentQueryThreadNum - 1)));
  }

  private static ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(perReadSize));

//  public long get2(int index, int count) throws Exception {
//    long[] data = MyAnalyticDB.helper.get();
//    ByteBuffer byteBuffer = threadLocal.get();
//    byte[] array = byteBuffer.array();
//    int idx = 0;
//    long pos = 0;
//    while (true) {
//      byteBuffer.clear();
//      int flag = fileChannel.read(byteBuffer, pos);
//      pos += perReadSize;
//      if (flag == -1) {
//        break;
//      }
//      int length = byteBuffer.position();
//      for (int i = 0; i < length; i += 7) {
//        data[idx++] = makeLong(bytePrev, array[i], array[i + 1], array[i + 2],
//                array[i + 3], array[i + 4], array[i + 5], array[i + 6]);
//      }
//    }
//
//    long begin = System.currentTimeMillis();
//    long solve = PubTools.solve(data, 0, idx - 1, index);
//    MyAnalyticDB.cpuSloveTime.addAndGet(System.currentTimeMillis() - begin);
//    return solve;
//  }




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
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }



}
