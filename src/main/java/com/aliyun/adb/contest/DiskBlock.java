package com.aliyun.adb.contest;

import com.aliyun.adb.contest.utils.PubTools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

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

  public static final int cacheLength = 4096 * 3;

  public static final int secondCacheLength = (int) (cacheLength);

  public static final int concurrenceQueryDiskNum = 4;

  public static AtomicIntegerArray queryDiskFlag = new AtomicIntegerArray(concurrenceQueryDiskNum);

  public static int[] countArr = new int[concurrenceQueryDiskNum];

  public static int[] indexArr = new int[concurrenceQueryDiskNum];

  public static volatile byte totalBytePrev = 0;

  public static volatile FileChannel totalFileChannel = null;

  public static AtomicInteger finishQueryThreadNum = new AtomicInteger();





  public DiskBlock(int col, int blockIndex) {
    this.col = col;
    this.blockIndex = blockIndex;
    this.bytePrev = (byte) blockIndex;
  }

  public void storeArr(ByteBuffer byteBuffer) throws Exception {
    initFileChannel();
    fileChannel.write(byteBuffer);
  }

  public long get(int index, int count) throws Exception {
    int totalCount = count;
    totalBytePrev = bytePrev;
    totalFileChannel = fileChannel;

    int tmpCount = 0;

    for (int i = 0; i < concurrenceQueryDiskNum; i++) {
      if (i == concurrenceQueryDiskNum - 1) {
        countArr[i] = count - tmpCount;
        indexArr[i] = tmpCount;
        queryDiskFlag.set(i, 1);
      } else {
        countArr[i] = count / concurrenceQueryDiskNum;
        indexArr[i] = tmpCount;
        queryDiskFlag.set(i, 1);
        tmpCount += count / concurrenceQueryDiskNum;
      }
    }

    currentThreadQuery();

    while (finishQueryThreadNum.get() != concurrenceQueryDiskNum) {
      Thread.sleep(2);
    }
    finishQueryThreadNum.set(0);

    return PubTools.solve(MyAnalyticDB.sortArr, 0, totalCount - 1, index);
  }

  private void currentThreadQuery() throws Exception {
    int threadIndex = concurrenceQueryDiskNum - 1;
    DiskBlock.queryDiskFlag.set(threadIndex, 0);
    int count = DiskBlock.countArr[threadIndex];
    int index = DiskBlock.indexArr[threadIndex];

    byte[] batchWriteArr = new byte[count * 7];

    ByteBuffer buffer = ByteBuffer.wrap(batchWriteArr);
    DiskBlock.totalFileChannel.read(buffer, index * 7L);
    buffer.flip();

    int idx = 0;
    int endCount = index + count;
    for (int i = index; i < endCount; i++) {
      MyAnalyticDB.sortArr[i] = DiskBlock.makeLong(
              DiskBlock.totalBytePrev, batchWriteArr[idx++], batchWriteArr[idx++], batchWriteArr[idx++],
              batchWriteArr[idx++], batchWriteArr[idx++], batchWriteArr[idx++], batchWriteArr[idx++]);
    }

    DiskBlock.finishQueryThreadNum.incrementAndGet();
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

  private void initFileChannel() throws IOException {
    if (fileChannel == null) {
      synchronized (this) {
        if (fileChannel == null) {
          file = new File(workspaceDir + "/" + col + "_" + blockIndex + ".data");
          if (file.exists()) {
            file.delete();
          }
          file.createNewFile();
          fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
//          mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 25 * 1024 * 1024);
        }
      }
    }
  }
}
