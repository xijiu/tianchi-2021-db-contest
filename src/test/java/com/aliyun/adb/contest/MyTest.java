package com.aliyun.adb.contest;

import org.junit.Test;

import java.nio.ByteBuffer;

public class MyTest {

  @Test
  public void test() throws Exception {
  }


  public synchronized void storeLongArr2() throws Exception {
    long[] dataArr = new long[] {71213169107795968L, 71494644084506624L};

    putToByteBuffer(dataArr, 2);

    printResult();
  }

  private void printResult() {
    byte partNum = (byte) (15 << 4);
    byte bytePrev = 0;
    byte first = (byte) (((array[0] >> 4) & 15) | partNum);
    byte second = (byte) ((array[0] & 15) | partNum);
    System.out.println("first byte is " + Integer.toBinaryString(first));
    System.out.println("second byte is " + Integer.toBinaryString(second));
//    long a = DiskBlock.makeLong(bytePrev, first, array[1], array[2],
//            array[3], array[4], array[5], array[6]);
//    long b = DiskBlock.makeLong(bytePrev, second, array[7], array[8],
//            array[9], array[10], array[11], array[12]);
//
//    System.out.println(a);
//    System.out.println(b);
  }

  byte[] array = new byte[13];

  private void putToByteBuffer(long[] dataArr, int length) {
    int actualLen = length % 2 == 0 ? length : length - 1;
    int index = 0;
    for (int i = 0; i < actualLen; i += 2) {
      long data1 = dataArr[i];
      long data2 = dataArr[i + 1];

      array[index++] = (byte) ((data1 >> 48 << 4) | (data2 << 12 >>> 60));
      array[index++] = (byte)(data1 >> 40);
      array[index++] = (byte)(data1 >> 32);
      array[index++] = (byte)(data1 >> 24);
      array[index++] = (byte)(data1 >> 16);
      array[index++] = (byte)(data1 >> 8);
      array[index++] = (byte)(data1);

      array[index++] = (byte)(data2 >> 40);
      array[index++] = (byte)(data2 >> 32);
      array[index++] = (byte)(data2 >> 24);
      array[index++] = (byte)(data2 >> 16);
      array[index++] = (byte)(data2 >> 8);
      array[index++] = (byte)(data2);
    }
  }
}
