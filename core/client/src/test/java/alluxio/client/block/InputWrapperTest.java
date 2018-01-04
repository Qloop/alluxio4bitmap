/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link BufferedBlockInStream} class.
 */
public class InputWrapperTest {

//
//
//
//
//
  private BlockInputWrapper mTestStream;
  private long mBlockSize;
  private long mBufferSize;

  /**
   * Tests that {@link BufferedBlockInStream#read(byte[], int, int)} buffering logic.
   *
   * @throws Exception when reading from the stream fails
   */
//  @Test
//  public void bufferRead() throws Exception {
//    int position = 0;
//    int size = (int) mBufferSize / 2;
//    byte[] readBytes = new byte[size];
//    long shouldRemain = mBufferSize - size;

  /**
   * Sets up the stream before a test runs.
   */
  @Before
  public void before() {
    mBufferSize = Configuration.getBytes(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    mBlockSize = mBufferSize * 10;
    mTestStream = new BlockInputWrapper(new TestBufferedBlockInStreamV3(1L,
        BufferUtils.getIncreasingByteArray(0, (int) mBlockSize)));
  }
//    // Read half buffer, should be from buffer
//    Assert.assertEquals(size, mTestStream.read(readBytes));
//    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(position, size, readBytes));
//    Assert.assertEquals(shouldRemain, mTestStream.mBuffer.remaining());

  /**
   * Verifies the byte by byte read is equal to an increasing byte array, where the written data is
   * an increasing byte array.
   */
  @Test
  public void singleByteRead() throws Exception {
    for (int i = 0; i < mBlockSize; i++) {
      Assert.assertEquals(i & 0xFF, mTestStream.readByte());
    }
  }
//    position += size;
//    shouldRemain -= size;

  /**
   * Tests for the {@link BufferedBlockInStream#skip(long)} method.
   */
  @Test
  public void skip() throws Exception {
    // Skip forward
    mTestStream.skip(10);
//    Assert.assertEquals(10, mTestStream);
    Assert.assertEquals(10, mTestStream.readByte());

    // Skip 0
    mTestStream.skip(0);
    Assert.assertEquals(11, mTestStream.readByte());
  }


  /**
   * Tests that {@link BufferedBlockInStream#read(byte[], int, int)} works for bulk reads.
   */
  @Test
  public void bulkRead() throws Exception {
    int size = (int) mBlockSize / 10;
    byte[] readBytes = new byte[size];

//    // Read first 1/10th bytes
//    Assert.assertEquals(size, mTestStream.readBytes(readBytes));
//    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(0, size, readBytes));
//
//    // Read next 1/10th bytes
//    Assert.assertEquals(size, mTestStream.readByte(readBytes));
//    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, size, readBytes));
//
//    // Read with offset and length
//    Assert.assertEquals(1, mTestStream.read(readBytes, size - 1, 1));
//    Assert.assertEquals(size * 2 & 0xFF, readBytes[size - 1]);
  }
//    // Read buffer size bytes, should not be from buffer
//    Assert.assertEquals(size, mTestStream.read(readBytes));
//    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(position, size, readBytes));
//    Assert.assertEquals(shouldRemain, mTestStream.mBuffer.remaining());
//  }
}
