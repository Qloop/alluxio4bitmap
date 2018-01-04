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

package alluxio.client.file;

import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.TestBufferedBlockInStreamV2;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.PreconditionMessage;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for the {@link FileInStreamV2}; class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, UnderFileSystem.class})
public class FileInStreamV2Test {

  private static final long STEP_LENGTH = 100L;
  private static final long FILE_LENGTH = 350L;
  private static final long NUM_STREAMS = 1;

  private AlluxioBlockStore mBlockStore;
  private FileSystemContext mContext;
  private FileSystemWorkerClient mWorkerClient;
  private FileInfo mInfo;
  private URIStatus mStatus;

  private List<TestBufferedBlockOutStream> mCacheStreams;

  private FileInStream mTestStream;

  private boolean mDelegateUfsOps;

  private long getBlockLength(int streamId) {
    return streamId == NUM_STREAMS - 1 ? 50 : STEP_LENGTH;
  }

  /**
   * Sets up the context and streams before a test runs.
   *
   * @throws AlluxioException when the worker ufs operations fail
   * @throws IOException when the read and write streams fail
   */
  @Before
  public void before() throws Exception {
    mInfo = new FileInfo().setBlockSizeBytes(STEP_LENGTH).setLength(FILE_LENGTH);
    mContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = Mockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);
    List<Long> blockIds = new ArrayList<>();
    blockIds.add(0l);
    Mockito.when(mBlockStore.getInStream(Mockito.eq(0l), Mockito.any(InStreamOptions.class)))
        .thenAnswer(new Answer<TestBufferedBlockInStreamV2>() {
          @Override
          public TestBufferedBlockInStreamV2 answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            long i = (Long) invocationOnMock.getArguments()[0];
            byte[] input = BufferUtils
                .getIncreasingByteArray((int) (i * FILE_LENGTH), (int) FILE_LENGTH);
            return new TestBufferedBlockInStreamV2(i, input);
          }
        });
    mInfo.setBlockIds(blockIds);
    mStatus = new URIStatus(mInfo);
    mTestStream =
        new FileInStreamV2(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(false), mContext);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleByteRead() throws Exception {
    for (int i = 0; i < FILE_LENGTH; i++) {
      Assert.assertEquals(i & 0xff, mTestStream.read());
    }
    mTestStream.close();
  }

  /**
   * Tests that reading half of a file works.
   */
  @Test
  public void readHalfFile() throws Exception {
    testReadBuffer((int) (FILE_LENGTH / 2));
  }

  /**
   * Tests that reading a part of a file works.
   */
  @Test
  public void readPartialBlock() throws Exception {
    testReadBuffer((int) (STEP_LENGTH / 2));
  }

  /**
   * Tests that reading the complete block works.
   */
  @Test
  public void readBlock() throws Exception {
    testReadBuffer((int) STEP_LENGTH);
  }

  /**
   * Tests that reading the complete file works.
   */
  @Test
  public void readFile() throws Exception {
    testReadBuffer((int) FILE_LENGTH);
  }

  /**
   * Tests that reading a buffer at an offset writes the bytes to the correct places.
   */
  @Test
  public void readOffset() throws IOException {
    int offset = (int) (STEP_LENGTH / 3);
    int len = (int) STEP_LENGTH;
    byte[] buffer = new byte[offset + len];
    // Create expectedBuffer containing `offset` 0's followed by `len` increasing bytes
    byte[] expectedBuffer = new byte[offset + len];
    System.arraycopy(BufferUtils.getIncreasingByteArray(len), 0, expectedBuffer, offset, len);
    mTestStream.read(buffer, offset, len);
    Assert.assertArrayEquals(expectedBuffer, buffer);
  }

  /**
   * Read through the file in small chunks and verify each chunk.
   */
  @Test
  public void readManyChunks() throws IOException {
    int chunksize = 10;
    // chunksize must divide FILE_LENGTH evenly for this test to work
    Assert.assertEquals(0, FILE_LENGTH % chunksize);
    byte[] buffer = new byte[chunksize];
    int offset = 0;
    for (int i = 0; i < FILE_LENGTH / chunksize; i++) {
      mTestStream.read(buffer, 0, chunksize);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, chunksize), buffer);
      offset += chunksize;
    }
  }

  /**
   * Tests that {@link FileInStream#remaining()} is correctly updated during reads, skips, and
   * seeks.
   */
  @Test
  public void testRemaining() throws IOException {
    Assert.assertEquals(FILE_LENGTH, mTestStream.remaining());
    mTestStream.read();
    Assert.assertEquals(FILE_LENGTH - 1, mTestStream.remaining());
    mTestStream.read(new byte[150]);
    Assert.assertEquals(FILE_LENGTH - 151, mTestStream.remaining());
    mTestStream.skip(140);
    Assert.assertEquals(FILE_LENGTH - 291, mTestStream.remaining());
    mTestStream.seek(310);
    Assert.assertEquals(FILE_LENGTH - 310, mTestStream.remaining());
    mTestStream.seek(130);
    Assert.assertEquals(FILE_LENGTH - 130, mTestStream.remaining());
  }

  /**
   * Tests seek, particularly that seeking over part of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSeek() throws IOException {
    int seekAmount = (int) (STEP_LENGTH / 2);
    int readAmount = (int) (STEP_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Seek halfway into block 1
    mTestStream.seek(seekAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(seekAmount, readAmount), buffer);

    // Seek to current position (does nothing)
    mTestStream.seek(seekAmount + readAmount);
    // Seek a short way past start of block 3
    mTestStream.seek((long) (STEP_LENGTH * 3.1));
    Assert.assertEquals((byte) (STEP_LENGTH * 3.1), mTestStream.read());
    mTestStream.seek(FILE_LENGTH);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void sequenceSeek() throws IOException {
    mTestStream = new FileInStreamV2(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    long fl = mTestStream.remaining();
    for (int i = 0; i < fl; i++) {
      mTestStream.seek((long) i);
      Assert.assertEquals((byte) i, (byte) mTestStream.read());
    }
  }

  @Test
  public void randomSeek() throws IOException {
    mTestStream = new FileInStreamV2(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int time = 1000_00;
    long[] con = new long[time];
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      con[i] = Math.abs(generator.nextInt() / 2) % FILE_LENGTH;
    }
    for (int i = 0; i < time; i++) {
      long p = con[i];
      mTestStream.seek(p);
      Assert.assertEquals((byte) p, (byte) mTestStream.read());
    }
  }

  @Test
  public void sequenceSkip() throws IOException {
    mTestStream = new FileInStreamV2(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    long fl = mTestStream.remaining();
    int v = 0;
    byte[] con = BufferUtils
        .getIncreasingByteArray(0, (int) FILE_LENGTH);
    for (int i = 0; i < fl / 2; i++) {
      mTestStream.skip((long) 1);
      v += 1;
      Assert.assertEquals(con[v], (byte) mTestStream.read());
      v += 1;
    }
  }

  @Test
  public void randomSkip() throws IOException {
    mTestStream = new FileInStreamV2(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    byte[] con = BufferUtils
        .getIncreasingByteArray(0, (int) FILE_LENGTH);
    int pos = 0;
    for (int i = 0; i < time; i++) {
      int tPos;
      int step;
      do {
        step = (generator.nextInt() / 2) % 100;
        tPos = pos + step;
      } while (tPos >= FILE_LENGTH || tPos < 0);
      if (step < 0) {
        mTestStream.seek(tPos);
      } else {
        mTestStream.skip(step);
      }
      Assert.assertEquals(con[tPos], (byte) mTestStream.read());
      pos += step + 1;
    }
  }

  /**
   * Tests skip, particularly that skipping the start of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSkip() throws IOException {
    int skipAmount = (int) (STEP_LENGTH / 2);
    int readAmount = (int) (STEP_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Skip halfway into block 1
    mTestStream.skip(skipAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(skipAmount, readAmount), buffer);

    Assert.assertEquals(0, mTestStream.skip(0l));
    // Skip the next half block, bringing us to block 3
    Assert.assertEquals(STEP_LENGTH / 2, mTestStream.skip(STEP_LENGTH / 2));
    Assert.assertEquals((byte) (STEP_LENGTH * 3), mTestStream.read());
  }


  /**
   * Tests that reading out of bounds properly returns -1.
   */
  @Test
  public void readOutOfBounds() throws IOException {
    mTestStream.read(new byte[(int) FILE_LENGTH]);
    Assert.assertEquals(-1, mTestStream.read());
    Assert.assertEquals(-1, mTestStream.read(new byte[10]));
  }

  /**
   * Tests that specifying an invalid offset/length for a buffer read throws the right exception.
   */
  @Test
  public void readBadBuffer() throws IOException {
    try {
      mTestStream.read(new byte[10], 5, 6);
      Assert.fail("the buffer read of invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking to a negative position will throw the right exception.
   */
  @Test
  public void seekNegative() throws IOException {
    try {
      mTestStream.seek(-1);
      Assert.fail("seeking negative position should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking past the end of the stream will throw the right exception.
   */
  @Test
  public void seekPastEnd() throws IOException {
    try {
      mTestStream.seek(FILE_LENGTH + 1);
      Assert.fail("seeking past the end of the stream should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
          FILE_LENGTH + 1), e.getMessage());
    }
  }

  /**
   * Tests that skipping a negative amount correctly reports that 0 bytes were skipped.
   */
  @Test
  public void skipNegative() throws IOException {
    Assert.assertEquals(0, mTestStream.skip(-10));
  }

  /**
   * Tests that the file in stream uses the supplied location policy.
   */
  @Test
  public void locationPolicy() throws Exception {
    FileWriteLocationPolicy policy = Mockito.mock(FileWriteLocationPolicy.class);
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE).setLocationPolicy(policy), mContext);
    mTestStream.read();
    Mockito.verify(policy).getWorkerForNextBlock(Mockito.anyListOf(BlockWorkerInfo.class),
        Mockito.anyLong());
  }

  /**
   * Tests that the correct exception message is produced when the location policy is not specified.
   */
  @Test
  public void missingLocationPolicy() {
    try {
      mTestStream = new FileInStream(mStatus,
          InStreamOptions.defaults().setReadType(ReadType.CACHE).setLocationPolicy(null), mContext);
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString(),
          e.getMessage());
    }
  }

  /**
   * Tests that reading dataRead bytes into a buffer will properly write those bytes to the cache
   * streams and that the correct bytes are read from the {@link FileInStream}.
   *
   * @param dataRead the bytes to read
   * @throws Exception when reading from the stream fails
   */
  private void testReadBuffer(int dataRead) throws Exception {
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);
  }

  /**
   * Verifies that data was properly written to the cache streams.
   *
   * @param dataRead the bytes to read
   */
  private void verifyCacheStreams(long dataRead) {
    for (int streamIndex = 0; streamIndex < NUM_STREAMS; streamIndex++) {
      TestBufferedBlockOutStream stream = mCacheStreams.get(streamIndex);
      byte[] data = stream.getWrittenData();
      if (streamIndex * STEP_LENGTH > dataRead) {
        Assert.assertEquals(0, data.length);
      } else {
        long dataStart = streamIndex * STEP_LENGTH;
        for (int i = 0; i < STEP_LENGTH && dataStart + i < dataRead; i++) {
          Assert.assertEquals((byte) (dataStart + i), data[i]);
        }
      }
    }
  }
}
