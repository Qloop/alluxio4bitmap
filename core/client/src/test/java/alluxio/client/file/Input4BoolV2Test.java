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
import alluxio.client.block.TestBufferedBlockInStreamV2;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.AlluxioException;
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
public class Input4BoolV2Test {

  protected static final long STEP_LENGTH = 100L;
  protected static final long UNIT_LENGTH = 350L;
  protected static final short NUM_STREAMS = 1;
  protected static final int UNIT_SIZE = 1;
  protected AlluxioBlockStore mBlockStore;
  protected FileSystemContext mContext;
  protected FileSystemWorkerClient mWorkerClient;
  protected FileInfo mInfo;
  protected URIStatus mStatus;

  protected List<TestBufferedBlockOutStream> mCacheStreams;

  protected FileInStream mTestStream;

  private boolean mDelegateUfsOps;

  private long
  getBlockLength(int streamId) {
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
    mInfo = new FileInfo().setBlockSizeBytes(STEP_LENGTH).setLength(UNIT_LENGTH * UNIT_SIZE);
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
                .getIncreasingBoolByteArray((int) (i * UNIT_LENGTH), (int) UNIT_LENGTH);
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
  public void singleReadPos() throws Exception {
    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(i % 2 == 1, mTestStream.readBool(i * UNIT_SIZE));
      Assert.assertEquals(UNIT_LENGTH * UNIT_SIZE, mTestStream.remaining());

    }
    mTestStream.close();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleRead() throws Exception {
    for (int i = 0; i < UNIT_LENGTH; i++) {
      Assert.assertEquals(i % 2 == 1, mTestStream.readBool());
      Assert.assertEquals(UNIT_LENGTH * UNIT_SIZE - UNIT_SIZE - i * UNIT_SIZE,
          mTestStream.remaining());

    }
    mTestStream.close();
  }

  @Test
  public void randomRead() throws Exception {

    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      int p = (int) (Math.abs(generator.nextInt() / 2) % UNIT_LENGTH);
      Assert.assertEquals(p % 2 == 1, mTestStream.readBool(p * UNIT_SIZE));
    }
  }


  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void sequenceSeek() throws IOException {

    long fl = UNIT_LENGTH;
    for (int i = 0; i < fl; i++) {
      mTestStream.seek(i * UNIT_SIZE);
      Assert.assertEquals(i % 2 == 1, (boolean) mTestStream.readBool());
    }
    Assert.assertEquals(0, mTestStream.remaining());
  }

  @Test
  public void randomSeek() throws IOException {

    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    for (int i = 0; i < time; i++) {
      long p = Math.abs(generator.nextInt() / 2) % UNIT_LENGTH;
      mTestStream.seek(p * UNIT_SIZE);
      Assert.assertEquals(p % 2 == 1, (boolean) mTestStream.readBool());
    }
  }

  @Test
  public void sequenceSkip() throws IOException {

    int v = 0;
    boolean[] con = BufferUtils
        .getIncreasingBoolArray(0, (int) UNIT_LENGTH);
    for (int i = 0; i < UNIT_LENGTH / 2; i++) {
      mTestStream.skip(UNIT_SIZE);
      v += 1;
      Assert.assertEquals(con[v], (boolean) mTestStream.readBool());
      v += 1;
    }
    Assert.assertEquals(0, mTestStream.remaining());

  }

  @Test
  public void randomSkip() throws IOException {
    int time = 1000_00;
    RandomGenerator generator = new JDKRandomGenerator();

    boolean[] con = BufferUtils
        .getIncreasingBoolArray(0, (int) UNIT_LENGTH);
    int pos = 0;
    for (int i = 0; i < time; i++) {
      int tPos;
      int step;
      do {
        step = (generator.nextInt() / 2) % 100;
        tPos = pos + step;
      } while (tPos * UNIT_SIZE >= UNIT_LENGTH * UNIT_SIZE || tPos < 0);
      if (step < 0) {
        mTestStream.seek(tPos * UNIT_SIZE);
      } else {
        mTestStream.skip(step * UNIT_SIZE);
      }
      Assert.assertEquals(con[tPos], (boolean) mTestStream.readBool());
      pos += step + 1;
    }
  }


}
