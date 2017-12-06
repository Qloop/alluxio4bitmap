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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.TestBufferedBlockInStreamV3;
import alluxio.client.file.options.InStreamOptions;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
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
public class Input4IntV3Test extends Input4IntV2Test {

  @Before
  public void before() throws Exception {
    mInfo = new FileInfo().setBlockSizeBytes(STEP_LENGTH).setLength(UNIT_LENGTH * UNIT_SIZE);
    mContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = Mockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);
    List<Long> blockIds = new ArrayList<>();
    blockIds.add(0l);
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, 50);

    Mockito.when(mBlockStore.getInStream(Mockito.eq(0l), Mockito.any(InStreamOptions.class)))
        .thenAnswer(new Answer<TestBufferedBlockInStreamV3>() {
          @Override
          public TestBufferedBlockInStreamV3 answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            long i = (Long) invocationOnMock.getArguments()[0];
            byte[] input = BufferUtils
                .getIncreasingIntByteArray((int) (i * UNIT_LENGTH), (int) UNIT_LENGTH);
            return new TestBufferedBlockInStreamV3(i, input);
          }
        });
    mInfo.setBlockIds(blockIds);
    mStatus = new URIStatus(mInfo);
    initStream();
  }


  public void initStream() {
    mTestStream =
        new FileInStreamV3(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(false), mContext);
  }
}
