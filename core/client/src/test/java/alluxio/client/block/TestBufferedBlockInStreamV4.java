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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.io.Closer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * Mock of {@link BufferedBlockInStreamV3} to creates a BlockInStream on a single block. The
 * data of this stream will be read from the give byte array.
 */
public class TestBufferedBlockInStreamV4 extends LocalBlockInStreamV4 {

  public TestBufferedBlockInStreamV4(long blockId, long blockSize, String tmpBlockPath,
      FileSystemContext context,
      InStreamOptions options, byte[] mData) throws IOException {
    super(blockId, blockSize, tmpBlockPath, context, options);

  }


}
