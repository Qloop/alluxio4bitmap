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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockInputWrapper;
import alluxio.client.block.LocalBlockInStreamV2;
import alluxio.client.block.RemoteBlockInStream;
import alluxio.client.block.SeekUnsupportedException;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.block.UnderStoreBlockInStream.UnderStoreStreamFactory;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.block.BlockId;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 * <p>
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 * 如果文件不是 Local 模式，同时本地又没有 Worker 的情况下。那么文件系统会将远端文件数据读取到本机的配置位置，
 * 给一个临时文件名，close 就删除的这么一个文件。此时的数据读取是在Alluxio 管控以外的。
 */
@PublicApi
@NotThreadSafe
public class FileInStreamV4 extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);

  /**
   * The instream options.
   */
  private final InStreamOptions mInStreamOptions;
  /**
   * The outstream options.
   */
  private final OutStreamOptions mOutStreamOptions;
  private final AlluxioBlockStore mBlockStore;
  /**
   * Caches the entire block even if only a portion of the block is read. Only valid when
   * mShouldCache is true.
   */
  private final boolean mShouldCachePartiallyReadBlock;
  /**
   * Whether to cache blocks in this file into Alluxio.
   */
  private final boolean mShouldCache;

  // The following 3 fields must be kept in sync. They are only updated in updateStreams together.


  /**
   * The blockId used in the block streams.
   */
  private long mStreamBlockId;

  /**
   * The read buffer in file seek. This is used in {@link #readCurrentBlockToEnd()}.
   */
  private byte[] mSeekBuffer;
  private Input input;
  private String mTmpBlockPath;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   */
  protected FileInStreamV4(URIStatus status, InStreamOptions options, FileSystemContext context,
      String tmpBlockPath) {
    super(status, options, context);
    this.mTmpBlockPath = tmpBlockPath;
    mStatus = status;
    mInStreamOptions = options;
    mOutStreamOptions = OutStreamOptions.defaults();
    mShouldCache = false;
    mShouldCachePartiallyReadBlock = false;
    mClosed = false;
    int seekBufferSizeBytes = Math.max((int) options.getSeekBufferSizeBytes(), 1);
    mSeekBuffer = new byte[seekBufferSizeBytes];
    mBlockStore = AlluxioBlockStore.create(context);

    try {
      updateStreams();
      if (mCurrentBlockInStream instanceof Input) {
        Preconditions.checkState(mCurrentBlockInStream instanceof Input,
            "block stream in file:%s is:%s should be local mode ",
            mCurrentBlockInStream != null ? mCurrentBlockInStream.getClass() : "null stream",
            mStatus.getPath());
        input = (Input) mCurrentBlockInStream;
      } else {
//        LOG.warn("block stream in file:{} is:{} not local mode and use file input wrapper",
//            mCurrentBlockInStream != null ? mCurrentBlockInStream.getClass() : "null stream");
//        input = new BlockInputWrapper(mCurrentBlockInStream);
        throw new SeekUnsupportedException("file stream v4 must get a seekable block stream.The class of block stream returned" +
                "from block store is:" + mCurrentBlockInStream.getClass());
      }

    } catch (IOException e) {
      LOG.error("the fileInStream of path:{} failed to update stream", status.getPath());
      throw new RuntimeException(e);
    }
    if (mStatus.getBlockIds().size() != 0) {
      Preconditions.checkState(mStatus.getBlockIds().size() == 1,
          "the file:%s must be consisted of one block", mStatus.getPath());
      if (!isReadingFromLocalBlockWorker()) {
        LOG.warn("file:{} should be local mode", mStatus.getPath());
      }
    } else {
      LOG.warn("file:{} own null block", mStatus.getPath());
    }

    LOG.debug("Init FileInStream V2 with options {}", options);
  }

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link FileInStreamV4} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options,
      FileSystemContext context, String tmpBlockPath) {
    if (status.getLength() == Constants.UNKNOWN_SIZE) {
//      return new UnknownLengthFileInStream(status, options, context);
      throw new UnsupportedOperationException("FileInStreamV2 not support unknown size file");
    }
    return new FileInStreamV4(status, options, context, tmpBlockPath);
  }


  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    input.close();
    try {
      LOG.warn("delete tmp local disk data at  path:{}", mTmpBlockPath);
      org.apache.commons.io.FileUtils.forceDelete(new File(mTmpBlockPath));
    } catch (IOException e) {
      LOG.warn("the path:{} can't delete", mTmpBlockPath);
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mCurrentBlockInStream == null) {
      return -1;
    }
    int data = mCurrentBlockInStream.read();
    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (mCurrentBlockInStream == null) {
      return -1;
    }

    int currentOffset = off;
    int bytesLeftToRead = len;

    while (bytesLeftToRead > 0 && remaining() > 0) {
      int bytesToRead = (int) Math.min(bytesLeftToRead, inStreamRemaining());
      int bytesRead = mCurrentBlockInStream.read(b, currentOffset, bytesToRead);
      if (bytesRead > 0) {
        mPos += bytesRead;
        bytesLeftToRead -= bytesRead;
        currentOffset += bytesRead;
      }
    }

    if (bytesLeftToRead == len && inStreamRemaining() == 0) {
      // Nothing was read, and the underlying stream is done.
      return -1;
    }

    return len - bytesLeftToRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    if (!PACKET_STREAMING_ENABLED) {
      throw new RuntimeException(String.format(
          "Positioned read is not supported, please set %s to true to enable positioned read.",
          PropertyKey.USER_PACKET_STREAMING_ENABLED.toString()));
    }
    if (pos < 0 || pos >= mFileLength) {
      return -1;
    }

    // If partial read cache is enabled, we fall back to the normal read.
    if (mShouldCachePartiallyReadBlock) {
      synchronized (this) {
        long oldPos = mPos;
        try {
          seek(pos);
          return read(b, off, len);
        } finally {
          seek(oldPos);
        }
      }
    }

    int lenCopy = len;

    while (len > 0) {
      if (pos >= mFileLength) {
        break;
      }
      long blockId = getBlockId(pos);
      long blockPos = pos % mBlockSize;
      try (InputStream inputStream = getBlockInStream(blockId)) {
        assert inputStream instanceof PositionedReadable;
        int bytesRead = ((PositionedReadable) inputStream).positionedRead(blockPos, b, off, len);
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
      }
    }
    return lenCopy - len;
  }

  @Override
  public long remaining() {
    if (!mClosed) {
      /**
       * 当前Block存在，那么就用block的剩余量。
       */
      if (mCurrentBlockInStream != null) {
        return ((BoundedStream) mCurrentBlockInStream).remaining();
      } else {
        /**
         * 当前没有block流存在，那么文件总长度就是剩余量。
         */
        return mFileLength;
      }
    } else {
      return 0;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0 || pos > mFileLength) {
      Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
      Preconditions.checkArgument(pos <= mFileLength,
          PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);
    }
    ((Seekable) mCurrentBlockInStream).seek(pos);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    return mCurrentBlockInStream.skip(n);
  }

  /**
   * @return the maximum position to seek to
   */
  protected long maxSeekPosition() {
    return mFileLength;
  }

  /**
   * @param pos the position to check
   * @return the block size in bytes for the given pos, used for worker allocation
   */
  protected long getBlockSizeAllocation(long pos) {
    return getBlockSize(pos);
  }

  /**
   * Creates and returns a {@link InputStream} for the UFS.
   *
   * @param blockStart the offset to start the block from
   * @param length the length of the block
   * @param path the UFS path
   * @return the {@link InputStream} for the UFS
   * @throws IOException if the stream cannot be created
   */
  protected InputStream createUnderStoreBlockInStream(long blockStart, long length, String path)
      throws IOException {
    return new UnderStoreBlockInStream(mContext, blockStart, length, mBlockSize,
        getUnderStoreStreamFactory(path, mContext));
  }

  protected UnderStoreStreamFactory getUnderStoreStreamFactory(String path, FileSystemContext
      context) throws IOException {
    if (Configuration.getBoolean(PropertyKey.USER_UFS_DELEGATION_ENABLED)) {
      return new DelegatedUnderStoreStreamFactory(context, path);
    } else {
      return new DirectUnderStoreStreamFactory(path);
    }
  }

  /**
   * If we are not in the last block or if the last block is equal to the normal block size,
   * return the normal block size. Otherwise return the block size of the last block.
   *
   * @param pos the position to get the block size for
   * @return the size of the block that covers pos
   */
  protected long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mFileLength % mBlockSize;
    if (mFileLength - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }

  /**
   * Checks whether block instream and cache outstream should be updated.
   * This function is only called by {@link #updateStreams()}.
   *
   * @param currentBlockId cached result of {@link #getCurrentBlockId()}
   * @return true if the block stream should be updated
   */
  protected boolean shouldUpdateStreams(long currentBlockId) {
    if (mCurrentBlockInStream == null || currentBlockId != mStreamBlockId) {
      return true;
    }
    if (mCurrentCacheStream != null && inStreamRemaining() != cacheStreamRemaining()) {
      throw new IllegalStateException(
          String.format("BlockInStream and CacheStream is out of sync %d %d.",
              inStreamRemaining(), cacheStreamRemaining()));
    }
    return inStreamRemaining() == 0;
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   */
  private void closeOrCancelCacheStream() {
    if (mCurrentCacheStream == null) {
      return;
    }
    try {
      if (cacheStreamRemaining() == 0) {
        mCurrentCacheStream.close();
      } else {
        cacheStreamCancel();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof BlockDoesNotExistException) {
        // This happens if two concurrent readers read trying to cache the same block. One cancelled
        // before the other. Then the other reader will see this exception since we only keep
        // one block per blockId in block worker.
        LOG.info("Block {} does not exist when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof InvalidWorkerStateException) {
        // This happens if two concurrent readers trying to cache the same block and they acquired
        // different file system contexts.
        // instances (each instance has its only session ID).
        LOG.info("Block {} has invalid worker state when being cancelled.", getCurrentBlockId());
      } else if (e.getCause() instanceof BlockAlreadyExistsException) {
        // This happens if two concurrent readers trying to cache the same block. One successfully
        // committed. The other reader sees this.
        LOG.info("Block {} exists.", getCurrentBlockId());
      } else {
        // This happens when there are any other cache stream close/cancel related errors (e.g.
        // server unreachable due to network partition, server busy due to alluxio worker is
        // busy, timeout due to congested network etc). But we want to proceed since we want
        // the user to continue reading when one Alluxio worker is having trouble.
        LOG.info(
            "Closing or cancelling the cache stream encountered IOExecption {}, reading from the "
                + "regular stream won't be affected.", e.getMessage());
      }
    }
    mCurrentCacheStream = null;
  }

  /**
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    return mStatus.getBlockIds().get(0);
  }

  /**
   * @param pos the pos
   * @return the block ID based on the pos
   */
  private long getBlockId(long pos) {
    int index = (int) (pos / mBlockSize);
    Preconditions
        .checkState(index < mStatus.getBlockIds().size(), PreconditionMessage.ERR_BLOCK_INDEX);
    return mStatus.getBlockIds().get(index);
  }

  /**
   * Handles IO exceptions thrown in response to the worker cache request. Cache stream is closed
   * or cancelled after logging some messages about the exceptions.
   *
   * @param e the exception to handle
   */
  private void handleCacheStreamIOException(IOException e) {
    if (e.getCause() instanceof BlockAlreadyExistsException) {
      // This can happen if there are two readers trying to cache the same block. The first one
      // created the block (either as temp block or committed block). The second sees this
      // exception.
      LOG.info(
          "The block with ID {} is already stored in the target worker, canceling the cache "
              + "request.", getCurrentBlockId());
    } else {
      LOG.warn("The block with ID {} could not be cached into Alluxio storage.",
          getCurrentBlockId());
    }
    closeOrCancelCacheStream();
  }

  /**
   * Only updates {@link #mCurrentCacheStream}, {@link #mCurrentBlockInStream} and
   * {@link #mStreamBlockId} to be in sync with the current block (i.e.
   * {@link #getCurrentBlockId()}).
   * If this method is called multiple times, the subsequent invokes become no-op.
   * Call this function every read and seek unless you are sure about the block streams are
   * up-to-date.
   *
   * @throws IOException if the next cache stream or block stream cannot be created
   */
  @Override
  protected void updateStreams() throws IOException {
    long currentBlockId = getCurrentBlockId();
    if (shouldUpdateStreams(currentBlockId)) {
      // The following two function handle negative currentBlockId (i.e. the end of file)
      // correctly.
      updateBlockInStream(currentBlockId);
      mStreamBlockId = currentBlockId;
    }
  }


  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with mPos's block. The new block
   * stream created with {@link UnderStoreBlockInStream#mPos} at position 0.
   * This function is only called in {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   * @throws IOException if the next block in stream cannot be obtained
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream = null;
    }

    // blockId = -1 if mPos = EOF.
    if (blockId < 0) {
      return;
    }
    mCurrentBlockInStream = getBlockInStream(blockId);
  }

  /**
   * Gets the block in stream corresponding a block ID.
   *
   * @param blockId the block ID
   * @return the block in stream
   * @throws IOException if the block in stream cannot be obtained
   */
  private InputStream getBlockInStream(long blockId) throws IOException {
    try {

      return mBlockStore.getLocalDiskInStream(blockId, mInStreamOptions, mTmpBlockPath);
    } catch (IOException e) {
      LOG.debug("Failed to get BlockInStream for block with ID {}, using UFS instead. {}", blockId,
          e);
      if (!mStatus.isPersisted()) {
        LOG.error("Could not obtain data for block with ID {} from Alluxio."
            + " The block is also not available in the under storage.", blockId);
        throw e;
      }
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      return createUnderStoreBlockInStream(blockStart, getBlockSize(blockStart),
          mStatus.getUfsPath());
    }
  }

  /**
   * Seeks to a file position. Blocks are not cached unless they are fully read. This is only called
   * by {@link FileInStreamV4#seek}.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   * pos <= mFileLength)
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  private void seekInternal(long pos) throws IOException {

  }

  /**
   * @return true if {@code mCurrentBlockInStream} is reading from a local block worker
   */
  private boolean isReadingFromLocalBlockWorker() {
    return (mCurrentBlockInStream instanceof LocalBlockInStreamV2) || (
        mCurrentBlockInStream instanceof Locatable && ((Locatable) mCurrentBlockInStream)
            .isLocal());
  }

  /**
   * @return true if {@code mCurrentBlockInStream} is reading from a remote block worker
   */
  private boolean isReadingFromRemoteBlockWorker() {
    return (mCurrentBlockInStream instanceof RemoteBlockInStream) || (
        mCurrentBlockInStream instanceof Locatable && !(((Locatable) mCurrentBlockInStream)
            .isLocal()));
  }


  /**
   * @return the remaining bytes in the current block in stream
   */
  protected long inStreamRemaining() {
    return ((BoundedStream) mCurrentBlockInStream).remaining();
  }

  /**
   * Seeks to the given pos in the current in stream.
   *
   * @param pos the pos
   * @throws IOException if it fails to seek
   */
  private void inStreamSeek(long pos) throws IOException {
    ((Seekable) mCurrentBlockInStream).seek(pos);
  }

  /**
   * @return the remaining bytes in the current cache out stream
   */
  private long cacheStreamRemaining() {
    assert mCurrentCacheStream instanceof BoundedStream;
    return ((BoundedStream) mCurrentCacheStream).remaining();
  }

  /**
   * Cancels the current cache out stream.
   *
   * @throws IOException if it fails to cancel the cache out stream
   */
  private void cacheStreamCancel() throws IOException {
    assert mCurrentCacheStream instanceof Cancelable;
    ((Cancelable) mCurrentCacheStream).cancel();
  }

  @Override
  public int readByte() throws IOException {
    return input.readByte();
  }

  @Override
  public boolean readBool() throws IOException {
    return input.readBool();
  }

  @Override
  public int readShort() throws IOException {
    return input.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return input.readInt();
  }

  @Override
  public float readFloat() throws IOException {
    return input.readFloat();
  }

  @Override
  public long readLong() throws IOException {
    return input.readLong();
  }

  @Override
  public double readDouble() throws IOException {
    return input.readDouble();
  }

  @Override
  public String readString() throws IOException {
    return input.readString();
  }


  @Override
  public int readByte(int pos) throws IOException {
    return input.readByte(pos);
  }

  @Override
  public boolean readBool(int pos) throws IOException {
    return input.readBool(pos);
  }

  @Override
  public int readShort(int pos) throws IOException {
    return input.readShort(pos);
  }

  @Override
  public int readInt(int pos) throws IOException {
    return input.readInt(pos);
  }

  @Override
  public float readFloat(int pos) throws IOException {
    return input.readFloat(pos);
  }

  @Override
  public long readLong(int pos) throws IOException {
    return input.readLong(pos);

  }

  @Override
  public double readDouble(int pos) throws IOException {
    return input.readDouble(pos);
  }

  @Override
  public String readString(int pos) throws IOException {
    return input.readString(pos);
  }

  @Override
  public void readBytes(byte[] bytes, int pos) throws IOException {
    input.readBytes(bytes, pos);
  }
}
