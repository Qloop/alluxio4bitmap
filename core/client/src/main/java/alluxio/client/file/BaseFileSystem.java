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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.UUID;

/**
 * Default implementation of the {@link FileSystem} interface. Developers can extend this class
 * instead of implementing the interface. This implementation reads and writes data through
 * {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
 */
@PublicApi
@ThreadSafe
public class BaseFileSystem implements FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected final FileSystemContext mFileSystemContext;

  protected BaseFileSystem(FileSystemContext context) {
    mFileSystemContext = context;
  }

  /**
   * @param context file system context
   * @return a {@link BaseFileSystem}
   */
  public static BaseFileSystem get(FileSystemContext context) {
    return new BaseFileSystem(context);
  }

  public static String genTmpPath(String dataPath, String blockID) {

    String name = UUID.randomUUID().toString() + "." + blockID;
    return dataPath + File.separator + name;

  }

  @Override
  public void createDirectory(AlluxioURI path)
          throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    createDirectory(path, CreateDirectoryOptions.defaults());
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
          throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.createDirectory(path, options);
      LOG.debug("Created directory " + path.getPath());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream createFile(AlluxioURI path)
          throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, CreateFileOptions.defaults());
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
          throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    URIStatus status;
    try {
      masterClient.createFile(path, options);
      status = masterClient.getStatus(path);
      LOG.debug("Created file " + path.getPath());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
    OutStreamOptions outStreamOptions = options.toOutStreamOptions();
    outStreamOptions.setUfsPath(status.getUfsPath());
    return new FileOutStream(mFileSystemContext, path, outStreamOptions);
  }

  @Override
  public void delete(AlluxioURI path)
          throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    delete(path, DeleteOptions.defaults());
  }

  @Override
  public void delete(AlluxioURI path, DeleteOptions options)
          throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.delete(path, options);
      LOG.debug("Deleted file " + path.getName());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean exists(AlluxioURI path)
          throws InvalidPathException, IOException, AlluxioException {
    return exists(path, ExistsOptions.defaults());
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsOptions options)
          throws InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this more efficient
      masterClient.getStatus(path);
      return true;
    } catch (FileDoesNotExistException | InvalidPathException e) {
      return false;
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void free(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    free(path, FreeOptions.defaults());
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.free(path, options);
      LOG.debug("Freed file " + path.getPath());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public URIStatus getStatus(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    return getStatus(path, GetStatusOptions.defaults());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getStatus(path);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    return listStatus(path, ListStatusOptions.defaults());
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    // TODO(calvin): Fix the exception handling in the master
    try {
      return masterClient.listStatus(path, options);
    } catch (FileDoesNotExistException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    loadMetadata(path, LoadMetadataOptions.defaults());
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.loadMetadata(path, options);
      LOG.debug("loaded metadata {} with options {}", path.getParent(), options);
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
          throws IOException, AlluxioException {
    mount(alluxioPath, ufsPath, MountOptions.defaults());
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
          throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this fail on the master side
      masterClient.mount(alluxioPath, ufsPath, options);
      LOG.info("Mount " + ufsPath.toString() + " to " + alluxioPath.getPath());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    return openFile(path, OpenFileOptions.defaults());
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFileOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    URIStatus status = getStatus(path);
    if (status.isFolder()) {
      throw new FileNotFoundException(
              ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }

    InStreamOptions inStreamOptions = options.toInStreamOptions();
    int version = options.getFileStreamVersion();
    if (version == 1) {
      LOG.info("open normal inStream");
      return FileInStream.create(status, inStreamOptions, mFileSystemContext);
    } else {
      try {
        /**
         * TODO use config
         */
        int retryTime = 1;
        if (!isEmptyFile(status)) {
          int retryCount = 0;
          StringBuilder sb = new StringBuilder();
          boolean showStatusInfo = true;
          while (!isLocalWorker(status, sb) && retryCount++ < retryTime) {
            LOG.info("file:{} is not local mode,read and localization.count :{}", status.getPath(),
                    retryCount);
            if (showStatusInfo) {
              LOG.info("file:{},info:{}", status.getPath(), sb.toString());
              showStatusInfo = false;
            }
            forceLocal(status);
            status = getStatus(path);
          }
          if (!isLocalWorker(status, null)) {
            if (version == 1024) {
              LOG.warn("path:{} is not localization after retry {} times,and config is client read data to support localization," +
                              "so must extract data into client local disk ",
                      status.getPath(),
                      retryCount);
              return createLocalDiskFileStream(status, inStreamOptions);
            }
            LOG.warn("Oops!path:{} localization is failed after retry {} times,and return normal file in stream that can't random read",
                    status.getPath(),
                    retryCount);
            return FileInStream.create(status, inStreamOptions, mFileSystemContext);
          } else {
            LOG.info("GREAT!path:{} is local mode,try count:{}", status.getPath(),
                    retryCount);
          }
        } else {
          return new FileInStreamEmpty(status, inStreamOptions, mFileSystemContext);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (version == 2) {
        LOG.info("open advanced inStream v2");
        return FileInStreamV2.create(status, inStreamOptions, mFileSystemContext);
      } else {
        LOG.info("open advanced inStream v3");
        return FileInStreamV3.create(status, inStreamOptions, mFileSystemContext);
      }
    }

  }

  public FileInStream createWrapperV2(URIStatus status, InStreamOptions inStreamOptions) {
    LOG.info("use advanced inStream v2 in wrapper mode");
    return FileInStreamV2.create(status, inStreamOptions, mFileSystemContext);
  }

  public FileInStream createLocalDiskFileStream(URIStatus status, InStreamOptions inStreamOptions)
          throws Exception {

    String dataPath = Configuration.get(PropertyKey.USER_LOCAL_FILE_IN_STREAM_TMP_PATH);
    FileUtils.forceMkdir(new File(dataPath));
    String fullPath = genTmpPath(dataPath, String.valueOf(status.getBlockIds().get(0)));
    forceLocalDisk(status, fullPath);
    return FileInStreamV4.create(status, inStreamOptions, mFileSystemContext, fullPath);
  }

  private boolean isEmptyFile(URIStatus status) {
    return status.getLength() == 0;
  }

  private boolean isLocalWorker(URIStatus status, StringBuilder sb) throws Exception {
    Preconditions.checkNotNull(status);
    Preconditions.checkNotNull(status.getBlockIds());
    Preconditions
            .checkState(status.getBlockIds().size() == 1, "the file:%s must be consisted of one block",
                    status.getPath());

    AlluxioBlockStore mBlockStore = AlluxioBlockStore.create(mFileSystemContext);

    BlockInfo info = mBlockStore.getInfo(status.getBlockIds().get(0));
    if (info.getLocations().isEmpty()) {
      return false;
    } else {
      for (BlockLocation location : info.getLocations()) {
        if (location.getWorkerAddress().getHost().equals(NetworkAddressUtils.getLocalHostName())) {
          return true;
        } else {
          if (sb != null) {
            sb.append(" worker:[ id:").append(location.getWorkerId())
                    .append(", host:").append(location.getWorkerAddress().getHost()).append("]");
          }
        }
      }
      if (sb != null) {
        sb.append("local client host:").append(NetworkAddressUtils.getLocalHostName());
      }
    }
    return false;
  }

  private void forceLocal(URIStatus status) throws Exception {
    LOG.warn("start localization.read file:{} and  load data into local worker:{}", status.getPath(),
            NetworkAddressUtils.getLocalHostName());
    Closer closer = Closer.create();
    try {
      OpenFileOptions options = OpenFileOptions.defaults()
              .setReadType(ReadType.CACHE_PROMOTE)
              .setLocationPolicy(new SpecificHostPolicy(NetworkAddressUtils.getLocalHostName()));
      FileInStream in = closer
              .register(FileInStream.create(status, options.toInStreamOptions(), mFileSystemContext));
      byte[] buf = new byte[8 * Constants.MB];
      while (true) {
        if (in.read(buf) == -1) {
          break;
        }
      }
    } catch (Exception e) {
      throw closer.rethrow(e);
    } finally {
      LOG.warn("finish localization, file:{} worker:{}", status.getPath(),
              NetworkAddressUtils.getLocalHostName());
      closer.close();
    }
  }

  private void forceLocalDisk(URIStatus status, String path) throws Exception {
    if (!isLocalWorker(status, null)) {
      LOG.warn("the file:{} is not local mode,to load data into local disk:{}", status.getPath()
              , path);
      Closer closer = Closer.create();
      RandomAccessFile f = closer.register(new RandomAccessFile(
              new File(path).getAbsoluteFile(), "rw"));
      FileChannel fc = closer.register(f.getChannel());
      long blockSize = status.getBlockSizeBytes();
      MappedByteBuffer buffer = fc.map(MapMode.READ_WRITE, 0, blockSize);
      try {
        OpenFileOptions options = OpenFileOptions.defaults()
                .setReadType(ReadType.CACHE_PROMOTE)
                .setLocationPolicy(new SpecificHostPolicy(NetworkAddressUtils.getLocalHostName()));
        FileInStream in = closer
                .register(
                        FileInStream.create(status, options.toInStreamOptions(), mFileSystemContext));
        byte[] buf = new byte[8 * Constants.MB];
        while (true) {
          int l = in.read(buf);
          if (l == -1) {
            break;
          }
          buffer.put(buf, 0, l);
        }
      } catch (Exception e) {
        throw closer.rethrow(e);
      } finally {
        closer.close();
        BufferUtils.cleanDirectBuffer(buffer);
      }
    }
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst)
          throws FileDoesNotExistException, IOException, AlluxioException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Update this code on the master side.
      masterClient.rename(src, dst);
      LOG.debug("Renamed file " + src.getPath() + " to " + dst.getPath());
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAttribute(AlluxioURI path)
          throws FileDoesNotExistException, IOException, AlluxioException {
    setAttribute(path, SetAttributeOptions.defaults());
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
          throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.setAttribute(path, options);
      LOG.debug("Set attributes for path {} with options {}", path.getPath(), options);
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void unmount(AlluxioURI path) throws IOException, AlluxioException {
    unmount(path, UnmountOptions.defaults());
  }

  @Override
  public void unmount(AlluxioURI path, UnmountOptions options)
          throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.unmount(path);
      LOG.info("Unmount " + path);
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }
}
