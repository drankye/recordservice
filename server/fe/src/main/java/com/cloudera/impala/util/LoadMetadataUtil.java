// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.HdfsCompression;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition.BlockReplica;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Hdfs metadata loading utilities.
 */
public class LoadMetadataUtil {
  private static final Logger LOG = LoggerFactory.getLogger(LoadMetadataUtil.class);

  // An invalid network address, which will always be treated as remote.
  private static final TNetworkAddress REMOTE_NETWORK_ADDRESS =
      new TNetworkAddress("remote*addr", 0);

  // Minimum block size in bytes allowed for synthetic file blocks (other than the last
  // block, which may be shorter).
  private static final long MIN_SYNTHETIC_BLOCK_SIZE = 1024 * 1024;

  // TODO(henry): confirm that this is thread safe - cursory inspection of the class
  // and its usage in getFileSystem suggests it should be.
  private static final Configuration CONF = new Configuration();

  // Prefix in storageId.
  private static final String STORAGE_ID_PREFIX = "DS-";

  public static Configuration getConf() {
    return CONF;
  }

  /**
   * Wrapper around a FileSystem object to hash based on the underlying FileSystem's
   * scheme and authority.
   */
  public static class FsKey {
    private final FileSystem filesystem;

    public FsKey(FileSystem fs) {
      filesystem = fs;
    }

    @Override
    public int hashCode() {
      return filesystem.getUri().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (o != null && o instanceof FsKey) {
        URI uri = filesystem.getUri();
        URI otherUri = ((FsKey) o).filesystem.getUri();
        return uri.equals(otherUri);
      }
      return false;
    }

    @Override
    public String toString() {
      return filesystem.getUri().toString();
    }
  }

  /**
   * Keeps track of THdfsFileBlock metadata and its corresponding BlockLocation. For each
   * i, blocks.get(i) corresponds to locations.get(i).
   */
  public static class FileBlocksInfo {
    public final List<THdfsFileBlock> blocks = Lists.newArrayList();
    public final List<BlockLocation> locations = Lists.newArrayList();

    public void addBlocks(List<THdfsFileBlock> b, List<BlockLocation> l) {
      Preconditions.checkState(b.size() == l.size());
      blocks.addAll(b);
      locations.addAll(l);
    }
  }

  /**
   * Load and return a list of file descriptors for the files in 'dirPath', using the
   * listStatus HDFS API in filesystem to load filestatus. It will not load the file
   * descriptor if the file is a directory, or hidden file starting with . or _, or LZO
   * index files. If the file can be found in the old File description map and not
   * modified, and not 'isMarkedCached' - partition marked as cached, just reuse the one
   * in cache. Otherwise it will create a new File description with filename, file length
   * and modification time.
   *
   * Must be threadsafe. Access to 'oldFileDescMap', 'perFsFileBlocks', 'hostIndex' and
   * 'fileDescMap' must be protected.
   */
  public static List<FileDescriptor> loadFileDescriptors(FileSystem fs, Path dirPath,
      Map<String, List<FileDescriptor>> oldFileDescMap, HdfsFileFormat fileFormat,
      Map<FsKey, FileBlocksInfo> perFsFileBlocks, boolean isMarkedCached, String tblName,
      ListMap<TNetworkAddress> hostIndex, Map<String, List<FileDescriptor>> fileDescMap)
          throws FileNotFoundException, IOException {
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();

    for (FileStatus fileStatus: fs.listStatus(dirPath)) {
      FileDescriptor fd = getFileDescriptor(fs, fileStatus, fileFormat, oldFileDescMap,
          isMarkedCached, perFsFileBlocks, tblName, hostIndex);

      if (fd == null) continue;

      // Add partition dir to fileDescMap if it does not exist.
      String partitionDir = fileStatus.getPath().getParent().toString();
      synchronized (fileDescMap) {
        if (!fileDescMap.containsKey(partitionDir)) {
          fileDescMap.put(partitionDir, new ArrayList<FileDescriptor>());
        }
        fileDescMap.get(partitionDir).add(fd);
      }

      // Add to the list of FileDescriptors for this partition.
      fileDescriptors.add(fd);
    }

    return fileDescriptors;
  }

  /**
   * Identical to loadFileDescriptors, except using the ListStatusIterator HDFS API to
   * load file status.
   */
  public static List<FileDescriptor> loadViaListStatusIterator(FileSystem fs,
      Path partDirPath, Map<String, List<FileDescriptor>> oldFileDescMap,
      HdfsFileFormat fileFormat, Map<FsKey, FileBlocksInfo> perFsFileBlocks,
      boolean isMarkedCached, String tblName, ListMap<TNetworkAddress> hostIndex,
      Map<String, List<FileDescriptor>> fileDescMap) throws FileNotFoundException,
      IOException {
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();

    AbstractFileSystem abstractFs = AbstractFileSystem.createFileSystem(
        partDirPath.toUri(), CONF);
    RemoteIterator<FileStatus> fileStatusItor = abstractFs.listStatusIterator(
        partDirPath);

    while (fileStatusItor.hasNext()) {
      FileStatus fileStatus = fileStatusItor.next();
      FileDescriptor fd = getFileDescriptor(fs, fileStatus, fileFormat, oldFileDescMap,
          isMarkedCached, perFsFileBlocks, tblName, hostIndex);

      if (fd == null) continue;

      // Add partition dir to fileDescMap if it does not exist.
      String partitionDir = fileStatus.getPath().getParent().toString();
      if (!fileDescMap.containsKey(partitionDir)) {
        fileDescMap.put(partitionDir,new ArrayList<FileDescriptor>());
      }
      fileDescMap.get(partitionDir).add(fd);

      // Add to the list of FileDescriptors for this partition.
      fileDescriptors.add(fd);
    }

    return fileDescriptors;
  }

  /**
   * Identical to loadFileDescriptors, except using the ListLocatedStatus HDFS API to load
   * file status.
   * TODO: Got AnalysisException error: Failed to load metadata for table
   * CAUSED BY: ClassCastException: DFSClient#getVolumeBlockLocations expected to be
   * passed HdfsBlockLocations
   * TODO: Use new HDFS API resolved by CDH-30342.
   */
  public static List<FileDescriptor> loadViaListLocatedStatus(FileSystem fs,
      Path partDirPath, Map<String, List<FileDescriptor>> oldFileDescMap,
      HdfsFileFormat fileFormat, Map<FsKey, FileBlocksInfo> perFsFileBlocks,
      boolean isMarkedCached, String tblName, ListMap<TNetworkAddress> hostIndex,
      Map<String, List<FileDescriptor>> fileDescMap) throws FileNotFoundException,
      IOException {
    List<FileDescriptor> fileDescriptors = Lists.newArrayList();

    RemoteIterator<LocatedFileStatus> fileStatusItor = fs.listLocatedStatus(partDirPath);

    while (fileStatusItor.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusItor.next();
      FileDescriptor fd = getFileDescriptor(fs, fileStatus, fileFormat, oldFileDescMap,
          isMarkedCached, perFsFileBlocks, tblName, hostIndex);

      if (fd == null) continue;

      // Add partition dir to fileDescMap if it does not exist.
      String partitionDir = fileStatus.getPath().getParent().toString();
      if (!fileDescMap.containsKey(partitionDir)) {
        fileDescMap.put(partitionDir,new ArrayList<FileDescriptor>());
      }
      fileDescMap.get(partitionDir).add(fd);

      // Add to the list of FileDescriptors for this partition.
      fileDescriptors.add(fd);
    }

    return fileDescriptors;
  }

  /**
   * Populate storage ID metadata inside the newly created THdfsFileBlocks.
   * perFsFileBlocks maps from each filesystem to a FileBLocksInfo. The first list
   * contains the newly created THdfsFileBlocks and the second contains the
   * corresponding BlockLocations.
   */
  public static void loadStorageIds(String tblFullName, int tblNumNodes,
      Map<FsKey, FileBlocksInfo> perFsFileBlocks) {
    // Retrieve the storage IDs for all the blocks from BlockLocation.
    for (FsKey fsKey: perFsFileBlocks.keySet()) {
      LOG.trace("Loading storage ids for: " + tblFullName + ". nodes: " + tblNumNodes +
          ". filesystem: " + fsKey);
      FileBlocksInfo blockLists = perFsFileBlocks.get(fsKey);
      Preconditions.checkNotNull(blockLists);

      long unknownStorageIdCount = 0;
      String[] storageIds;

      for (int i = blockLists.locations.size() - 1; i >= 0; --i) {
        storageIds = blockLists.locations.get(i).getStorageIds();
        Preconditions.checkArgument(storageIds != null);
        // Remove prefix in StorageId.
        for (int j = storageIds.length - 1; j >= 0; --j) {
          if (storageIds[j] == null) {
            ++unknownStorageIdCount;
            storageIds[j] = "";
          } else {
            storageIds[j] = storageIds[j].replaceFirst(STORAGE_ID_PREFIX, "");
          }
        }
        FileBlock.setStorageIds(storageIds, blockLists.blocks.get(i));
      }
      if (unknownStorageIdCount > 0) {
        LOG.warn("Unknown storage id count: " + unknownStorageIdCount);
      }
    }
  }

  /**
   * Get file descriptor according to fileStatus and oldFileDescMap. It will return null
   * if the file is a directory, or hidden file starting with . or _, or LZO index files.
   * If the file can be found in the old File description map and not modified, and not
   * 'isMarkedCached' - partition marked as cached, just reuse the one in cache. Otherwise
   * it will create a new File description with filename, file length and modification
   * time.
   *
   * Must be thread safe. Access to 'oldFileDescMap', 'perFsFileBlocks' and 'hostIndex'
   * must be protected.
   */
  private static FileDescriptor getFileDescriptor(FileSystem fs, FileStatus fileStatus,
      HdfsFileFormat fileFormat, Map<String, List<FileDescriptor>> oldFileDescMap,
      boolean isMarkedCached, Map<FsKey, FileBlocksInfo> perFsFileBlocks, String tblName,
      ListMap<TNetworkAddress> hostIndex) {
    String fileName = fileStatus.getPath().getName().toString();

    if (fileStatus.isDirectory() || FileSystemUtil.isHiddenFile(fileName) ||
        HdfsCompression.fromFileName(fileName) == HdfsCompression.LZO_INDEX) {
      // Ignore directory, hidden file starting with . or _, and LZO index files
      // If a directory is erroneously created as a subdirectory of a partition dir
      // we should ignore it and move on. Hive will not recurse into directories.
      // Skip index files, these are read by the LZO scanner directly.
      return null;
    }

    String partitionDir = fileStatus.getPath().getParent().toString();
    FileDescriptor fd = null;
    // Search for a FileDescriptor with the same partition dir and file name. If one
    // is found, it will be chosen as a candidate to reuse.
    if (oldFileDescMap != null) {
      synchronized (oldFileDescMap) {
        if (oldFileDescMap.get(partitionDir) != null) {
          for (FileDescriptor oldFileDesc: oldFileDescMap.get(partitionDir)) {
            // TODO: This doesn't seem like the right data structure if a directory has
            // a lot of files.
            if (oldFileDesc.getFileName().equals(fileName)) {
              fd = oldFileDesc;
              break;
            }
          }
        }
      }
    }

    // Check if this FileDescriptor has been modified since last loading its block
    // location information. If it has not been changed, the previously loaded value can
    // be reused.
    if (fd == null || isMarkedCached || fd.getFileLength() != fileStatus.getLen()
        || fd.getModificationTime() != fileStatus.getModificationTime()) {
      // Create a new file descriptor and load the file block metadata,
      // collecting the block metadata into perFsFileBlocks.  The disk IDs for
      // all the blocks of each filesystem will be loaded by loadDiskIds().
      fd = new FileDescriptor(fileName, fileStatus.getLen(),
          fileStatus.getModificationTime());
      loadBlockMetadata(fs, fileStatus, fd, fileFormat, perFsFileBlocks, tblName,
          hostIndex);
    }

    return fd;
  }

  /**
   * Create FileBlock according to BlockLocation and hostIndex. Get host names and ports
   * from BlockLocation, and get all replicas' host id from hostIndex.
   *
   * Must be threadsafe. Access to 'hostIndex' must be protected.
   */
  private static FileBlock createFileBlock(BlockLocation loc,
      ListMap<TNetworkAddress> hostIndex) throws IOException {
    // Get the location of all block replicas in ip:port format.
    String[] blockHostPorts = loc.getNames();
    // Get the hostnames for all block replicas. Used to resolve which hosts
    // contain cached data. The results are returned in the same order as
    // block.getNames() so it allows us to match a host specified as ip:port to
    // corresponding hostname using the same array index.
    String[] blockHostNames = loc.getHosts();
    Preconditions.checkState(blockHostNames.length == blockHostPorts.length);
    // Get the hostnames that contain cached replicas of this block.
    Set<String> cachedHosts =
        Sets.newHashSet(Arrays.asList(loc.getCachedHosts()));
    Preconditions.checkState(cachedHosts.size() <= blockHostNames.length);

    // Now enumerate all replicas of the block, adding any unknown hosts
    // to hostMap_/hostList_. The host ID (index in to the hostList_) for each
    // replica is stored in replicaHostIdxs.
    List<BlockReplica> replicas = Lists.newArrayListWithExpectedSize(
        blockHostPorts.length);
    for (int i = 0; i < blockHostPorts.length; ++i) {
      TNetworkAddress networkAddress = BlockReplica.parseLocation(blockHostPorts[i]);
      Preconditions.checkState(networkAddress != null);
      networkAddress.setHdfs_host_name(blockHostNames[i]);
      int idx = -1;
      synchronized (hostIndex) {
        idx = hostIndex.getIndex(networkAddress);
      }
      replicas.add(new BlockReplica(idx, cachedHosts.contains(blockHostNames[i])));
    }
    return new FileBlock(loc.getOffset(), loc.getLength(), replicas);
  }

  /**
   * Queries the filesystem to load the file block metadata (e.g. DFS blocks) for the
   * given file. Adds the newly created block metadata and block location to the
   * perFsFileBlocks, so that the storage IDs for each block can be retrieved from
   * BlockLocation.
   *
   * Must be threadsafe. Access to 'perFsFileBlocks' and 'hostIndex' must be protected.
   */
  private static void loadBlockMetadata(FileSystem fs, FileStatus file, FileDescriptor fd,
      HdfsFileFormat fileFormat, Map<FsKey, FileBlocksInfo> perFsFileBlocks,
      String tblName, ListMap<TNetworkAddress> hostIndex) {
    Preconditions.checkNotNull(fd);
    Preconditions.checkNotNull(perFsFileBlocks);
    Preconditions.checkArgument(!file.isDirectory());
    LOG.debug("load block md for " + tblName + " file " + fd.getFileName());

    if (!FileSystemUtil.hasGetFileBlockLocations(fs)) {
      synthesizeBlockMetadata(file, fd, fileFormat, hostIndex);
      return;
    }
    try {
      BlockLocation[] locations = null;
      if (file instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) file).getBlockLocations();
      } else {
        locations = fs.getFileBlockLocations(file, 0, file.getLen());
      }
      Preconditions.checkNotNull(locations);

      // Loop over all blocks in the file.
      for (BlockLocation loc: locations) {
        Preconditions.checkNotNull(loc);
        fd.addFileBlock(createFileBlock(loc, hostIndex));
      }

      // Remember the THdfsFileBlocks and corresponding BlockLocations. Once all the
      // blocks are collected, the disk IDs will be queried in one batch per filesystem.
      FsKey fsKey = new FsKey(fs);
      synchronized (perFsFileBlocks) {
        FileBlocksInfo infos = perFsFileBlocks.get(fsKey);
        if (infos == null) {
          infos = new FileBlocksInfo();
          perFsFileBlocks.put(fsKey, infos);
        }
        infos.addBlocks(fd.getFileBlocks(), Arrays.asList(locations));
      }
    } catch (IOException e) {
      throw new RuntimeException("couldn't determine block locations for path '" +
          file.getPath() + "':\n" + e.getMessage(), e);
    }
  }

  /**
   * For filesystems that don't override getFileBlockLocations, synthesize file blocks by
   * manually splitting the file range into fixed-size blocks. That way, scan ranges can
   * be derived from file blocks as usual. All synthesized blocks are given an invalid
   * network address so that the scheduler will treat them as remote.
   *
   * Must be threadsafe. Access to 'hostIndex' must be protected.
   */
  private static void synthesizeBlockMetadata(FileStatus file, FileDescriptor fd,
      HdfsFileFormat fileFormat, ListMap<TNetworkAddress> hostIndex) {
    long start = 0;
    long remaining = fd.getFileLength();
    long blockSize = file.getBlockSize();
    if (blockSize < MIN_SYNTHETIC_BLOCK_SIZE) blockSize = MIN_SYNTHETIC_BLOCK_SIZE;
    if (!fileFormat.isSplittable(HdfsCompression.fromFileName(fd.getFileName()))) {
      blockSize = remaining;
    }
    while (remaining > 0) {
      long len = Math.min(remaining, blockSize);
      int idx = -1;
      synchronized (hostIndex) {
        idx = hostIndex.getIndex(REMOTE_NETWORK_ADDRESS);
      }
      List<BlockReplica> replicas = Lists.newArrayList(new BlockReplica(idx, false));
      fd.addFileBlock(new FileBlock(start, len, replicas));
      remaining -= len;
      start += len;
    }
  }
}
