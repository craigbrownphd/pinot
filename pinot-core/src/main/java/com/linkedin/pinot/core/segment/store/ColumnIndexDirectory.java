/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.segment.store;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;


/**
 * Abstract class to map the columnar indices to their location on disk.
 *
 */
abstract class ColumnIndexDirectory implements AutoCloseable {

  protected File segmentDirectory;
  protected SegmentMetadataImpl metadata;
  protected ReadMode readMode;

  /**
   * @param segmentDirectory File pointing to segment directory
   * @param metadata segment metadata. Metadata must be fully initialized
   * @param readMode mmap vs heap map mode
   */
  protected ColumnIndexDirectory(File segmentDirectory, SegmentMetadataImpl metadata, ReadMode readMode) {
    Preconditions.checkNotNull(segmentDirectory);
    Preconditions.checkNotNull(readMode);
    Preconditions.checkNotNull(metadata);

    Preconditions.checkArgument(segmentDirectory.exists(),
        "SegmentDirectory: " + segmentDirectory.toString() + " does not exist");
    Preconditions.checkArgument(segmentDirectory.isDirectory(),
        "SegmentDirectory: " + segmentDirectory.toString() + " is not a directory");


    this.segmentDirectory = segmentDirectory;
    this.metadata = metadata;
    this.readMode = readMode;
  }

  /**
   * Allocation context for tracking memory
   * @param f file for which memory is allocatedx
   * @param context additional context string
   * @return formatted string for memory tracking
   */
  protected String allocationContext(File f, String context)  {
    return this.getClass().getSimpleName() + "." + f.toString() + "." + context;
  }

  /**
   * Get dictionary data buffer for a column
   * @param column column name
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer getDictionaryBufferFor(String column)
      throws IOException;
  /**
   * Get forward index data buffer for a column
   * @param column column name
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer getForwardIndexBufferFor(String column)
      throws IOException;
  /**
   * Get inverted index data buffer for a column
   * @param column column name
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException;

  /**
   * Allocate a new data buffer of specified size in the columnar index directory
   * @param column column name
   * @param size size for the buffer allocation
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer newDictionaryBuffer(String column, int size)
      throws IOException;
  /**
   * Allocate a new data buffer of specified size in the columnar index directory
   * @param column column name
   * @param size size for the buffer allocation
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer newForwardIndexBuffer(String column, int size)
      throws IOException;
  /**
   * Allocate a new data buffer of specified size in the columnar index directory
   * @param column column name
   * @param size size for the buffer allocation
   * @return in-memory ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer newInvertedIndexBuffer(String column, int size)
      throws IOException;

  /**
   * Check if an index exists for a column
   * @param column column name
   * @param type index type
   * @return true if the index exists; false otherwise
   */
  public abstract boolean hasIndexFor(String column, ColumnIndexType type);

  @Override
  public abstract void close();
}