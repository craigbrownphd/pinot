package com.linkedin.pinot.core.block.intarray;

import java.util.Arrays;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class DocIdSetBlock implements Block {

  private IndexSegment _indexSegment;
  private int[] _docIdSet;
  private int _searchableLength;

  public DocIdSetBlock(IndexSegment indexSegment, int[] docIdSet, int searchableLength) {
    _indexSegment = indexSegment;
    _docIdSet = docIdSet;
    _searchableLength = searchableLength;
  }

  public int[] getDocIdSet() {
    return _docIdSet;
  }

  public int getSearchableLength() {
    return _searchableLength;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return true;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
    //    return new BlockValSet() {
    //      @Override
    //      public BlockValIterator iterator() {
    //        return new ColumnarReaderBlockValIterator(_columnarReader, _docIdSet, _searchableLength);
    //      }
    //    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          int _pos = 0;

          @Override
          public int skipTo(int targetDocId) {
            _pos = Arrays.binarySearch(_docIdSet, targetDocId);
            if (_pos < 0) {
              _pos = (_pos + 1) * -1;
            }
            return _docIdSet[_pos];
          }

          @Override
          public int next() {
            if (_pos == _searchableLength) {
              return Constants.EOF;
            }
            return _docIdSet[_pos++];
          }

          @Override
          public int currentDocId() {
            return _docIdSet[_pos];
          }
        };
      }
    };
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetBlock() {
    throw new UnsupportedOperationException();
  }

}
