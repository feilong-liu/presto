/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import it.unimi.dsi.fastutil.HashCommon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;

// Can I assume that only one driver will be executed per each task?
public class StarJoinPageIndex
{
    private static final Logger log = Logger.get(StarJoinPageIndex.class);

    private final PagesIndex.Factory pagesIndexFactory;
    private final PagesIndex[][] pagesIndex;
    private final int mask;
    private final List<List<PagesHashStrategy>> pagesHashStrategy;
    private final List<List<List<Integer>>> hashChannels;
    private final List<List<List<List<Block>>>> channels;
    private final List<List<byte[]>> positionToHashes;
    // Now let me fix the size, we need to think about how to set the size later
    int[][][] key;
    boolean[] hasData;
    private final int tableNum;

    public StarJoinPageIndex(PagesIndex.Factory pagesIndexFactory, int buildTableNum, int partitionNum, int hashTableSize)
    {
        this.pagesIndexFactory = pagesIndexFactory;

        log.info("partitionNum " + partitionNum + " buildTableNum " + buildTableNum);
        this.pagesIndex = new PagesIndex[partitionNum][buildTableNum];
        int hashSize = HashCommon.arraySize(hashTableSize, 0.75f);
        log.info("hashSize " + hashSize);
        mask = hashSize - 1;
        key = new int[hashSize][partitionNum][buildTableNum];
        log.info("key address " + key);
        for (int i = 0; i < hashSize; ++i) {
            for (int j = 0; j < partitionNum; ++j) {
                Arrays.fill(key[i][j], -1);
            }
        }
        hasData = new boolean[hashSize];
        Arrays.fill(hasData, false);
        pagesHashStrategy = new ArrayList<>(partitionNum);
        for (int i = 0; i < partitionNum; ++i) {
            List<PagesHashStrategy> pagesHashStrategyList = new ArrayList<>(buildTableNum);
            for (int j = 0; j < buildTableNum; ++j) {
                pagesHashStrategyList.add(null);
            }
            pagesHashStrategy.add(pagesHashStrategyList);
        }
        hashChannels = new ArrayList<>(partitionNum);
        for (int i = 0; i < partitionNum; ++i) {
            List<List<Integer>> temp = new ArrayList<>(buildTableNum);
            for (int j = 0; j < buildTableNum; ++j) {
                temp.add(null);
            }
            hashChannels.add(temp);
        }
        channels = new ArrayList<>(partitionNum);
        for (int i = 0; i < partitionNum; ++i) {
            List<List<List<Block>>> temp = new ArrayList<>(buildTableNum);
            for (int j = 0; j < buildTableNum; ++j) {
                temp.add(null);
            }
            channels.add(temp);
        }
        positionToHashes = new ArrayList<>(partitionNum);
        for (int i = 0; i < partitionNum; ++i) {
            List<byte[]> temp = new ArrayList<>(buildTableNum);
            for (int j = 0; j < buildTableNum; ++j) {
                temp.add(null);
            }
            positionToHashes.add(temp);
        }
        this.tableNum = buildTableNum;
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }

    public void setPositionToHashes(int partition, int table, byte[] hash)
    {
        positionToHashes.get(partition).set(table, hash);
        return;
    }

    public void setChannels(int partition, int table, List<List<Block>> channel)
    {
        channels.get(partition).set(table, channel);
        return;
    }

    public void setHashChannels(int partition, int table, List<Integer> hashChannel)
    {
        hashChannels.get(partition).set(table, hashChannel);
        return;
    }

    public void setPagesHashStrategy(int partition, int table, PagesHashStrategy pagesHashStrategy)
    {
        this.pagesHashStrategy.get(partition).set(table, pagesHashStrategy);
        return;
    }

    public PagesIndex newPagesIndex(int table, int partition, List<Type> types, int expectedPositions)
    {
        PagesIndex newPagesIndex = pagesIndexFactory.newPagesIndex(types, expectedPositions);
        log.info("newPagesIndex partition " + partition + " table " + table);
        this.pagesIndex[partition][table] = newPagesIndex;
        return newPagesIndex;
    }

    public void populate(int hashTableIndex, int partitionIndex, byte[] positionToHashes, int positionsInStep, int positionCount, PositionLinks.FactoryBuilder positionLinks)
    {
        long[] positionToFullHashes = new long[positionsInStep];
        for (int step = 0; step * positionsInStep <= positionCount; step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, positionCount);
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition, partitionIndex, hashTableIndex);
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition, partitionIndex, hashTableIndex)) {
                    continue;
                }

                long hash = positionToFullHashes[position];
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                long hashCollisionsLocal = 0;
                while (true) {
                    int tableIndex = getTableIndex(pos, partitionIndex);
                    if (tableIndex == -1) {
                        break;
                    }
                    int currentKey = key[pos][partitionIndex][tableIndex];
                    if (((byte) hash) == this.positionToHashes.get(partitionIndex).get(tableIndex)[currentKey] && positionEqualsPositionIgnoreNulls(
                            partitionIndex, hashTableIndex, realPosition, partitionIndex, tableIndex, currentKey)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, key[pos][partitionIndex][hashTableIndex]);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                    if (hashCollisionsLocal >= key.length) {
                        log.info("input build page index size for partition " + partitionIndex);
                        for (int i = 0; i < tableNum; ++i) {
                            log.info("table " + i + " " + pagesIndex[partitionIndex][i]);
                        }
                        checkState(false, "starjoin: hash table cannot hold all build keys");
                    }
                }

                key[pos][partitionIndex][hashTableIndex] = realPosition;
                hasData[pos] = true;
            }
        }
    }

    private int getTableIndex(int pos, int partitionIndex)
    {
        if (!hasData[pos]) {
            return -1;
        }
        for (int i = 0; i < tableNum; ++i) {
            if (key[pos][partitionIndex][i] != -1) {
                return i;
            }
        }
        return -1;
    }

    private long readHashPosition(int position, int partitionIndex, int hashTableIndex)
    {
        long pageAddress = pagesIndex[partitionIndex][hashTableIndex].getValueAddresses().get(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.get(partitionIndex).get(hashTableIndex).hashPosition(blockIndex, blockPosition);
    }

    private boolean isPositionNull(int position, int partitionIndex, int hashTableIndex)
    {
        long pageAddress = pagesIndex[partitionIndex][hashTableIndex].getValueAddresses().get(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.get(partitionIndex).get(hashTableIndex).isPositionNull(blockIndex, blockPosition);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPartition, int leftHashTable, int leftPosition, int rightPartition, int rightHashTable, int rightPosition)
    {
        long leftPageAddress = pagesIndex[leftPartition][leftHashTable].getValueAddresses().get(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = pagesIndex[rightPartition][rightHashTable].getValueAddresses().get(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return positionEqualsPositionIgnoreNulls(leftPartition, leftHashTable, leftBlockIndex, leftBlockPosition,
                rightPartition, rightHashTable, rightBlockIndex, rightBlockPosition);
    }

    public boolean positionEqualsPositionIgnoreNulls(int leftPartition, int leftHashTable, int leftBlockIndex, int leftPosition,
            int rightPartition, int rightHashTable, int rightBlockIndex, int rightPosition)
    {
        int hashChannelSize = hashChannels.get(leftPartition).get(leftHashTable).size();
        for (int i = 0; i < hashChannelSize; ++i) {
            int leftHashChannel = hashChannels.get(leftPartition).get(leftHashTable).get(i);
            int rightHashChannel = hashChannels.get(rightPartition).get(rightHashTable).get(i);
            Type type = pagesIndex[leftPartition][leftHashTable].getTypes().get(leftHashChannel);
            List<Block> leftChannel = channels.get(leftPartition).get(leftHashTable).get(leftHashChannel);
            List<Block> rightChannel = channels.get(rightPartition).get(rightHashTable).get(rightHashChannel);
            Block leftBlock = leftChannel.get(leftBlockIndex);
            Block rightBlock = rightChannel.get(rightBlockIndex);
            try {
                if (!type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition)) {
                    return false;
                }
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }
        }
        return true;
    }

    public int[] getAddressIndex(int partition, int hashtable, int position, Page hashChannelsPage)
    {
        return getAddressIndex(partition, hashtable, position, hashChannelsPage, pagesHashStrategy.get(partition).get(hashtable).hashRow(position, hashChannelsPage));
    }

    public int[] getAddressIndex(int partitionIndex, int hashtable, int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);
        while (hasData[pos]) {
            int tableIndex = getTableIndex(pos, partitionIndex);
            if (tableIndex == -1) {
                break;
            }
            if (positionEqualsCurrentRowIgnoreNulls(partitionIndex, tableIndex, key[pos][partitionIndex][tableIndex], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return key[pos][partitionIndex];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return null;
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int partition, int hashtable, int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes.get(partition).get(hashtable)[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = pagesIndex[partition][hashtable].getValueAddresses().get(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.get(partition).get(hashtable).positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }
}
