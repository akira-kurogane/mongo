/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "mongo/platform/basic.h"

#include "mongo/s/chunk_manager.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_writes_tracker.h"
#include "mongo/s/chunks_test_util.h"
#include "mongo/s/mongos_server_parameters_gen.h"
#include "mongo/s/shard_server_test_fixture.h"

#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"


namespace mongo {

using chunks_test_util::assertEqualChunkInfo;
using chunks_test_util::calculateCollVersion;
using chunks_test_util::calculateIntermediateShardKey;
using chunks_test_util::calculateShardVersions;
using chunks_test_util::genChunkVector;
using chunks_test_util::genRandomChunkVector;
using chunks_test_util::getShardId;
using chunks_test_util::performRandomChunkOperations;

namespace {

PseudoRandom _random{SecureRandom().nextInt64()};

const ShardId kThisShard("thisShard");
const NamespaceString kNss("TestDB", "TestColl");

template <class ParamType>
class ScopedServerParameterChange {
public:
    ScopedServerParameterChange(ParamType* param, ParamType newValue)
        : _param(param), _originalValue(*_param) {
        *param = newValue;
    }

    ~ScopedServerParameterChange() {
        *_param = _originalValue;
    }

private:
    ParamType* const _param;
    const ParamType _originalValue;
};

/**
 * Creates a new routing table from the input routing table by inserting the chunks specified by
 * newChunkBoundaryPoints.  newChunkBoundaryPoints specifies a contiguous array of keys indicating
 * chunk boundaries to be inserted. As an example, if you want to split the range [0, 2] into chunks
 * [0, 1] and [1, 2], newChunkBoundaryPoints should be [0, 1, 2].
 */
std::shared_ptr<RoutingTableHistory> splitChunk(
    const std::shared_ptr<RoutingTableHistory>& rt,
    const std::vector<BSONObj>& newChunkBoundaryPoints) {

    // Convert the boundary points into chunk range objects, e.g. {0, 1, 2} ->
    // {{ChunkRange{0, 1}, ChunkRange{1, 2}}
    std::vector<ChunkRange> newChunkRanges;
    invariant(newChunkBoundaryPoints.size() > 1);
    for (size_t i = 0; i < newChunkBoundaryPoints.size() - 1; ++i) {
        newChunkRanges.emplace_back(newChunkBoundaryPoints[i], newChunkBoundaryPoints[i + 1]);
    }

    std::vector<ChunkType> newChunks;
    auto curVersion = rt->getVersion();

    for (const auto& range : newChunkRanges) {
        // Chunks must be inserted ordered by version
        curVersion.incMajor();
        newChunks.emplace_back(kNss, range, curVersion, kThisShard);
    }
    return rt->makeUpdated(newChunks);
}

/**
 * Gets a set of raw pointers to ChunkInfo objects in the specified range,
 */
std::set<ChunkInfo*> getChunksInRange(std::shared_ptr<RoutingTableHistory> rt,
                                      const BSONObj& min,
                                      const BSONObj& max) {
    std::set<ChunkInfo*> chunksFromSplit;

    rt->forEachOverlappingChunk(min, max, false, [&](auto& chunk) {
        chunksFromSplit.insert(chunk.get());
        return true;
    });

    return chunksFromSplit;
}

// Looks up a chunk that corresponds to or contains the range [min, max). There
// should only be one such chunk in the input RoutingTableHistory object.
std::shared_ptr<ChunkInfo> getChunkToSplit(std::shared_ptr<RoutingTableHistory> rt,
                                           const BSONObj& min,
                                           const BSONObj& max) {
    std::shared_ptr<ChunkInfo> firstOverlappingChunk;

    rt->forEachOverlappingChunk(min, max, false, [&](auto& chunkInfo) {
        firstOverlappingChunk = chunkInfo;
        return false;  // only need first chunk
    });

    invariant(firstOverlappingChunk);

    return firstOverlappingChunk;
}

/**
 * Helper function for testing the results of a chunk split.
 *
 * Finds the chunks in a routing table resulting from a split on the range [minSplitBoundary,
 * maxSplitBoundary). Checks that the correct number of chunks are in the routing table for the
 * corresponding range. To check that the bytes written have been correctly propagated from the
 * chunk being split to the chunks resulting from the split, we check:
 *
 *      For each chunk:
 *          If the chunk was a result of the split:
 *              Make sure it has expectedBytesInChunksFromSplit bytes in its writes tracker
 *          Else:
 *              Make sure its bytes written have not been changed due to the split (e.g. it has
 *              expectedBytesInChunksNotSplit in its writes tracker)
 *
 */
void assertCorrectBytesWritten(std::shared_ptr<RoutingTableHistory> rt,
                               const BSONObj& minSplitBoundary,
                               const BSONObj& maxSplitBoundary,
                               size_t expectedNumChunksFromSplit,
                               uint64_t expectedBytesInChunksFromSplit,
                               uint64_t expectedBytesInChunksNotSplit) {
    auto chunksFromSplit = getChunksInRange(rt, minSplitBoundary, maxSplitBoundary);
    ASSERT_EQ(chunksFromSplit.size(), expectedNumChunksFromSplit);

    rt->forEachChunk([&](const auto& chunkInfo) {
        auto writesTracker = chunkInfo->getWritesTracker();
        auto bytesWritten = writesTracker->getBytesWritten();
        if (chunksFromSplit.count(chunkInfo.get()) > 0) {
            ASSERT_EQ(bytesWritten, expectedBytesInChunksFromSplit);
        } else {
            ASSERT_EQ(bytesWritten, expectedBytesInChunksNotSplit);
        }

        return true;
    });
}

class RoutingTableHistoryTest : public unittest::Test {
public:
    const KeyPattern& getShardKeyPattern() const {
        return _shardKeyPattern;
    }

    const OID& collEpoch() const {
        return _epoch;
    }

    const UUID& collUUID() const {
        return _collUUID;
    }

    std::vector<ChunkType> genRandomChunkVector(size_t minNumChunks = 1,
                                                size_t maxNumChunks = 30) const {
        return chunks_test_util::genRandomChunkVector(kNss, _epoch, maxNumChunks, minNumChunks);
    }

    std::shared_ptr<RoutingTableHistory> makeNewRt(const std::vector<ChunkType>& chunks) const {
        const auto chunkBucketSize = llround(_random.nextInt64(chunks.size() * 1.2)) + 1;
        LOGV2(7162710,
              "Creating new RoutingTable",
              "chunkBucketSize"_attr = chunkBucketSize,
              "numChunks"_attr = chunks.size());
        ScopedServerParameterChange chunkBucketSizeParameter{&gRoutingTableCacheChunkBucketSize,
                                                             chunkBucketSize};
        return RoutingTableHistory::makeNew(
            kNss, _collUUID, _shardKeyPattern, nullptr, false, _epoch, chunks);
    }

protected:
    KeyPattern _shardKeyPattern{chunks_test_util::kShardKeyPattern};
    const OID _epoch{OID::gen()};
    const UUID _collUUID{UUID::gen()};
};

/**
 * Test fixture for tests that need to start with three chunks in it, with the
 * same number of bytes written to every chunk object.
 */
class RoutingTableHistoryTestThreeInitialChunks : public RoutingTableHistoryTest {
public:
    void setUp() override {
        RoutingTableHistoryTest::setUp();

        _initialChunkBoundaryPoints = {getShardKeyPattern().globalMin(),
                                       BSON("a" << 10),
                                       BSON("a" << 20),
                                       getShardKeyPattern().globalMax()};
        ChunkVersion version{1, 0, collEpoch()};
        auto chunks = genChunkVector(kNss, _initialChunkBoundaryPoints, version, 1 /* numShards */);

        _rt = makeNewRt(chunks);

        ASSERT_EQ(_rt->numChunks(), 3ull);

        _rt->forEachChunk([&](const auto& chunkInfo) {
            auto writesTracker = chunkInfo->getWritesTracker();
            writesTracker->addBytesWritten(_bytesInOriginalChunk);
            return true;
        });
    }

    uint64_t getBytesInOriginalChunk() const {
        return _bytesInOriginalChunk;
    }

    const std::shared_ptr<RoutingTableHistory>& getInitialRoutingTable() const {
        return _rt;
    }

    std::vector<BSONObj> getInitialChunkBoundaryPoints() {
        return _initialChunkBoundaryPoints;
    }

private:
    uint64_t _bytesInOriginalChunk{4ull};

    std::shared_ptr<RoutingTableHistory> _rt;

    std::vector<BSONObj> _initialChunkBoundaryPoints;
};

/*
 * Test creation of a Routing Table with randomly generated chunks
 */
TEST_F(RoutingTableHistoryTest, RandomCreateBasic) {
    const auto chunks = genRandomChunkVector();
    const auto expectedShardVersions = calculateShardVersions(chunks);
    const auto expectedCollVersion = calculateCollVersion(expectedShardVersions);

    // Create a new routing table from the randomly generated chunks
    auto rt = makeNewRt(chunks);

    // Checks basic getter of routing table return correct values
    ASSERT_EQ(kNss, rt->getns());
    ASSERT_EQ(ShardKeyPattern(getShardKeyPattern()).toString(),
              rt->getShardKeyPattern().toString());
    ASSERT_EQ(chunks.size(), rt->numChunks());

    // Check that chunks have correct info
    size_t i = 0;
    rt->forEachChunk([&](const auto& chunkInfo) {
        assertEqualChunkInfo(ChunkInfo{chunks[i++]}, *chunkInfo);
        return true;
    });
    ASSERT_EQ(i, chunks.size());

    // Checks collection version is correct
    ASSERT_EQ(expectedCollVersion, rt->getVersion());

    // Checks version for each chunk
    for (const auto& [shardId, shardVersion] : expectedShardVersions) {
        ASSERT_EQ(shardVersion, rt->getVersion(shardId));
    }

    ASSERT_EQ(expectedShardVersions.size(), rt->getNShardsOwningChunks());

    std::set<ShardId> expectedShardIds;
    for (const auto& [shardId, shardVersion] : expectedShardVersions) {
        expectedShardIds.insert(shardId);
    }
    std::set<ShardId> shardIds;
    rt->getAllShardIds(&shardIds);
    ASSERT(expectedShardIds == shardIds);
}

/*
 * Test that creation of Routing Table with chunks that do not cover the entire shard key space
 * fails.
 *
 * The gap is produced by removing a random chunks from the randomly generated chunk list. Thus it
 * also cover the case for which min/max key is missing.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateWithMissingChunkFail) {
    auto chunks = genRandomChunkVector(2 /*minNumChunks*/);

    // Remove one random chunk to simulate a gap in the shardkey
    chunks.erase(chunks.begin() + _random.nextInt64(chunks.size()));

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Test that creation of Routing Table with chunks that do not cover the entire shard key space
 * fails.
 *
 * The gap is produced by shrinking the range of a random chunk.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateWithChunkGapFail) {
    auto chunks = genRandomChunkVector(2 /*minNumChunks*/);

    auto& shrinkedChunk = chunks.at(_random.nextInt64(chunks.size()));
    auto intermediateKey =
        calculateIntermediateShardKey(shrinkedChunk.getMin(), shrinkedChunk.getMax());
    if (_random.nextInt64(2)) {
        // Shrink right bound
        shrinkedChunk.setMax(intermediateKey);
    } else {
        // Shrink left bound
        shrinkedChunk.setMin(intermediateKey);
    }

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Updating ChunkMap with gaps must fail
 */
TEST_F(RoutingTableHistoryTest, RandomUpdateWithChunkGapFail) {
    auto chunks = genRandomChunkVector();

    // Create a new routing table from the randomly generated chunks
    auto rt = makeNewRt(chunks);
    auto collVersion = rt->getVersion();

    auto shrinkedChunk = chunks.at(_random.nextInt64(chunks.size()));
    auto intermediateKey =
        calculateIntermediateShardKey(shrinkedChunk.getMin(), shrinkedChunk.getMax());
    if (_random.nextInt64(2)) {
        // Shrink right bound
        shrinkedChunk.setMax(intermediateKey);
    } else {
        // Shrink left bound
        shrinkedChunk.setMin(intermediateKey);
    }

    // Bump chunk version
    collVersion.incMajor();
    shrinkedChunk.setVersion(collVersion);

    ASSERT_THROWS_CODE(rt->makeUpdated({std::move(shrinkedChunk)}),
                       AssertionException,
                       ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Creating a Routing Table with overlapping chunks must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateWithChunkOverlapFail) {
    auto chunks = genRandomChunkVector(2 /* minNumChunks */);

    auto chunkToExtendIt = chunks.begin() + _random.nextInt64(chunks.size());

    const auto canExtendLeft = chunkToExtendIt > chunks.begin();
    const auto extendRight =
        !canExtendLeft || ((chunkToExtendIt < std::prev(chunks.end())) && _random.nextInt64(2));
    const auto extendLeft = !extendRight;
    if (extendRight) {
        // extend right bound
        chunkToExtendIt->setMax(calculateIntermediateShardKey(chunkToExtendIt->getMax(),
                                                              std::next(chunkToExtendIt)->getMax(),
                                                              0.0 /* minKeyProb */,
                                                              0.1 /* maxKeyProb */));
        auto newVersion = chunkToExtendIt->getVersion();
        newVersion.incMajor();
        std::next(chunkToExtendIt)->setVersion(newVersion);
    }

    if (extendLeft) {
        invariant(canExtendLeft);
        // extend left bound
        chunkToExtendIt->setMin(calculateIntermediateShardKey(std::prev(chunkToExtendIt)->getMin(),
                                                              chunkToExtendIt->getMin(),
                                                              0.1 /* minKeyProb */,
                                                              0.0 /* maxKeyProb */));
        auto newVersion = chunkToExtendIt->getVersion();
        newVersion.incMajor();
        std::prev(chunkToExtendIt)->setVersion(newVersion);
    }

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Updating a ChunkMap with overlapping chunks must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomUpdateWithChunkOverlapFail) {
    auto chunks = genRandomChunkVector(2 /* minNumChunks */);

    // Create a new routing table from the randomly generated chunks
    auto rt = makeNewRt(chunks);

    auto collVersion = rt->getVersion();

    auto chunkToExtendIt = chunks.begin() + _random.nextInt64(chunks.size());

    const auto canExtendLeft = chunkToExtendIt > chunks.begin();
    const auto extendRight =
        !canExtendLeft || (chunkToExtendIt < std::prev(chunks.end()) && _random.nextInt64(2));
    const auto extendLeft = !extendRight;
    if (extendRight) {
        // extend right bound
        chunkToExtendIt->setMax(calculateIntermediateShardKey(
            chunkToExtendIt->getMax(), std::next(chunkToExtendIt)->getMax()));
    }

    if (extendLeft) {
        invariant(canExtendLeft);
        // extend left bound
        chunkToExtendIt->setMin(calculateIntermediateShardKey(std::prev(chunkToExtendIt)->getMin(),
                                                              chunkToExtendIt->getMin()));
    }

    // Bump chunk version
    collVersion.incMajor();
    chunkToExtendIt->setVersion(collVersion);

    ASSERT_THROWS_CODE(rt->makeUpdated({*chunkToExtendIt}),
                       AssertionException,
                       ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Creating a Routing Table with wrong min key must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateWrongMinFail) {
    auto chunks = genRandomChunkVector();

    chunks.begin()->setMin(BSON("a" << std::numeric_limits<int64_t>::min()));

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Creating a Routing Table with wrong max key must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateWrongMaxFail) {
    auto chunks = genRandomChunkVector();

    chunks.begin()->setMax(BSON("a" << std::numeric_limits<int64_t>::max()));

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Creating a Routing Table with mismatching epoch must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomCreateMismatchingEpochFail) {
    auto chunks = genRandomChunkVector();

    // Change epoch on a random chunk
    auto chunkIt = chunks.begin() + _random.nextInt64(chunks.size());
    const auto& oldVersion = chunkIt->getVersion();
    ChunkVersion newVersion{oldVersion.majorVersion(), oldVersion.minorVersion(), OID::gen()};
    chunkIt->setVersion(newVersion);

    // Create a new routing table from the randomly generated chunks
    ASSERT_THROWS_CODE(makeNewRt(chunks), DBException, ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Updating a Routing Table with mismatching epoch must fail.
 */
TEST_F(RoutingTableHistoryTest, RandomUpdateMismatchingEpochFail) {
    auto chunks = genRandomChunkVector();

    // Create a new routing table from the randomly generated chunks
    auto rt = makeNewRt(chunks);

    // Change epoch on a random chunk
    auto chunkIt = chunks.begin() + _random.nextInt64(chunks.size());
    const auto& oldVersion = chunkIt->getVersion();
    ChunkVersion newVersion{oldVersion.majorVersion(), oldVersion.minorVersion(), OID::gen()};
    chunkIt->setVersion(newVersion);

    ASSERT_THROWS_CODE(rt->makeUpdated({*chunkIt}),
                       AssertionException,
                       ErrorCodes::ConflictingOperationInProgress);
}

/*
 * Test update of the Routing Table with randomly generated changed chunks.
 */
TEST_F(RoutingTableHistoryTest, RandomUpdate) {
    auto initialChunks = genRandomChunkVector();

    const auto initialShardVersions = calculateShardVersions(initialChunks);
    const auto initialCollVersion = calculateCollVersion(initialShardVersions);

    // Create a new routing table from the randomly generated initialChunks
    auto initialRt = makeNewRt(initialChunks);

    auto chunks = initialChunks;
    const auto maxNumChunkOps = 2 * initialChunks.size();
    const auto numChunkOps = _random.nextInt32(maxNumChunkOps);

    performRandomChunkOperations(&chunks, numChunkOps);

    std::vector<ChunkType> updatedChunks;
    for (const auto& chunk : chunks) {
        if (initialCollVersion.isOlderThan(chunk.getVersion())) {
            updatedChunks.push_back(chunk);
        }
    }

    const auto expectedShardVersions = calculateShardVersions(chunks);
    const auto expectedCollVersion = calculateCollVersion(expectedShardVersions);

    auto rt = initialRt->makeUpdated(updatedChunks);

    // Checks basic getter of routing table return correct values
    ASSERT_EQ(kNss, rt->getns());
    ASSERT_EQ(ShardKeyPattern(getShardKeyPattern()).toString(),
              rt->getShardKeyPattern().toString());
    ASSERT_EQ(chunks.size(), rt->numChunks());

    // Check that chunks have correct info
    size_t i = 0;
    rt->forEachChunk([&](const auto& chunkInfo) {
        assertEqualChunkInfo(ChunkInfo{chunks[i++]}, *chunkInfo);
        return true;
    });
    ASSERT_EQ(i, chunks.size());

    // Checks collection version is correct
    ASSERT_EQ(expectedCollVersion, rt->getVersion());

    // Checks version for each shard
    for (const auto& [shardId, shardVersion] : expectedShardVersions) {
        ASSERT_EQ(shardVersion, rt->getVersion(shardId));
    }

    ASSERT_EQ(expectedShardVersions.size(), rt->getNShardsOwningChunks());

    std::set<ShardId> expectedShardIds;
    for (const auto& [shardId, shardVersion] : expectedShardVersions) {
        expectedShardIds.insert(shardId);
    }
    std::set<ShardId> shardIds;
    rt->getAllShardIds(&shardIds);
    ASSERT(expectedShardIds == shardIds);
}

/*
 * Test update of the Routing Table with randomly generated changed chunks.
 */
TEST_F(RoutingTableHistoryTest, SequenceNumberIsMonotonicallyIncreasing) {
    auto initialChunks = genRandomChunkVector();
    auto initialRt = makeNewRt(initialChunks);

    auto initialChunks2 = genRandomChunkVector();
    auto initialRt2 = makeNewRt(initialChunks);
    ASSERT_GT(initialRt2->getSequenceNumber(), initialRt->getSequenceNumber());


    auto rt = initialRt->makeUpdated({});
    ASSERT_EQ(rt->getSequenceNumber(), initialRt->getSequenceNumber());

    const auto initialCollVersion = calculateCollVersion(calculateShardVersions(initialChunks));
    auto updatedChunk = initialChunks[_random.nextInt32(initialChunks.size())];
    auto updatedVersion = initialCollVersion;
    updatedVersion.incMinor();
    updatedChunk.setVersion(updatedVersion);

    rt = initialRt->makeUpdated({updatedChunk});
    ASSERT_GT(rt->getSequenceNumber(), initialRt->getSequenceNumber());
    ASSERT_GT(rt->getSequenceNumber(), initialRt2->getSequenceNumber());
}

TEST_F(RoutingTableHistoryTest, SplittingOnlyChunkCopiesBytesWrittenToAllSubchunks) {
    ChunkVersion version{1, 0, collEpoch()};

    const ChunkType initialChunk{
        kNss,
        ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
        version,
        kThisShard};

    auto rt = makeNewRt({initialChunk});
    ASSERT_EQ(1, rt->numChunks());

    // Set the 4 written bytes in each chunk
    const size_t bytesInOriginalChunk{4};
    rt->forEachChunk([&](const auto& chunkInfo) {
        chunkInfo->getWritesTracker()->addBytesWritten(bytesInOriginalChunk);
        return true;
    });

    version.incMinor();
    auto newChunks = genChunkVector(kNss,
                                    {getShardKeyPattern().globalMin(),
                                     BSON("a" << 10),
                                     BSON("a" << 20),
                                     getShardKeyPattern().globalMax()},
                                    version,
                                    1 /*numShards*/);
    auto newRt = rt->makeUpdated(newChunks);
    ASSERT_EQ(3, newRt->numChunks());

    rt->forEachChunk([&](const auto& chunkInfo) {
        ASSERT_EQ(bytesInOriginalChunk, chunkInfo->getWritesTracker()->getBytesWritten());
        return true;
    });
}

TEST_F(RoutingTableHistoryTestThreeInitialChunks,
       SplittingFirstChunkOfSeveralCopiesBytesWrittenToAllSubchunks) {
    auto minKey = getInitialChunkBoundaryPoints()[0];
    auto maxKey = getInitialChunkBoundaryPoints()[1];
    std::vector<BSONObj> newChunkBoundaryPoints = {minKey, BSON("a" << 5), maxKey};

    auto chunkToSplit = getChunkToSplit(getInitialRoutingTable(), minKey, maxKey);
    auto bytesToWrite = 5ull;
    chunkToSplit->getWritesTracker()->addBytesWritten(bytesToWrite);

    // Split first chunk into two
    auto rt = splitChunk(getInitialRoutingTable(), newChunkBoundaryPoints);

    auto expectedNumChunksFromSplit = 2;
    auto expectedBytesInChunksFromSplit = getBytesInOriginalChunk() + bytesToWrite;
    auto expectedBytesInChunksNotSplit = getBytesInOriginalChunk();
    ASSERT_EQ(rt->numChunks(), 4ull);
    assertCorrectBytesWritten(rt,
                              minKey,
                              maxKey,
                              expectedNumChunksFromSplit,
                              expectedBytesInChunksFromSplit,
                              expectedBytesInChunksNotSplit);
}


TEST_F(RoutingTableHistoryTestThreeInitialChunks,
       SplittingMiddleChunkOfSeveralCopiesBytesWrittenToAllSubchunks) {
    auto minKey = getInitialChunkBoundaryPoints()[1];
    auto maxKey = getInitialChunkBoundaryPoints()[2];
    auto newChunkBoundaryPoints = {minKey, BSON("a" << 16), BSON("a" << 17), maxKey};

    auto chunkToSplit = getChunkToSplit(getInitialRoutingTable(), minKey, maxKey);
    auto bytesToWrite = 5ull;
    chunkToSplit->getWritesTracker()->addBytesWritten(bytesToWrite);

    // Split middle chunk into three
    auto rt = splitChunk(getInitialRoutingTable(), newChunkBoundaryPoints);

    auto expectedNumChunksFromSplit = 3;
    auto expectedBytesInChunksFromSplit = getBytesInOriginalChunk() + bytesToWrite;
    auto expectedBytesInChunksNotSplit = getBytesInOriginalChunk();
    ASSERT_EQ(rt->numChunks(), 5ull);
    assertCorrectBytesWritten(rt,
                              minKey,
                              maxKey,
                              expectedNumChunksFromSplit,
                              expectedBytesInChunksFromSplit,
                              expectedBytesInChunksNotSplit);
}

TEST_F(RoutingTableHistoryTestThreeInitialChunks,
       SplittingLastChunkOfSeveralCopiesBytesWrittenToAllSubchunks) {
    auto minKey = getInitialChunkBoundaryPoints()[2];
    auto maxKey = getInitialChunkBoundaryPoints()[3];
    auto newChunkBoundaryPoints = {minKey, BSON("a" << 25), maxKey};

    auto chunkToSplit = getChunkToSplit(getInitialRoutingTable(), minKey, maxKey);
    auto bytesToWrite = 5ull;
    chunkToSplit->getWritesTracker()->addBytesWritten(bytesToWrite);

    // Split last chunk into two
    auto rt = splitChunk(getInitialRoutingTable(), newChunkBoundaryPoints);

    auto expectedNumChunksFromSplit = 2;
    auto expectedBytesInChunksFromSplit = getBytesInOriginalChunk() + bytesToWrite;
    auto expectedBytesInChunksNotSplit = getBytesInOriginalChunk();
    ASSERT_EQ(rt->numChunks(), 4ull);
    assertCorrectBytesWritten(rt,
                              minKey,
                              maxKey,
                              expectedNumChunksFromSplit,
                              expectedBytesInChunksFromSplit,
                              expectedBytesInChunksNotSplit);
}

TEST_F(RoutingTableHistoryTest, TestSplits) {
    ChunkVersion version{1, 0, collEpoch()};

    auto chunkAll =
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
                  version,
                  kThisShard};

    auto rt = makeNewRt({chunkAll});

    std::vector<ChunkType> chunks1 = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(chunks1);
    auto v1 = ChunkVersion{2, 2, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));

    std::vector<ChunkType> chunks2 = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << -1)},
                  ChunkVersion{3, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << -1), BSON("a" << 0)},
                  ChunkVersion{3, 2, collEpoch()},
                  kThisShard}};

    auto rt2 = rt1->makeUpdated(chunks2);
    auto v2 = ChunkVersion{3, 2, collEpoch()};
    ASSERT_EQ(v2, rt2->getVersion(kThisShard));
}

TEST_F(RoutingTableHistoryTest, TestReplaceChunk) {
    ChunkVersion version{2, 2, collEpoch()};

    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt = RoutingTableHistory::makeNew(
        kNss, UUID::gen(), getShardKeyPattern(), nullptr, false, collEpoch(), {initialChunks});

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{2, 2, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);
    ASSERT_EQ(rt.get(), rt1.get());

    std::shared_ptr<ChunkInfo> found;

    rt1->forEachChunk(
        [&](auto& chunkInfo) {
            if (chunkInfo->getShardIdAt(boost::none) == kThisShard) {
                found = chunkInfo;
                return false;
            }
            return true;
        },
        BSON("a" << 0));
    ASSERT(found);
}

TEST_F(RoutingTableHistoryTest, TestReplaceEmptyChunk) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
                  ChunkVersion{1, 0, collEpoch()},
                  kThisShard}};

    auto rt = makeNewRt(initialChunks);

    ASSERT_EQ(rt->numChunks(), 1);

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{2, 2, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);

    std::shared_ptr<ChunkInfo> found;

    rt1->forEachChunk(
        [&](auto& chunkInfo) {
            if (chunkInfo->getShardIdAt(boost::none) == kThisShard) {
                found = chunkInfo;
                return false;
            }
            return true;
        },
        BSON("a" << 0));
    ASSERT(found);
}

TEST_F(RoutingTableHistoryTest, TestUseLatestVersions) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
                  ChunkVersion{1, 0, collEpoch()},
                  kThisShard}};

    auto rt = makeNewRt(initialChunks);
    ASSERT_EQ(rt->numChunks(), 1);

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
                  ChunkVersion{1, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{2, 2, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);
}

TEST_F(RoutingTableHistoryTest, TestOutOfOrderVersion) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt = makeNewRt(initialChunks);
    ASSERT_EQ(rt->numChunks(), 2);

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), getShardKeyPattern().globalMax()},
                  ChunkVersion{3, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{3, 1, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{3, 1, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);

    auto chunk1 = rt1->findIntersectingChunk(BSON("a" << 0));
    ASSERT_EQ(chunk1->getLastmod(), ChunkVersion(3, 0, collEpoch()));
    ASSERT_EQ(chunk1->getMin().woCompare(BSON("a" << 0)), 0);
    ASSERT_EQ(chunk1->getMax().woCompare(getShardKeyPattern().globalMax()), 0);
}

TEST_F(RoutingTableHistoryTest, TestMergeChunks) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 0), BSON("a" << 10)},
                  ChunkVersion{2, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 0)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 10), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt = makeNewRt(initialChunks);
    ASSERT_EQ(rt->numChunks(), 3);
    ASSERT_EQ(rt->getVersion(), ChunkVersion(2, 2, collEpoch()));

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 10), getShardKeyPattern().globalMax()},
                  ChunkVersion{3, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 10)},
                  ChunkVersion{3, 1, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{3, 1, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);
}

TEST_F(RoutingTableHistoryTest, TestMergeChunksOrdering) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << -10), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << -500)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << -500), BSON("a" << -10)},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard}};

    auto rt = makeNewRt(initialChunks);
    ASSERT_EQ(rt->numChunks(), 3);
    ASSERT_EQ(rt->getVersion(), ChunkVersion(2, 2, collEpoch()));

    std::vector<ChunkType> changedChunks = {
        ChunkType{kNss,
                  ChunkRange{BSON("a" << -500), BSON("a" << -10)},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << -10)},
                  ChunkVersion{3, 1, collEpoch()},
                  kThisShard}};

    auto rt1 = rt->makeUpdated(changedChunks);
    auto v1 = ChunkVersion{3, 1, collEpoch()};
    ASSERT_EQ(v1, rt1->getVersion(kThisShard));
    ASSERT_EQ(rt1->numChunks(), 2);

    auto chunk1 = rt1->findIntersectingChunk(BSON("a" << -500));
    ASSERT_EQ(chunk1->getLastmod(), ChunkVersion(3, 1, collEpoch()));
    ASSERT_EQ(chunk1->getMin().woCompare(getShardKeyPattern().globalMin()), 0);
    ASSERT_EQ(chunk1->getMax().woCompare(BSON("a" << -10)), 0);
}

TEST_F(RoutingTableHistoryTest, TestFlatten) {
    std::vector<ChunkType> initialChunks = {
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 10)},
                  ChunkVersion{2, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 10), BSON("a" << 20)},
                  ChunkVersion{2, 1, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 20), getShardKeyPattern().globalMax()},
                  ChunkVersion{2, 2, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), getShardKeyPattern().globalMax()},
                  ChunkVersion{3, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{getShardKeyPattern().globalMin(), BSON("a" << 10)},
                  ChunkVersion{4, 0, collEpoch()},
                  kThisShard},
        ChunkType{kNss,
                  ChunkRange{BSON("a" << 10), getShardKeyPattern().globalMax()},
                  ChunkVersion{4, 1, collEpoch()},
                  kThisShard},
    };

    auto rt = makeNewRt(initialChunks);
    ASSERT_EQ(rt->numChunks(), 2);
    ASSERT_EQ(rt->getVersion(), ChunkVersion(4, 1, collEpoch()));

    auto chunk1 = rt->findIntersectingChunk(BSON("a" << 0));
    ASSERT_EQ(chunk1->getLastmod(), ChunkVersion(4, 0, collEpoch()));
    ASSERT_EQ(chunk1->getMin().woCompare(getShardKeyPattern().globalMin()), 0);
    ASSERT_EQ(chunk1->getMax().woCompare(BSON("a" << 10)), 0);
}
}  // namespace
}  // namespace mongo
