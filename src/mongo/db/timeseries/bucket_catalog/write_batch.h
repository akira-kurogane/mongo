/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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

#pragma once

#include <boost/container/small_vector.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/bson/util/bsoncolumnbuilder.h"
#include "mongo/db/operation_id.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/timeseries/bucket_catalog/bucket_identifiers.h"
#include "mongo/db/timeseries/bucket_catalog/execution_stats.h"
#include "mongo/db/timeseries/bucket_catalog/measurement_map.h"
#include "mongo/db/timeseries/bucket_compression.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"
#include "mongo/util/string_map.h"

namespace mongo::timeseries::bucket_catalog {

struct Bucket;

struct CommitInfo {
    boost::optional<repl::OpTime> opTime;
    boost::optional<OID> electionId;
};

struct Sizes {
    // Contains the verified size for:
    // - The meta, control, and field names for previously unaccounted fields in a Bucket.
    // - Data fields after intermediate().
    int32_t uncommittedVerifiedSize = 0;

    // The estimated size of uncommitted data fields being inserted into a Bucket.
    int32_t uncommittedMeasurementEstimate = 0;

    int32_t total() const {
        return uncommittedVerifiedSize + uncommittedMeasurementEstimate;
    }

    void operator+=(const Sizes& other) {
        uncommittedVerifiedSize += other.uncommittedVerifiedSize;
        uncommittedMeasurementEstimate += other.uncommittedMeasurementEstimate;
    }
};

/**
 * The basic unit of work for a bucket. Each insert will return a shared_ptr to a WriteBatch.
 * When a writer is finished with all their insertions, they should then take steps to ensure
 * each batch they wrote into is committed. To ensure a batch is committed, a writer should
 * first attempt to claimWriteBatchCommitRights(). If successful, the writer can proceed to commit
 * (or abort) the batch via BucketCatalog::prepareCommit and BucketCatalog::finish. If unsuccessful,
 * it means another writer is in the process of committing. The writer can proceed to do other
 * work (like commit another batch), and when they have no other work to do, they can wait for
 * this batch to be committed by executing the blocking operation getWriteBatchResult().
 *
 * The order of members of this struct have been optimized for memory alignment, and therefore
 * a low memory footprint. Take extra care if modifying the order or adding or removing fields.
 */
struct WriteBatch {
    WriteBatch() = delete;
    WriteBatch(TrackingContext& trackingContext,
               const BucketHandle& bucketHandle,
               BucketKey bucketKey,
               OperationId opId,
               ExecutionStatsController& stats,
               StringData timeField);

    BSONObj toBSON() const;

    bool generateCompressedDiff = false;

    // Whether the measurements in the bucket are sorted by timestamp or not.
    // True by default, if a v2 buckets gets promoted to v3 this is set to false.
    // It should not be used for v1 buckets. v2 buckets are preferred over v3 for improved
    // read/query performance, v3 buckets get created as necessary to retain higher write
    // performance.
    bool bucketIsSortedByTime = true;

    bool openedDueToMetadata =
        false;  // If true, bucket has been opened due to the inserted measurement having different
    // metadata than available buckets.

    AtomicWord<bool> commitRights{false};

    const OperationId opId;

    uint32_t numPreviouslyCommittedMeasurements = 0;

    // For always compressed, adds the compressed measurement sizes while committing.
    int32_t size = 0;

    TrackingContext& trackingContext;

    StringData timeField;  // Necessary so we can compress on writes, since the compression
                           // algorithm sorts on the timeField. See compressBucket().

    BSONObj min;  // Batch-local min; full if first batch, updates otherwise.
    BSONObj max;  // Batch-local max; full if first batch, updates otherwise.

    SharedPromise<CommitInfo> promise;

    ExecutionStatsController stats;

    // Marginal numbers for this batch only. Uncommitted is a rough estimate of data in this batch,
    // using 0 for anything under threshold, and uncompressed size for anything over threshold.
    // Committed is 0 until it is populated by intermediate as the delta for committing this batch.
    Sizes sizes;

    StringMap<std::size_t> newFieldNamesToBeInserted;  // Value is hash of string key

    // Number of measurements we can hold in a batch without needing to allocate memory.
    static constexpr std::size_t kNumStaticBatchMeasurements = 10;
    using BatchMeasurements = boost::container::small_vector<BSONObj, kNumStaticBatchMeasurements>;
    BatchMeasurements measurements;

    const BucketHandle bucketHandle;

    /**
     * In-memory data fields. Allows for quick compression of bucket data.
     *
     * Initially these are for committed data fields, but this will be the working set of builders
     * for the current WriteBatch and will contain uncommitted data fields in
     * makeTimeseriesCompressedDiffUpdateOp.
     */
    MeasurementMap measurementMap;

    const BucketKey bucketKey;
};

const BSONObj& getUncompressedBucketDoc(const WriteBatch& batch);
void setUncompressedBucketDoc(WriteBatch& batch, BSONObj uncompressedBucketDoc);

/**
 * Returns whether the batch has already been committed or aborted.
 */
bool isWriteBatchFinished(const WriteBatch& batch);

/**
 * Attempts to claim the right to commit a batch. If it returns true, rights are
 * granted. If it returns false, rights are revoked, and the caller should get the result
 * of the batch with getWriteBatchResult(). Non-blocking.
 */
bool claimWriteBatchCommitRights(WriteBatch& batch);

/**
 * Retrieves the result of the write batch commit. Should be called by any interested party
 * that does not have commit rights. Blocking.
 */
StatusWith<CommitInfo> getWriteBatchResult(WriteBatch& batch);

}  // namespace mongo::timeseries::bucket_catalog
