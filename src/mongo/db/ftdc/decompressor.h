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

#pragma once

#include <vector>

#include "mongo/base/data_range.h"
#include "mongo/base/status_with.h"
#include "mongo/db/ftdc/block_compressor.h"
#include "mongo/db/jsobj.h"

namespace mongo {

/**
 * Inflates a compressed chunk of metrics into a list of BSON documents
 */
class FTDCDecompressor {
    FTDCDecompressor(const FTDCDecompressor&) = delete;
    FTDCDecompressor& operator=(const FTDCDecompressor&) = delete;

public:
    FTDCDecompressor() = default;

    /**
     * Inflates a compressed chunk of metrics into a tuple of it's reference
     * BSONDoc, the sample and metrics count, and a N*M length array of the
     * decompressed metrics.
     *
     * Will fail if the chunk is corrupt or too short.
     *
     * Returns N samples where N = sample count + 1. The 1 is the reference document.
     */
    StatusWith<std::tuple<BSONObj, std::uint32_t, std::uint32_t, std::vector<std::uint64_t>>>
    uncompressToRefDocAndMetrics(ConstDataRange buf, bool skipMetrics = false);

    /**
     * Inflates a compressed chunk of metrics into a vector of owned BSON documents.
     *
     * Will fail if uncompressToRefDocAndMetrics() fails.
     */
    StatusWith<std::vector<BSONObj>> uncompress(ConstDataRange buf);

    /**
     * Like uncompress but avoiding further decompression and BSON object
     * reassembly after the refDoc, metricsCount and sampleCount are unpacked.
     */
    StatusWith<std::tuple<BSONObj, std::uint32_t, std::uint32_t>>
    uncompressMetricsPreview(ConstDataRange buf);

private:
    BlockCompressor _compressor;
};

}  // namespace mongo
