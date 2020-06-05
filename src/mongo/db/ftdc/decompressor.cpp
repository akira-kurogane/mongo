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

#include "mongo/platform/basic.h"

#include "mongo/db/ftdc/decompressor.h"

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/db/ftdc/compressor.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/ftdc/varint.h"
#include "mongo/db/jsobj.h"
#include "mongo/rpc/object_check.h"
#include "mongo/util/assert_util.h"

namespace mongo {

StatusWith<std::tuple<BSONObj, std::uint32_t, std::uint32_t, std::vector<std::uint64_t>>>
FTDCDecompressor::uncompressToRefDocAndMetrics(ConstDataRange buf, bool skipMetrics) {
    ConstDataRangeCursor compressedDataRange(buf);

    // Read the length of the uncompressed buffer
    auto swUncompressedLength =
        compressedDataRange.readAndAdvanceNoThrow<LittleEndian<std::uint32_t>>();
    if (!swUncompressedLength.isOK()) {
        return {swUncompressedLength.getStatus()};
    }

    // Now uncompress the data
    // Limit size of the buffer we need zlib
    auto uncompressedLength = swUncompressedLength.getValue();

    if (uncompressedLength > 10000000) {
        return Status(ErrorCodes::InvalidLength, "Metrics chunk has exceeded the allowable size.");
    }

    // Devnote: When skipMetrics=true it seems wasteful to use the zlib block
    // decompression for the whole thing when we only want the ref doc. But it
    // seems that each metric chunk is typically 200k, with the ref doc taking
    // 1/4 of that already.
    /*if (skipMetrics) {
        LittleEndian<std::uint32_t> refDocEstSz = 100 * 1024;
        //getDiagnosticData is about 50kb in samples of v4.2
        uncompressedLength = uncompressedLength > refDocEstSz ? refDocEstSz : uncompressedLength;
        // Devnote: Ideally we'd peek inside the first four bytes to get the
        // exact BSON size of the refDoc, but the current DataRangeCursor class
        // only has read *And* advance methods, no 'just read'.
    }*/

    // Sampling with some fields collection from the field shows that
    // size of the ref doc and deltas is approx 4 times larger than the zlib-
    // compressed size.
    auto statusUncompress = _compressor.uncompress(compressedDataRange, uncompressedLength);

    if (!statusUncompress.isOK()) {
        return {statusUncompress.getStatus()};
    }

    ConstDataRangeCursor cdc = statusUncompress.getValue();

    // The document is not part of any checksum so we must validate it is correct
    auto swRef = cdc.readAndAdvanceNoThrow<Validated<BSONObj>>();
    if (!swRef.isOK()) {
        return {swRef.getStatus()};
    }

    BSONObj ref = swRef.getValue();

    // Read count of metrics
    auto swMetricsCount = cdc.readAndAdvanceNoThrow<LittleEndian<std::uint32_t>>();
    if (!swMetricsCount.isOK()) {
        return {swMetricsCount.getStatus()};
    }

    std::uint32_t metricsCount = swMetricsCount.getValue();

    // Read count of samples
    auto swSampleCount = cdc.readAndAdvanceNoThrow<LittleEndian<std::uint32_t>>();
    if (!swSampleCount.isOK()) {
        return {swSampleCount.getStatus()};
    }

    std::uint32_t sampleCount = swSampleCount.getValue();

    if (skipMetrics) {
        return {{ref, metricsCount, sampleCount, {}}};
    }

    // Limit size of the buffer we need for metrics and samples
    if (metricsCount * sampleCount > 1000000) {
        return Status(ErrorCodes::InvalidLength,
                      "Metrics Count and Sample Count have exceeded the allowable range.");
    }

    std::vector<std::uint64_t> refDocVals;

    refDocVals.reserve(metricsCount);

    // We pass the reference document as both the reference document and current document as we only
    // want the array of metrics.
    (void)FTDCBSONUtil::extractMetricsFromDocument(ref, ref, &refDocVals);

    if (refDocVals.size() != metricsCount) {
        return {ErrorCodes::BadValue,
                "The number of FTDC-type metrics in the reference document and metricsCount do not match"};
    }

    // We must always return the reference document
    if (sampleCount == 0) {
        return {{ref, metricsCount, sampleCount, {}}};
    }

    // Read the samples
    std::vector<std::uint64_t> deltas(metricsCount * sampleCount);

    // decompress the deltas
    std::uint64_t zeroesCount = 0;

    auto cdrc = ConstDataRangeCursor(cdc);

    for (std::uint32_t i = 0; i < metricsCount; i++) {
        for (std::uint32_t j = 0; j < sampleCount; j++) {
            if (zeroesCount) {
                deltas[FTDCCompressor::getArrayOffset(sampleCount, j, i)] = 0;
                zeroesCount--;
                continue;
            }

            auto swDelta = cdrc.readAndAdvanceNoThrow<FTDCVarInt>();

            if (!swDelta.isOK()) {
                return swDelta.getStatus();
            }

            if (swDelta.getValue() == 0) {
                auto swZero = cdrc.readAndAdvanceNoThrow<FTDCVarInt>();

                if (!swZero.isOK()) {
                    return swZero.getStatus();
                }

                zeroesCount = swZero.getValue();
            }

            deltas[FTDCCompressor::getArrayOffset(sampleCount, j, i)] = swDelta.getValue();
        }
    }

    // Inflate the deltas
    for (std::uint32_t i = 0; i < metricsCount; i++) {
        deltas[FTDCCompressor::getArrayOffset(sampleCount, 0, i)] += refDocVals[i];
    }

    for (std::uint32_t i = 0; i < metricsCount; i++) {
        for (std::uint32_t j = 1; j < sampleCount; j++) {
            deltas[FTDCCompressor::getArrayOffset(sampleCount, j, i)] +=
                deltas[FTDCCompressor::getArrayOffset(sampleCount, j - 1, i)];
        }
    }
    // Now they are full values, not deltas

    return {{ref, metricsCount, sampleCount, deltas}};
}

//Todo: initialize the metrics array here and pass by ref to uncompressToRefDocAndMetrics()
StatusWith<std::vector<BSONObj>> FTDCDecompressor::uncompress(ConstDataRange buf) {
    //std::vector<std::uint64_t> metrics;

    auto swRM = uncompressToRefDocAndMetrics(buf);
    if (!swRM.isOK()) {
        return {swRM.getStatus()};
    }
    auto const& [ref, metricsCount, sampleCount, metrics] = swRM.getValue();

    std::vector<BSONObj> docs;

    // Allocate space for the reference document + samples
    docs.reserve(1 + sampleCount);

    docs.emplace_back(ref.getOwned());

    std::vector<std::uint64_t> sampleMetrics;
    sampleMetrics.reserve(metricsCount);
    for (std::uint32_t i = 0; i < sampleCount; ++i) {
        sampleMetrics.clear();
        for (std::uint32_t j = 0; j < metricsCount; ++j) {
            sampleMetrics[j] = metrics[j * sampleCount + i];
        }
        docs.emplace_back(FTDCBSONUtil::constructDocumentFromMetrics(ref, sampleMetrics).getValue());
    }

    return {docs};

}

//Todo: initialize the metrics array here and pass by ref to uncompressToRefDocAndMetrics(). It will be of no use, this is just to keep it like uncompress() does
StatusWith<std::tuple<BSONObj, std::uint32_t, std::uint32_t>>
FTDCDecompressor::uncompressMetricsPreview(ConstDataRange buf) {
    //TODO: change to using a 'uncompressToRefDocAndSampleCount[AndTS]()'
    auto swRM = uncompressToRefDocAndMetrics(buf, true);
    if (!swRM.isOK()) {
        return {swRM.getStatus()};
    }
    auto const& [ref, metricsCount, sampleCount, metrics] = swRM.getValue();

    return {std::tuple<BSONObj, std::uint32_t, std::uint32_t>(ref, metricsCount, sampleCount)};
}

}  // namespace mongo
