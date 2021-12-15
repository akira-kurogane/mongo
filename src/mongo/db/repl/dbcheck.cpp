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

#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/health_log.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/repl/dbcheck.h"
#include "mongo/db/repl/dbcheck_gen.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/storage/durable_catalog.h"

namespace mongo {

MONGO_FAIL_POINT_DEFINE(SleepDbCheckInBatch);

namespace {

/*
 * Some utilities for dealing with the expected/found documents in health log entries.
 */

bool operator==(const std::vector<BSONObj>& lhs, const std::vector<BSONObj>& rhs) {
    return std::equal(lhs.cbegin(),
                      lhs.cend(),
                      rhs.cbegin(),
                      rhs.cend(),
                      [](const auto& x, const auto& y) -> bool { return x.woCompare(y) == 0; });
}

/**
 * Get whether the expected and found objects match, plus an expected/found object to report to the
 * health log.
 */
template <typename T>
std::pair<bool, BSONObj> expectedFound(const T& expected, const T& found) {
    auto obj = BSON("expected" << expected << "found" << found);
    return std::pair<bool, BSONObj>(expected == found, obj);
}

template <>
std::pair<bool, BSONObj> expectedFound(const BSONObj& expected, const BSONObj& found) {
    auto obj = BSON("expected" << expected << "found" << found);
    return std::pair<bool, BSONObj>(expected.woCompare(found) == 0, obj);
}

/**
 * An overload for boost::optionals, which omits boost::none fields.
 */
template <typename T>
std::pair<bool, BSONObj> expectedFound(const boost::optional<T>& expected,
                                       const boost::optional<T>& found) {
    BSONObjBuilder builder;
    if (expected) {
        builder << "expected" << *expected;
    }
    if (found) {
        builder << "found" << *found;
    }

    auto obj = builder.obj();

    if (expected && found) {
        return std::pair<bool, BSONObj>(*expected == *found, obj);
    } else if (expected || found) {
        return std::pair<bool, BSONObj>(false, obj);
    }

    return std::pair<bool, BSONObj>(true, obj);
}

std::string renderForHealthLog(OplogEntriesEnum op) {
    switch (op) {
        case OplogEntriesEnum::Batch:
            return "dbCheckBatch";
        case OplogEntriesEnum::Collection:
            return "dbCheckCollection";
    }

    MONGO_UNREACHABLE;
}

}  // namespace
/**
 * Fills in the timestamp and scope, which are always the same for dbCheck's entries.
 */
std::unique_ptr<HealthLogEntry> dbCheckHealthLogEntry(const NamespaceString& nss,
                                                      SeverityEnum severity,
                                                      const std::string& msg,
                                                      OplogEntriesEnum operation,
                                                      const BSONObj& data) {
    auto entry = std::make_unique<HealthLogEntry>();
    entry->setNss(nss);
    entry->setTimestamp(Date_t::now());
    entry->setSeverity(severity);
    entry->setScope(ScopeEnum::Cluster);
    entry->setMsg(msg);
    entry->setOperation(renderForHealthLog(operation));
    entry->setData(data);
    return entry;
}

/**
 * Get an error message if the check fails.
 */
std::unique_ptr<HealthLogEntry> dbCheckErrorHealthLogEntry(const NamespaceString& nss,
                                                           const std::string& msg,
                                                           OplogEntriesEnum operation,
                                                           const Status& err) {
    return dbCheckHealthLogEntry(nss,
                                 SeverityEnum::Error,
                                 msg,
                                 operation,
                                 BSON("success" << false << "error" << err.toString()));
}

std::unique_ptr<HealthLogEntry> dbCheckWarningHealthLogEntry(const NamespaceString& nss,
                                                             const std::string& msg,
                                                             OplogEntriesEnum operation,
                                                             const Status& err) {
    return dbCheckHealthLogEntry(nss,
                                 SeverityEnum::Warning,
                                 msg,
                                 operation,
                                 BSON("success" << false << "error" << err.toString()));
}

/**
 * Get a HealthLogEntry for a dbCheck batch.
 */
std::unique_ptr<HealthLogEntry> dbCheckBatchEntry(
    const NamespaceString& nss,
    int64_t count,
    int64_t bytes,
    const std::string& expectedHash,
    const std::string& foundHash,
    const BSONKey& minKey,
    const BSONKey& maxKey,
    const repl::OpTime& optime,
    const boost::optional<CollectionOptions>& options) {
    auto hashes = expectedFound(expectedHash, foundHash);

    auto data = BSON("success" << true << "count" << count << "bytes" << bytes << "md5"
                               << hashes.second << "minKey" << minKey.elem() << "maxKey"
                               << maxKey.elem() << "optime" << optime);

    const auto hashesMatch = hashes.first;
    const auto severity = [&] {
        if (hashesMatch) {
            return SeverityEnum::Info;
        }
        // Implcitily replicated collections and capped collections not replicating truncation are
        // not designed to be consistent, so inconsistency is not necessarily pathological.
        if (nss.isChangeStreamPreImagesCollection() || nss.isConfigImagesCollection() ||
            (options && options->capped)) {
            return SeverityEnum::Warning;
        }

        return SeverityEnum::Error;
    }();
    std::string msg =
        "dbCheck batch " + (hashesMatch ? std::string("consistent") : std::string("inconsistent"));

    return dbCheckHealthLogEntry(nss, severity, msg, OplogEntriesEnum::Batch, data);
}

DbCheckHasher::DbCheckHasher(OperationContext* opCtx,
                             Collection* collection,
                             const BSONKey& start,
                             const BSONKey& end,
                             int64_t maxCount,
                             int64_t maxBytes)
    : _opCtx(opCtx), _maxKey(end), _maxCount(maxCount), _maxBytes(maxBytes) {

    // Get the MD5 hasher set up.
    md5_init(&_state);

    // Get the _id index.
    const IndexDescriptor* desc = collection->getIndexCatalog()->findIdIndex(opCtx);
    uassert(ErrorCodes::IndexNotFound, "dbCheck needs _id index", desc);

    // Set up a simple index scan on that.
    _exec = InternalPlanner::indexScan(opCtx,
                                       collection,
                                       desc,
                                       start.obj(),
                                       end.obj(),
                                       BoundInclusion::kIncludeEndKeyOnly,
                                       PlanExecutor::NO_YIELD,
                                       InternalPlanner::FORWARD,
                                       InternalPlanner::IXSCAN_FETCH);
}


template <typename T>
const md5_byte_t* md5Cast(const T* ptr) {
    return reinterpret_cast<const md5_byte_t*>(ptr);
}

void maybeAppend(md5_state_t* state, const boost::optional<UUID>& uuid) {
    if (uuid) {
        md5_append(state, md5Cast(uuid->toCDR().data()), uuid->toCDR().length());
    }
}

Status DbCheckHasher::hashAll(OperationContext* opCtx, Date_t deadline) {
    BSONObj currentObj;

    PlanExecutor::ExecState lastState;
    while (PlanExecutor::ADVANCED == (lastState = _exec->getNext(&currentObj, nullptr))) {

        SleepDbCheckInBatch.execute([opCtx](const BSONObj& data) {
            int sleepMs = data["sleepMs"].safeNumberInt();
            opCtx->sleepFor(Milliseconds(sleepMs));
        });

        if (!currentObj.hasField("_id")) {
            return Status(ErrorCodes::NoSuchKey, "Document missing _id");
        }

        // If this would put us over a limit, stop here.
        if (!_canHash(currentObj)) {
            return Status::OK();
        }

        // Update `last` every time.
        _last = BSONKey::parseFromBSON(currentObj["_id"]);
        _bytesSeen += currentObj.objsize();
        _countSeen += 1;

        md5_append(&_state, md5Cast(currentObj.objdata()), currentObj.objsize());

        if (Date_t::now() > deadline) {
            break;
        }
    }

    // If we got to the end of the collection, set the last key to MaxKey.
    if (lastState == PlanExecutor::IS_EOF) {
        _last = _maxKey;
    }

    return Status::OK();
}

std::string DbCheckHasher::total(void) {
    md5digest digest;
    md5_finish(&_state, digest);

    return digestToString(digest);
}

BSONKey DbCheckHasher::lastKey(void) const {
    return _last;
}

int64_t DbCheckHasher::bytesSeen(void) const {
    return _bytesSeen;
}

int64_t DbCheckHasher::docsSeen(void) const {
    return _countSeen;
}

bool DbCheckHasher::_canHash(const BSONObj& obj) {
    // Make sure we hash at least one document.
    if (_countSeen == 0) {
        return true;
    }

    // Check that this won't push us over our byte limit
    if (_bytesSeen + obj.objsize() > _maxBytes) {
        return false;
    }

    // or our document limit.
    if (_countSeen + 1 > _maxCount) {
        return false;
    }

    return true;
}

namespace {

Status dbCheckBatchOnSecondary(OperationContext* opCtx,
                               const repl::OpTime& optime,
                               const DbCheckOplogBatch& entry) {
    AutoGetCollection coll(opCtx, entry.getNss(), MODE_S);
    const auto& collection = coll.getCollection();

    if (!collection) {
        const auto msg = "Collection under dbCheck no longer exists";
        auto logEntry = dbCheckHealthLogEntry(entry.getNss(),
                                              SeverityEnum::Info,
                                              "dbCheck failed",
                                              OplogEntriesEnum::Batch,
                                              BSON("success" << false << "info" << msg));
        HealthLog::get(opCtx).log(*logEntry);
        return Status::OK();
    }

    const auto msg = "replication consistency check";

    // Set up the hasher,
    Status status = Status::OK();
    boost::optional<DbCheckHasher> hasher;
    try {
        hasher.emplace(opCtx, collection, entry.getMinKey(), entry.getMaxKey());
    } catch (const DBException& exception) {
        auto logEntry = dbCheckErrorHealthLogEntry(
            entry.getNss(), msg, OplogEntriesEnum::Batch, exception.toStatus());
        HealthLog::get(opCtx).log(*logEntry);
        return Status::OK();
    }

    // run the hasher.
    if (status.isOK()) {
        status = hasher->hashAll(opCtx);
    }

    // In case of an error, report it to the health log,
    if (!status.isOK()) {
        auto logEntry =
            dbCheckErrorHealthLogEntry(entry.getNss(), msg, OplogEntriesEnum::Batch, status);
        HealthLog::get(opCtx).log(*logEntry);
        return Status::OK();
    }

    std::string expected = entry.getMd5().toString();
    std::string found = hasher->total();

    auto logEntry = dbCheckBatchEntry(entry.getNss(),
                                      hasher->docsSeen(),
                                      hasher->bytesSeen(),
                                      expected,
                                      found,
                                      entry.getMinKey(),
                                      hasher->lastKey(),
                                      optime,
                                      boost::none);

    HealthLog::get(opCtx).log(*logEntry);

    return Status::OK();
}

}  // namespace

namespace repl {

/*
 * The corresponding command run during command application.
 */
Status dbCheckOplogCommand(OperationContext* opCtx,
                           const repl::OplogEntry& entry,
                           OplogApplication::Mode mode) {
    const auto& cmd = entry.getObject();
    OpTime opTime;
    if (!opCtx->writesAreReplicated()) {
        opTime = entry.getOpTime();
    }
    auto type = OplogEntries_parse(IDLParserErrorContext("type"), cmd.getStringField("type"));
    IDLParserErrorContext ctx("o");

    switch (type) {
        case OplogEntriesEnum::Batch: {
            auto invocation = DbCheckOplogBatch::parse(ctx, cmd);
            return dbCheckBatchOnSecondary(opCtx, opTime, invocation);
        }
        case OplogEntriesEnum::Collection: {
            // TODO SERVER-61963.
            return Status::OK();
        }
    }

    MONGO_UNREACHABLE;
}

}  // namespace repl
}  // namespace mongo
