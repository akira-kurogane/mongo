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

#include "mongo/db/service_context.h"
#include "mongo/util/concurrency/admission_context.h"
#include "mongo/util/concurrency/ticketholder.h"

#include <iostream>

#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace mongo {

namespace {
void updateQueueStatsOnRelease(ServiceContext* serviceContext,
                               TicketHolderWithQueueingStats::QueueStats& queueStats,
                               AdmissionContext* admCtx) {
    queueStats.totalFinishedProcessing.fetchAndAddRelaxed(1);
    auto startTime = admCtx->getStartProcessingTime();
    auto tickSource = serviceContext->getTickSource();
    auto delta = tickSource->ticksTo<Microseconds>(tickSource->getTicks() - startTime);
    queueStats.totalTimeProcessingMicros.fetchAndAddRelaxed(delta.count());
}

void updateQueueStatsOnTicketAcquisition(ServiceContext* serviceContext,
                                         TicketHolderWithQueueingStats::QueueStats& queueStats,
                                         AdmissionContext* admCtx) {
    if (admCtx->getAdmissions() == 0) {
        queueStats.totalNewAdmissions.fetchAndAddRelaxed(1);
    }
    admCtx->start(serviceContext->getTickSource());
    queueStats.totalStartedProcessing.fetchAndAddRelaxed(1);
}
}  // namespace

Ticket TicketHolderWithQueueingStats::acquireImmediateTicket(AdmissionContext* admCtx) {
    invariant(admCtx->getPriority() == AdmissionContext::Priority::kImmediate);
    if (recordImmediateTicketStatistics()) {
        auto& queueStats = _getQueueStatsToUse(admCtx);
        updateQueueStatsOnTicketAcquisition(_serviceContext, queueStats, admCtx);
    }
    return Ticket{this, admCtx};
}

void TicketHolderWithQueueingStats::resize(OperationContext* opCtx, int newSize) noexcept {
    stdx::lock_guard<Latch> lk(_resizeMutex);

    _resize(opCtx, newSize, _outof.load());
    _outof.store(newSize);
}

void TicketHolderWithQueueingStats::appendStats(BSONObjBuilder& b) const {
    b.append("out", used());
    b.append("available", available());
    b.append("totalTickets", outof());
    _appendImplStats(b);
}

void TicketHolderWithQueueingStats::_releaseImmediateTicket(AdmissionContext* admCtx) noexcept {
    if (recordImmediateTicketStatistics()) {
        auto& queueStats = _getQueueStatsToUse(admCtx);
        updateQueueStatsOnRelease(_serviceContext, queueStats, admCtx);
    }
}

void TicketHolderWithQueueingStats::_releaseToTicketPool(AdmissionContext* admCtx) noexcept {
    auto& queueStats = _getQueueStatsToUse(admCtx);
    updateQueueStatsOnRelease(_serviceContext, queueStats, admCtx);
    _releaseToTicketPoolImpl(admCtx);
}

Ticket TicketHolderWithQueueingStats::waitForTicket(OperationContext* opCtx,
                                                    AdmissionContext* admCtx,
                                                    WaitMode waitMode) {
    auto res = waitForTicketUntil(opCtx, admCtx, Date_t::max(), waitMode);
    invariant(res);
    return std::move(*res);
}

boost::optional<Ticket> TicketHolderWithQueueingStats::tryAcquire(AdmissionContext* admCtx) {
    // kImmediate operations don't need to 'try' to acquire a ticket, they should always get a
    // ticket immediately.
    invariant(admCtx && admCtx->getPriority() != AdmissionContext::Priority::kImmediate);
    auto ticket = _tryAcquireImpl(admCtx);

    if (ticket) {
        auto& queueStats = _getQueueStatsToUse(admCtx);
        updateQueueStatsOnTicketAcquisition(_serviceContext, queueStats, admCtx);
    }
    return ticket;
}


boost::optional<Ticket> TicketHolderWithQueueingStats::waitForTicketUntil(OperationContext* opCtx,
                                                                          AdmissionContext* admCtx,
                                                                          Date_t until,
                                                                          WaitMode waitMode) {
    invariant(admCtx);

    // Attempt a quick acquisition first.
    if (auto ticket = tryAcquire(admCtx)) {
        return ticket;
    }

    auto& queueStats = _getQueueStatsToUse(admCtx);
    auto tickSource = _serviceContext->getTickSource();
    auto currentWaitTime = tickSource->getTicks();
    auto updateQueuedTime = [&]() {
        auto oldWaitTime = std::exchange(currentWaitTime, tickSource->getTicks());
        auto waitDelta = tickSource->ticksTo<Microseconds>(currentWaitTime - oldWaitTime).count();
        queueStats.totalTimeQueuedMicros.fetchAndAddRelaxed(waitDelta);
    };
    queueStats.totalAddedQueue.fetchAndAddRelaxed(1);
    ON_BLOCK_EXIT([&] {
        updateQueuedTime();
        queueStats.totalRemovedQueue.fetchAndAddRelaxed(1);
    });

    ScopeGuard cancelWait([&] {
        // Update statistics.
        queueStats.totalCanceled.fetchAndAddRelaxed(1);
    });

    auto ticket = _waitForTicketUntilImpl(opCtx, admCtx, until, waitMode);

    if (ticket) {
        cancelWait.dismiss();
        updateQueueStatsOnTicketAcquisition(_serviceContext, queueStats, admCtx);
        return ticket;
    } else {
        return boost::none;
    }
}

void TicketHolderWithQueueingStats::_appendCommonQueueImplStats(BSONObjBuilder& b,
                                                                const QueueStats& stats) const {
    auto removed = stats.totalRemovedQueue.loadRelaxed();
    auto added = stats.totalAddedQueue.loadRelaxed();

    b.append("addedToQueue", added);
    b.append("removedFromQueue", removed);
    b.append("queueLength", std::max(static_cast<int>(added - removed), 0));

    auto finished = stats.totalFinishedProcessing.loadRelaxed();
    auto started = stats.totalStartedProcessing.loadRelaxed();
    b.append("startedProcessing", started);
    b.append("processing", std::max(static_cast<int>(started - finished), 0));
    b.append("finishedProcessing", finished);
    b.append("totalTimeProcessingMicros", stats.totalTimeProcessingMicros.loadRelaxed());
    b.append("canceled", stats.totalCanceled.loadRelaxed());
    b.append("newAdmissions", stats.totalNewAdmissions.loadRelaxed());
    b.append("totalTimeQueuedMicros", stats.totalTimeQueuedMicros.loadRelaxed());
}

}  // namespace mongo
