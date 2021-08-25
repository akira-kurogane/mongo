/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kProcessHealth

#include "mongo/db/process_health/fault_manager.h"

#include "mongo/db/process_health/fault_impl.h"
#include "mongo/db/process_health/health_observer_registration.h"
#include "mongo/logv2/log.h"

namespace mongo {

namespace process_health {

namespace {

const auto sFaultManager = ServiceContext::declareDecoration<std::unique_ptr<FaultManager>>();

ServiceContext::ConstructorActionRegisterer faultManagerRegisterer{
    "FaultManagerRegisterer", [](ServiceContext* svcCtx) {
        auto faultManager = std::make_unique<FaultManager>(svcCtx);
        FaultManager::set(svcCtx, std::move(faultManager));
    }};

}  // namespace


FaultManager* FaultManager::get(ServiceContext* svcCtx) {
    return sFaultManager(svcCtx).get();
}

void FaultManager::set(ServiceContext* svcCtx, std::unique_ptr<FaultManager> newFaultManager) {
    invariant(newFaultManager);
    auto& faultManager = sFaultManager(svcCtx);
    faultManager = std::move(newFaultManager);
}

FaultManager::FaultManager(ServiceContext* svcCtx) : _svcCtx(svcCtx) {}

FaultManager::~FaultManager() {}

FaultState FaultManager::getFaultState() const {
    stdx::lock_guard<Latch> lk(_stateMutex);
    return _currentState;
}

boost::optional<FaultConstPtr> FaultManager::activeFault() const {
    return {};
}

boost::optional<FaultFacetsContainerPtr> FaultManager::getFaultFacetsContainer() {
    auto lk = stdx::lock_guard(_mutex);
    if (!_fault) {
        return {};
    }
    return std::static_pointer_cast<FaultFacetsContainer>(_fault);
}

FaultFacetsContainerPtr FaultManager::getOrCreateFaultFacetsContainer() {
    auto lk = stdx::lock_guard(_mutex);
    if (!_fault) {
        // Create a new one.
        _fault = std::make_shared<FaultImpl>(_svcCtx);
    }
    return std::static_pointer_cast<FaultFacetsContainer>(_fault);
}

void FaultManager::healthCheck() {
    // One time init.
    _initHealthObserversIfNeeded();
}

Status FaultManager::transitionToState(FaultState newState) {
    Status status = Status::OK();
    switch (newState) {
        case FaultState::kOk:
            status = _transitionToKOk();
            break;
        case FaultState::kTransientFault:
            status = _transitionToKTransientFault();
            break;
        case FaultState::kActiveFault:
            status = _transitionToKActiveFault();
            break;
        default:
            return Status(ErrorCodes::BadValue,
                          fmt::format("Illegal transition from {} to {}", _currentState, newState));
            break;
    }

    if (status.isOK()) {
        LOGV2_DEBUG(5936201, 1, "Transitioned fault manager state", "newState"_attr = newState);
    }

    return status;
}

Status FaultManager::_transitionToKOk() {
    stdx::lock_guard<Latch> lk(_stateMutex);
    if (_currentState != FaultState::kStartupCheck && _currentState != FaultState::kTransientFault)
        return Status(
            ErrorCodes::BadValue,
            fmt::format("Illegal transition from {} to {}", _currentState, FaultState::kOk));

    _currentState = FaultState::kOk;
    return Status::OK();
}

Status FaultManager::_transitionToKTransientFault() {
    stdx::lock_guard<Latch> lk(_stateMutex);
    if (_currentState != FaultState::kStartupCheck && _currentState != FaultState::kOk)
        return Status(ErrorCodes::BadValue,
                      fmt::format("Illegal transition from {} to {}",
                                  _currentState,
                                  FaultState::kTransientFault));

    _currentState = FaultState::kTransientFault;
    return Status::OK();
}

Status FaultManager::_transitionToKActiveFault() {
    stdx::lock_guard<Latch> lk(_stateMutex);
    if (_currentState != FaultState::kTransientFault)
        return Status(ErrorCodes::BadValue,
                      fmt::format("Illegal transition from {} to {}",
                                  _currentState,
                                  FaultState::kActiveFault));

    _currentState = FaultState::kActiveFault;
    return Status::OK();
}

void FaultManager::_initHealthObserversIfNeeded() {
    if (_initializedAllHealthObservers.load()) {
        return;
    }

    stdx::lock_guard<Latch> lk(_mutex);
    // One more time under lock to avoid race.
    if (_initializedAllHealthObservers.load()) {
        return;
    }
    _initializedAllHealthObservers.store(true);

    HealthObserverRegistration& registration = *HealthObserverRegistration::get(_svcCtx);
    _observers = registration.instantiateAllObservers();

    // Verify that all observer types are unique.
    std::set<FaultFacetType> allTypes;
    for (const auto& observer : _observers) {
        allTypes.insert(observer->getType());
    }
    invariant(allTypes.size() == _observers.size());

    stdx::lock_guard<Latch> lk2(_stateMutex);
    LOGV2(5956701,
          "Instantiated health observers, periodic health checking starts",
          "managerState"_attr = _currentState,
          "observersCount"_attr = _observers.size());
}

std::vector<HealthObserver*> FaultManager::getHealthObservers() {
    std::vector<HealthObserver*> result;
    stdx::lock_guard<Latch> lk(_mutex);
    result.reserve(_observers.size());
    std::transform(_observers.cbegin(),
                   _observers.cend(),
                   std::back_inserter(result),
                   [](const std::unique_ptr<HealthObserver>& value) { return value.get(); });
    return result;
}

}  // namespace process_health
}  // namespace mongo
