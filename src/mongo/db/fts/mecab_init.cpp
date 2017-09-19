/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

//TODELETE? #define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/server_parameters.h"
//TODELETE? #include "mongo/util/assert_util.h"
//TODELETE? #include "mongo/util/log.h"

#include "mecab_language.h"
//TODO #include "rlp_loader.h"
//TODO #include "rlp_options.h"

namespace mongo {
namespace fts {

// Global System wide Mecab Loader
// This keeps the MecabLoader alive for the lifetime of the process
//
//TODO MecabLoader* mecabLoader;


MONGO_INITIALIZER_GENERAL(InitMecab, ("EndStartupOptionHandling"), ("default"))
(InitializerContext* context) {
    /**
     * if (something wrong with mecab) {
     *     LOG(1) << "Skipping Mecab Initialization, xyz is missing.";
     *     return Status::OK();
     * }
     */

    // TODO StatusWith<std::unique_ptr<MecabLoader>> sw =
    // TODO    MecabLoader::create(...);

    // TODO if (sw.getStatus().isOK()) {
    // TODO     mecabLoader = sw.getValue().release();
    // TODO }

    // TODO return sw.getStatus();
    return Status::OK();
}

// Register languages so they can be resolved at runtime
//
MONGO_INITIALIZER_WITH_PREREQUISITES(
    MecabLangInit, ("FTSAllLanguagesRegistered", "FTSRegisterLanguageAliases", "InitMecab"))
(::mongo::InitializerContext* context) {
    // No conditional - in this dev work let's assume it will always be used
    return registerMecabLanguage();

    return Status::OK();
}

}  // namespace fts
}  // namespace mongo
