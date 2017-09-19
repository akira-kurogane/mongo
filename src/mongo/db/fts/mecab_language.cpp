/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/fts/fts_language.h"
#include "mongo/db/fts/fts_tokenizer.h"
#include "mongo/stdx/memory.h"

#include "mecab_tokenizer.h"
//TODO#include "mecab_enviroment.h"

namespace mongo {
namespace fts {
namespace {

/**
 * MecabFTSLanguage
 *
 * Represents all the information needs to create a Mecab-based tokenizer for
 * Japanese language.
 */
class MecabFTSLanguage : public FTSLanguage {
public:
    MecabFTSLanguage() : _unicodePhraseMatcher("japanese") {}

    std::unique_ptr<FTSTokenizer> createTokenizer() const final {
        return stdx::make_unique<MecabFTSTokenizer>(); //using default constructor
    }

    const FTSPhraseMatcher& getPhraseMatcher() const final {
       return _unicodePhraseMatcher;
    }
private:
    UnicodeFTSPhraseMatcher _unicodePhraseMatcher;
};
}  // namespace

Status registerMecabLanguage() {

    //MecabFTSLanguage mecabFTSLanguage();
    //FTSLanguage::registerLanguage("japanese", TEXT_INDEX_VERSION_3, &mecabFTSLanguage);
    UnicodeFTSLanguage languageNoneV3("none");
    FTSLanguage::registerLanguage("japanese", TEXT_INDEX_VERSION_3, &languageNoneV3);

    return Status::OK();
}

}  // namespace fts
}  // namespace mongo
