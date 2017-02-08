/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/fts/fts_tokenizer.h"
//TODELETE? #include "mongo/util/stringutils.h"

namespace mongo {
namespace fts {

/**
 * MecabFTSTokenizer
 *
 * Implements an FTSTokenizer using Mecab
 */
class MecabFTSTokenizer : public FTSTokenizer {
    MONGO_DISALLOW_COPYING(MecabFTSTokenizer);

public:
    MecabFTSTokenizer();

    void reset(StringData document, Options options) final;

    bool moveNext() final;

    /**
     * Note: returned string lifetime is tied to lifetime of class.
     * Also, it is invalidated on each call to moveNext.
     */
    StringData get() const final;

private:
    Options _options;
};

}  // namespace fts
}  // namespace mongo
