/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/fts/fts_tokenizer.h"
//TODELETE? #include "mongo/util/stringutils.h"
#include "mongo/db/fts/unicode/string.h"

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
    ~MecabFTSTokenizer();

    void reset(StringData document, Options options) final;

    bool moveNext() final;

    /**
     * Note: returned string lifetime is tied to lifetime of class.
     * Also, it is invalidated on each call to moveNext.
     */
    StringData get() const final;

private:
    /**
     * Helper that moves the tokenizer past all delimiters that shouldn't be considered part of
     * tokens. Copied from UnicodeFTSTokenizer verbatim
     */
    void _skipDelimiters();

    StringData _document;
    size_t _pos;
    StringData _word;
    Options _options;

    StackBufBuilder _wordBuf;

};

}  // namespace fts
}  // namespace mongo
