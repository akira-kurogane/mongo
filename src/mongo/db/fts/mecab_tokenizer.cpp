/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mecab_tokenizer.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace fts {

MecabFTSTokenizer::MecabFTSTokenizer() {
}

void MecabFTSTokenizer::reset(StringData document, Options options) {
    _options = std::move(options);
}

bool MecabFTSTokenizer::moveNext() {
    return false;
}

StringData MecabFTSTokenizer::get() const {
    return "DUMMY_MECAB_TOKEN";
}

}  // namespace fts
}  // namespace mongo
