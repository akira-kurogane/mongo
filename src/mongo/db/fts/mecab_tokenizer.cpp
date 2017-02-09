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
    _options = std::move(options); //TODO: actually this, or remove it
    _pos = 0;
    _document.resetData(document);  // Validates that document is valid UTF8.

    _skipDelimiters();
}

bool MecabFTSTokenizer::moveNext() {
    while (true) {
        if (_pos >= _document.size()) {
            _word = "";
            return false;
        }

        // Skip the delimiters before the next token.
        _skipDelimiters();

        //DUMMY tokenization to make each char a word
        _word = _document.toLowerToBuf(&_wordBuf, unicode::CaseFoldMode::kNormal, _pos, _pos + 1);

        ++_pos;

        return true;
    }
}

StringData MecabFTSTokenizer::get() const {
    return _word;
}

void MecabFTSTokenizer::_skipDelimiters() {
    while (_pos < _document.size() &&
           unicode::codepointIsDelimiter(
               _document[_pos],
               unicode::DelimiterListLanguage::kEnglish)) {  // TODO: replace with kJapanese?
        ++_pos;
    }
}

}  // namespace fts
}  // namespace mongo
