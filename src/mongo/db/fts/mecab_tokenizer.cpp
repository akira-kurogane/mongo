/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"
#include "mongo/util/text.h"
#include "mongo/logger/log_severity.h"
#include "mongo/util/log.h"

#include "mecab_tokenizer.h"

#include "mongo/util/mongoutils/str.h"

#include <mecab.h>

namespace mongo {
namespace fts {

MeCab::Model *model;
MeCab::Tagger *tagger;
MeCab::Lattice *lattice;
const MeCab::Node* node;

std::string _lastFeatureField7; // var to cache the data of node->feature between moveNext() and get() calls

MecabFTSTokenizer::MecabFTSTokenizer() {
    model = MeCab::createModel(0, (char**)NULL);  // should only be one globally; TODO move to mecab_init.cpp etc.
    tagger = MeCab::createTagger("");
    lattice = model->createLattice();
}

void MecabFTSTokenizer::reset(StringData document, Options options) {
    _options = std::move(options); //TODO: actually use this, or remove it
    _pos = 0;
    _document = document;
    //TODO: set to lowercase in case non-japanese present. unicode::CaseFoldMode::kNormal
    lattice->set_sentence(_document.rawData());
    tagger->parse(lattice);
    node = lattice->bos_node()->next;  // the "Beginning-Of-Sentence" node is not the first real token
}

bool MecabFTSTokenizer::moveNext() {
    if (!node) {
        return false;
    }
    while (node &&
           (node->stat != MECAB_NOR_NODE || strncmp(node->feature, "助詞", 6) == 0 ||
            strncmp(node->feature, "記号", 6) == 0 ||
            strncmp(node->feature, "助動詞", 9) == 0 ||
            strncmp(node->surface, "いる", 6) == 0)) {
        node = node->next;
    }
    if (!node) {
        return false;
    }
    if (strncmp(node->feature, "動詞", 6) == 0) {
        std::vector<std::string> x = StringSplitter::split(node->feature, ",");
        _lastFeatureField7 = x[6];
        _word = _lastFeatureField7;
    } else {
        _word = _document.substr(node->surface - lattice->sentence(), node->length);
    }
    node = node->next;
    return true;
}

StringData MecabFTSTokenizer::get() const {
    return _word;
}

MecabFTSTokenizer::~MecabFTSTokenizer() {
    delete lattice;
    delete tagger;
    delete model;
}

}  // namespace fts
}  // namespace mongo
