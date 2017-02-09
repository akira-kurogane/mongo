/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/fts/fts_language.h"
#include "mongo/db/fts/mecab_language.h"
#include "mongo/db/fts/mecab_tokenizer.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace fts {

std::vector<std::string> tokenizeString(const char* str, FTSTokenizer::Options options) {
    StatusWithFTSLanguage swl = FTSLanguage::make("japanese", TEXT_INDEX_VERSION_3);
    ASSERT_OK(swl);

    MecabFTSTokenizer tokenizer;

    tokenizer.reset(str, options);

    std::vector<std::string> terms;

    while (tokenizer.moveNext()) {
        terms.push_back(tokenizer.get().toString());
    }

    return terms;
}

TEST(MecabTokenizer, JapaneseSumomo) {
    std::vector<std::string> terms =
        tokenizeString("すもももももももものうち", FTSTokenizer::kNone);

    ASSERT_EQUALS(4U, terms.size());
    ASSERT_EQUALS("すもも", terms[0]);
    //ASSERT_EQUALS("も", terms[1]);
    ASSERT_EQUALS("もも", terms[2]);
    //ASSERT_EQUALS("も", terms[3]);
    ASSERT_EQUALS("もも", terms[4]);
    //ASSERT_EQUALS("の", terms[5]);
    ASSERT_EQUALS("うち", terms[6]);
}

TEST(MecabTokenizer, JapaneseTaroJiro) {
    std::vector<std::string> terms =
        tokenizeString("太郎は次郎が持っている本を花子に渡した。", FTSTokenizer::kNone);

    ASSERT_EQUALS(7U, terms.size());
    ASSERT_EQUALS("太郎", terms[0]);
    // ASSERT_EQUALS("は", terms[1]);
    ASSERT_EQUALS("次郎", terms[2]);
    // ASSERT_EQUALS("が", terms[3]);
    ASSERT_EQUALS("持つ", terms[4]);  // N.b. not "持っ"
    // ASSERT_EQUALS("て", terms[5]);
    // ASSERT_EQUALS("いる", terms[6]);
    ASSERT_EQUALS("本", terms[7]);
    // ASSERT_EQUALS("を", terms[8]);
    ASSERT_EQUALS("花", terms[9]);
    ASSERT_EQUALS("子", terms[10]);
    // ASSERT_EQUALS("に", terms[11]);
    ASSERT_EQUALS("渡す", terms[12]);  // N.b. not "渡し"
    // ASSERT_EQUALS("た", terms[13]);
    // ASSERT_EQUALS("。", terms[14]);
}


}  // namespace fts
}  // namespace mongo
