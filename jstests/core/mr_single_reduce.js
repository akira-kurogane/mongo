/*
 * See SERVER-68766. Verify that the reduce function is run on a single value.
 *
 * @tags: [
 *   requires_fcv_44,
 *   multiversion_incompatible,
 * ]
 */
(function() {
"use strict";
const coll = db.bar;

assert.commandWorked(coll.insert({x: 1}));

const map = function() {
    emit(0, "mapped value");
};

const reduce = function(key, values) {
    return "reduced value";
};

const res = assert.commandWorked(
    db.runCommand({mapReduce: 'bar', map: map, reduce: reduce, out: {inline: 1}}));
assert.eq(res.results[0], {_id: 0, value: "reduced value"});
}());
