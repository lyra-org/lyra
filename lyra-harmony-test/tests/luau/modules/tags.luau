local fixture = ...

local tags = require("@lyra/tags")

local ids = fixture.ids
local missing = "missing-target"

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

assert(tags.add(ids.song, "  Mood ", "blue") == "Mood", "add returns the canonical name")
assert(tags.add(ids.album, "Mood", "red") == "Mood", "a release is taggable")
local tag_node = tags.get_for_target(ids.song)[1].id

for _, id in { ids.hidden, ids.theirs, missing, tag_node } do
	assert(tags.add(id, "Mood", "blue") == nil, `{id} can't be tagged`)
end

assert(tags.has(ids.song, "Mood"), "an attached tag is present")
assert(not tags.has(ids.plain, "Mood"), "an untagged track is absent")
assert(not tags.has(ids.hidden, "Mood"), "a hidden target's tag is absent")
assert(not tags.has(missing, "Mood"), "a missing target is absent")
assert(not tags.has(tag_node, "Mood"), "a tag node is absent")
assert(tags.has(ids.song, "  Mood "), "has normalizes the name")
for _, id in { 1, true } do
	rejects(tags.has, "expected string", id, "Mood")
	rejects(tags.get_for_target, "expected string", id)
	rejects(tags.add, "expected string", id, "Mood", "blue")
	rejects(tags.remove, "expected string", id, "Mood")
	rejects(tags.has_many, "must be a string", { ids.song, id }, "Mood")
end

local many = tags.has_many({ ids.song, ids.song, ids.plain, ids.hidden, missing, tag_node }, "Mood")
assert(many[ids.song] == true, "song")
assert(many[ids.plain] == false, "an untagged track maps to false")
for _, id in { ids.hidden, missing, tag_node } do
	assert(many[id] == nil, `{id} is absent`)
end

assert(#tags.get_for_target(ids.hidden) == 0, "a hidden target lists no tags")
assert(#tags.get_for_target(missing) == 0, "a missing target lists no tags")
local by_target = tags.get_for_targets_many({ ids.song, ids.plain, ids.hidden, ids.theirs, missing, tag_node })
assert(#by_target[ids.song] == 1 and by_target[ids.song][1].tag == "Mood", "song tags")
assert(by_target[ids.plain] and #by_target[ids.plain] == 0, "an untagged track has none")
for _, id in { ids.hidden, ids.theirs, missing, tag_node } do
	assert(by_target[id] == nil, `{id} is absent`)
end

local tagged = tags.get_tagged("Mood")
assert(#tagged == 2, `get_tagged hides the hidden target, got {#tagged}`)

tags.remove(ids.hidden, "Mood")
tags.remove(ids.song, "  Mood ")
tags.remove(ids.song, "Mood")
assert(not tags.has(ids.song, "Mood"), "a removed tag is gone")
tags.remove(missing, "Mood")
rejects(tags.remove, "target is not a supported kind", tag_node, "Mood")
