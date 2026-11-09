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
local tag_node = tags.by_target({ ids.song })[ids.song][1].id

for _, id in { ids.hidden, ids.theirs, missing, tag_node } do
	assert(tags.add(id, "Mood", "blue") == nil, `{id} can't be tagged`)
end

for _, id in { 1, true } do
	rejects(tags.add, "expected string", id, "Mood", "blue")
	rejects(tags.remove, "expected string", id, "Mood")
	rejects(tags.by_target, "must be a string", { ids.song, id })
end

local by_target = tags.by_target({ ids.song, ids.song, ids.plain, ids.hidden, ids.theirs, missing, tag_node })
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
assert(#tags.by_target({ ids.song })[ids.song] == 0, "a removed tag is gone")
tags.remove(missing, "Mood")
rejects(tags.remove, "target is not a supported kind", tag_node, "Mood")
