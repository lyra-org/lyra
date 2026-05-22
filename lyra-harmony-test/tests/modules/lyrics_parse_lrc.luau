local lyrics = require("@lyra/lyrics")

local parsed = lyrics.parse_lrc("[00:01.500]Hello\n[00:02.000]<00:02.100>Wide <00:02.500>World", " ")
local exports =
	`{type(lyrics.get)}:{type(lyrics.parse_lrc)}:{type(lyrics.upsert)}:{type(lyrics.upsert_user_override)}:{type(
		lyrics.delete_user_override_for_track
	)}:{type(lyrics.delete_for_track)}:{type(lyrics.has)}:{type(lyrics.has_many)}`

assert(exports == "function:function:function:function:function:function:function:function")
assert(parsed.id == "")
assert(parsed.language == "und")
assert(parsed.plain_text == "Hello\nWide World")
assert(#parsed.lines == 2)
assert(parsed.lines[1].ts_ms == 1500)
assert(parsed.lines[1].text == "Hello")
assert(parsed.lines[2].text == "Wide World")
assert(#parsed.lines[2].words == 2)
assert(parsed.lines[2].words[1].ts_ms == 2100)
