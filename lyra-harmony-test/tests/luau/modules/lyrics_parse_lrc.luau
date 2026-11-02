local lyrics = require("@lyra/lyrics")

local parsed = lyrics.parse_lrc("[00:01.500]Hello\n[00:02.000]<00:02.100>Wide <00:02.500>World", { language = " " })

assert(parsed.language == "und")
assert(parsed.plain_text == "Hello\nWide World")
assert(#parsed.lines == 2)
assert(parsed.lines[1].ts_ms == 1500)
assert(parsed.lines[1].text == "Hello")
assert(parsed.lines[2].text == "Wide World")
assert(#parsed.lines[2].words == 2)
assert(parsed.lines[2].words[1].ts_ms == 2100)

assert(lyrics.parse_lrc("[00:01.00]Bonjour", { language = "French" }).language == "fra", "normalized as stored")
assert(lyrics.parse_lrc("[00:01.00]Bonjour", { language = "UND" }).language == "und")
local ok = pcall(lyrics.parse_lrc, "[00:01.00]Bonjour", { language = "not a language" })
assert(not ok, "an invalid language raises")
assert(lyrics.parse_lrc("[00:01.00]Hello").language == "und")
