local preferences = require("../../protocol/preferences")

local function post(user_id: string, body: { [string]: any }): number
	local ctx = { request = { json = body, body_raw = "{}" } }
	return preferences.user_configuration_set(ctx :: any, user_id).status :: number
end

local function check(actual: any, expected: any, why: string)
	assert(actual == expected, `{why}: expected {expected}, got {actual}`)
end

-- A user who never saved anything gets upstream's defaults.
local fresh = preferences.user_configuration("fresh")
check(fresh.PlayDefaultAudioTrack, true, "default PlayDefaultAudioTrack")
check(fresh.HidePlayedInLatest, true, "default HidePlayedInLatest")
check(fresh.SubtitleMode, "Default", "default SubtitleMode")
check(fresh.AudioLanguagePreference, nil, "no default audio language")

-- A save replaces the whole configuration, keeps only its own fields and normalises view ids.
check(
	post("saver", {
		AudioLanguagePreference = "OriginalLanguage",
		PlayDefaultAudioTrack = false,
		SubtitleMode = "Smart",
		OrderedViews = { "0123456789ABCDEF-0123456789abcdef" },
		Unknown = "dropped",
	}),
	204,
	"a valid save"
)
local saved = preferences.user_configuration("saver")
check(saved.AudioLanguagePreference, "OriginalLanguage", "saved audio language")
check(saved.PlayDefaultAudioTrack, false, "saved PlayDefaultAudioTrack")
check(saved.SubtitleMode, "Smart", "saved SubtitleMode")
check((saved.OrderedViews :: any)[1], "0123456789abcdef0123456789abcdef", "view ids are normalised")
check(saved.Unknown, nil, "unknown fields are dropped")
check(saved.HidePlayedInLatest, true, "fields the save left out keep their defaults")
check(preferences.user_configuration("fresh").AudioLanguagePreference, nil, "another user's save stays theirs")

check(post("saver", { PlayDefaultAudioTrack = true }), 204, "a second save")
check(preferences.user_configuration("saver").AudioLanguagePreference, nil, "a save replaces the previous one")

-- A field of the wrong type rejects the whole save.
check(post("saver", { PlayDefaultAudioTrack = "yes" }), 400, "a string for a boolean")
check(post("saver", { SubtitleMode = "Sometimes" }), 400, "an unknown subtitle mode")
check(post("saver", { MyMediaExcludes = { "not-an-id" } }), 400, "a malformed view id")
check(post("saver", { OrderedViews = "0123456789abcdef0123456789abcdef" }), 400, "an id list that isn't a list")
check(preferences.user_configuration("saver").PlayDefaultAudioTrack, true, "a rejected save changes nothing")
