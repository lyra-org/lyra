local mix = require("@lyra/mix")

local mixer = mix.Mixer.new("luau-test-mixer")
mixer:from_track(function()
	return { tracks = {} }
end)
mixer:from_release(function()
	return { tracks = {} }
end)
mixer:from_artist(function()
	return { tracks = {} }
end)
mixer:from_recent_listens(function()
	return { tracks = {} }
end)
mixer:from_genre(function()
	return { tracks = {} }
end)
mixer:from_playlist(function()
	return { tracks = {} }
end)
mixer:declare_option({
	name = "source",
	label = "Source",
	type = "string",
})

assert(type(mix.from_track) == "function")
assert(type(mix.from_release) == "function")
assert(type(mix.from_artist) == "function")
assert(type(mix.from_genre) == "function")
assert(type(mix.from_playlist) == "function")
assert(type(mix.instant_mix_from_audio) == "function")
assert(type(mixer.from_track) == "function")
assert(type(mixer.from_recent_listens) == "function")
assert(type(mixer.declare_option) == "function")
