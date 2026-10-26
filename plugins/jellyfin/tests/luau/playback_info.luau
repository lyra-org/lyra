local fixture = ...

local playback = require("../../protocol/items/playback")

local track = playback.resolve_track_for_playback(fixture.ids.song)
assert(track ~= nil, "owner sees the song")

local function media_source(request: any): any
	local info = playback.build_playback_info(track.id, track, "session", request, nil, "secret")
	assert(info.ErrorCode == nil, `error {info.ErrorCode}`)
	local source = info.MediaSources[1]
	assert(source ~= nil, "the song's source is listed")
	return source
end

local function plays(direct_play_profiles: any): boolean
	return media_source({ DeviceProfile = { DirectPlayProfiles = direct_play_profiles, TranscodingProfiles = {} } }).SupportsDirectPlay
end

-- Direct-play profiles match containers and codecs as upstream's ContainerHelper does.
assert(plays({ { Type = "Audio", Container = "ogg", AudioCodec = "opus" } }), "ogg/opus")
assert(plays({ { Type = "Audio", Container = "-mp3" } }), "anything but mp3")
assert(not plays({ { Type = "Audio", Container = "-mp3,ogg" } }), "anything but mp3 or ogg")
assert(plays({ { Type = "Audio", Container = "ogg", AudioCodec = "-flac" } }), "any codec but flac")
assert(not plays({ { Type = "Audio", Container = "ogg", AudioCodec = "vorbis,flac" } }), "only vorbis or flac")
