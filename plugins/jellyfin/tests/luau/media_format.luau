local fixture = ...

local device_profiles = require("../../protocol/items/device_profile")
local media = require("../../protocol/items/media")
local playback = require("../../protocol/items/playback")

-- The public stream reads omit the file's path, as reads for anyone who can't manage libraries do.
local track = playback.resolve_track_for_public_stream(fixture.ids.song)
assert(track ~= nil, "song resolves")
local source = playback.build_audio_media_source(track.id, track, true)
assert(source ~= nil, "song has a media source")
assert(source.Path == nil, `path {source.Path}`)

-- Format comes from the scanned contents, not the path.
assert(source.Container == "ogg", `container {source.Container}`)
assert(source.MediaStreams[1].Codec == "opus", `codec {source.MediaStreams[1].Codec}`)
assert(source.SupportsDirectPlay, "direct play without a path")
-- Upstream sets no DirectStreamUrl for library audio.
assert(source.DirectStreamUrl == nil, `direct stream url {source.DirectStreamUrl}`)

-- Jellyfin names containers after FFmpeg's demuxers.
assert(media.jellyfin_container({ container = "wavpack" } :: any) == "wv")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack8" } :: any) == "mpc8")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack7" } :: any) == "mpc")
assert(media.jellyfin_container({ container = "m4a", codec = "alac" } :: any) == "m4a")
assert(media.jellyfin_container(nil) == nil)

-- Playback sources keep the whole list FFmpeg's demuxer goes by.
assert(media.probed_container({ container = "m4a", codec = "aac" } :: any) == "mov,mp4,m4a,3gp,3g2,mj2")
assert(media.probed_container({ container = "wavpack" } :: any) == "wv")

-- An item's media source reports a many-named format by the file's extension, or else its first
-- name, while the item keeps the whole list.
local function item_container(entry: any): string?
	return media.item_source_container({ container = "m4a", entry = entry } :: any)
end
assert(item_container({ full_path = "/music/a.b/song.m4a", name = "song.m4a" }) == "m4a")
assert(item_container({ full_path = "/music/song.MP4", name = "song.MP4" }) == "mp4", "any case")
assert(item_container({ full_path = "/music/book.m4b", name = "book.m4b" }) == "m4b", "audiobooks")
assert(item_container({ name = "song.mp4" }) == "mp4", "the name stands in for a path the caller can't see")
assert(item_container({ full_path = "/music/a.m4a/song", name = "song" }) == "mov", "a file without an extension")
assert(item_container({ full_path = "/music/song.aac", name = "song.aac" }) == "mov", "an unlisted extension")
assert(item_container(nil) == "m4a", "without the file, the format as Lyra names it")
assert(media.item_source_container({ container = "wavpack", entry = { name = "song.m4a" } } :: any) == "wv")
assert(media.item_source_container(nil) == nil)

-- A listing goes through the same, with the file's entry read only when media sources are asked for.
local catalog = require("../../protocol/items/catalog")
local options_module = require("../../protocol/items/options")
local book = playback.resolve_track_for_public_stream(fixture.ids.book)
assert(book ~= nil, "book resolves")
local function listed(fields: string): any
	local options = options_module.for_user(options_module.parse({ Fields = { fields } } :: any), fixture.users.viewer)
	return catalog.realize(catalog.track_stubs({ book }), options)[1]
end
local with_sources = listed("MediaSources")
assert(with_sources.Container == "mov,mp4,m4a,3gp,3g2,mj2", `item container {with_sources.Container}`)
assert(with_sources.MediaSources[1].Container == "m4b", `source container {with_sources.MediaSources[1].Container}`)
local without_sources = listed("Genres")
assert(without_sources.MediaSources == nil)
assert(without_sources.Container == "mov,mp4,m4a,3gp,3g2,mj2", "the item's container needs no entry")

-- Playback info keeps the whole list.
assert(
	playback.build_audio_media_source(book.id, book, true).Container == "mov,mp4,m4a,3gp,3g2,mj2",
	"PlaybackInfo reports the source as probed"
)

-- Profile lists match any of the source's names; a leading `-` negates them.
assert(device_profiles.contains_container("m4a", "mov,mp4,m4a,3gp,3g2,mj2"))
assert(device_profiles.contains_container("MP4,flac", "mov,mp4,m4a"))
assert(not device_profiles.contains_container("aac", "mov,mp4,m4a"))
assert(device_profiles.contains_container("-mkv", "ogg"))
assert(not device_profiles.contains_container("-mkv,ogg", "ogg"))
assert(device_profiles.contains_container(nil, "ogg") and device_profiles.contains_container("", "ogg"))
assert(not device_profiles.contains_container(nil, nil), "an unknown value matches only a negative list")
assert(device_profiles.contains_container("-mp3", nil))
