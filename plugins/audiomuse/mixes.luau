--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- The AudioMuse mixer: blends through similar tracks and alchemy, and paths through AudioMuse's
-- song paths.

local http = require("@harmony/http")
local mix = require("@lyra/mix")
local serde = require("@harmony/serde")
local tracks = require("@lyra/tracks")

local client = require("./client")

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpResponse = http.HttpResponse
type MixBlendContext = mix.MixBlendContext
type MixPathContext = mix.MixPathContext
type MixResult = mix.MixResult
type MixResultTrack = mix.MixResultTrack
type Options = { [string]: boolean | string | number }

-- Converts between Lyra track ids and the ids AudioMuse knows tracks by on its media server.
export type TrackIds = {
	encode: (track_id: string) -> string?,
	decode: (item_id: string) -> string?,
}

export type MixerConfig = {
	ids: TrackIds,
}

local mixes = {}

-- The most tracks a release or artist seed adds to a blend. Alchemy treats every item alike,
-- so this bounds how much one release or artist outweighs a single track seed.
local SEED_ENTITY_TRACKS = 5
-- The most items one alchemy request carries.
local MAX_ALCHEMY_ITEMS = 100
-- The most explicit seeds a blend takes; recent listens fill the rest. It matches the server's
-- `RECENT_LISTEN_COUNT`, so a history blend can draw on every recent listen, up to
-- `MAX_ALCHEMY_ITEMS` tracks.
local MAX_BLEND_SEEDS = 50
local DEFAULT_LIMIT = 50

-- Maps AudioMuse result rows to tracks, warning about ids that don't decode.
local function result_tracks(ids: TrackIds, label: string, items: { JsonValue }): { MixResultTrack }
	local results: { MixResultTrack } = {}
	local decode_failed = 0
	for _, item in items do
		local item_id = if type(item) == "table" then (item :: JsonObject).item_id else nil
		local track_id = if type(item_id) == "string" then ids.decode(item_id) else nil
		if track_id then
			table.insert(results, { track_id = track_id })
		else
			decode_failed += 1
		end
	end

	-- Encoder/version mismatch; escalate above routine orphans.
	if decode_failed > 0 then
		warn(`{label}: {decode_failed}/{#items} id decode failed — possible ID format mismatch`)
	end

	return results
end

-- The JSON body of a successful response; `{}` for a 404 (seeds not analysed, or not in the
-- index asked for), nil for a backend failure.
local function response_body(label: string, response: HttpResponse): JsonValue?
	if not client.ok(response) then
		if response.success and response.status_code == 404 then
			return {}
		end
		warn(
			`{label} backend failure: status={response.status_code or 0} kind={response.error_kind or ""}: {client.error_message(
				response
			)}`
		)
		return nil
	end

	local body = client.decode(response.body)
	if type(body) ~= "table" then
		warn(`{label} returned non-JSON or non-object body`)
		return nil
	end
	return body
end

-- The array under `key` of a response body; nil when the body failed or the field is no array.
local function body_rows(label: string, body: JsonValue?, key: string?): { JsonValue }?
	if body == nil then
		return nil
	end
	local rows = if key then (body :: JsonObject)[key] else body
	if rows == nil then
		return {}
	end
	if type(rows) ~= "table" then
		warn(`{label} returned non-array results`)
		return nil
	end
	return rows :: { JsonValue }
end

-- An option the request set, as AudioMuse's query string spells it.
local function flag(options: Options, name: string): string?
	local value = options[name]
	if type(value) == "boolean" then
		return if value then "true" else "false"
	end
	return nil
end

local function similar_tracks(config: MixerConfig, item_id: string, n: number): { MixResultTrack }?
	local label = `similar_tracks({item_id})`
	local params: { [string]: string } = { item_id = item_id, n = tostring(n) }
	local rows = body_rows(label, response_body(label, client.get("/api/similar_tracks", params)))
	return if rows then result_tracks(config.ids, label, rows) else nil
end

-- An item of an alchemy blend: a song by AudioMuse item id, added to or subtracted from the blend.
type AlchemyItem = { id: string, op: string, type: string }

local function alchemy(config: MixerConfig, items: { AlchemyItem }, n: number): { MixResultTrack }?
	local label = `alchemy({#items} items)`
	local body: JsonObject = { items = items :: any, n = n }
	local rows = body_rows(label, response_body(label, client.post("/api/alchemy", body)), "results")
	return if rows then result_tracks(config.ids, label, rows) else nil
end

-- The track ids a seed stands for: itself, or the first tracks of its release or artist.
-- Artists are expanded rather than sent as `type = "artist"`, since AudioMuse resolves artist
-- ids through a name map built from each track's album artist and first artist id, which a Lyra
-- artist id doesn't reliably reach.
local function seed_tracks(seed: mix.MixSeed): { string }
	if seed.type == mix.SeedType.Track then
		return { seed.id }
	end
	local seed_track_list: { tracks.Track } = if seed.type == mix.SeedType.Release
		then tracks.by_release({ seed.id })[seed.id] or {}
		else tracks.query({ artist_ids = { seed.id }, credit_role = "track" }).items
	local ids_out: { string } = {}
	for _, track in seed_track_list do
		if #ids_out >= SEED_ENTITY_TRACKS then
			break
		end
		table.insert(ids_out, track.id)
	end
	return ids_out
end

local function without(results: { MixResultTrack }, excluded: { [string]: boolean }): MixResult
	local kept: { MixResultTrack } = {}
	for _, result in results do
		if not excluded[result.track_id] then
			table.insert(kept, result)
		end
	end
	return { tracks = kept }
end

local function track_mix(config: MixerConfig, seed_id: string, limit: number): MixResult
	local item_id = config.ids.encode(seed_id)
	if not item_id then
		warn(`track mix: seed {seed_id} could not be encoded; returning empty mix`)
		return { tracks = {} }
	end
	local results = similar_tracks(config, item_id, limit + 1)
	return without(results or {}, { [seed_id] = true })
end

-- Blends the seeds through AudioMuse's alchemy: excluded seeds subtract, the rest add. Seeds are
-- left out of the mix.
local function alchemy_mix(config: MixerConfig, ctx: MixBlendContext, limit: number): MixResult
	local items: { AlchemyItem } = {}
	local seed_track_ids: { [string]: boolean } = {}
	local adds = 0
	for _, seed in ctx.seeds do
		local op = if seed.exclude then "SUBTRACT" else "ADD"
		for _, track_id in seed_tracks(seed) do
			if #items >= MAX_ALCHEMY_ITEMS then
				break
			end
			local item_id = if seed_track_ids[track_id] then nil else config.ids.encode(track_id)
			if item_id then
				seed_track_ids[track_id] = true
				table.insert(items, { id = item_id, op = op, type = "song" })
				if op == "ADD" then
					adds += 1
				end
			end
		end
	end

	-- Alchemy needs something to add.
	if adds == 0 then
		return { tracks = {} }
	end
	return without(alchemy(config, items, limit) or {}, seed_track_ids)
end

local function blend(config: MixerConfig, ctx: MixBlendContext): MixResult
	local limit = ctx.limit or DEFAULT_LIMIT
	local seeds = ctx.seeds
	if #seeds == 1 and seeds[1].type == mix.SeedType.Track and not seeds[1].exclude then
		return track_mix(config, seeds[1].id, limit)
	end
	return alchemy_mix(config, ctx, limit)
end

local function path(config: MixerConfig, ctx: MixPathContext): MixResult
	local start_id, end_id = ctx.seeds[1].id, ctx.seeds[2].id
	local start_item, end_item = config.ids.encode(start_id), config.ids.encode(end_id)
	if not start_item or not end_item then
		warn(`path: seeds {start_id}, {end_id} could not be encoded; returning empty mix`)
		return { tracks = {} }
	end
	local options: Options = ctx.options or {}
	local params: { [string]: string } = { start_song_id = start_item, end_song_id = end_item }
	if ctx.limit then
		params.max_steps = tostring(ctx.limit)
	end
	if type(options.path_space) == "string" then
		params.path_space = options.path_space
	end
	local fix_size = flag(options, "path_fix_size")
	if fix_size then
		params.path_fix_size = fix_size
	end
	local label = `find_path({start_item}, {end_item})`
	local rows = body_rows(label, response_body(label, client.get("/api/find_path", params)), "path")
	return { tracks = if rows then result_tracks(config.ids, label, rows) else {} }
end

-- Registers the blend and path handlers on `mixer`, and the options they read.
function mixes.register(mixer: mix.Mixer, config: MixerConfig)
	mixer:declare_option({
		name = "path_space",
		label = "Path through",
		type = "choice",
		options = {
			{ value = "audio", label = "Sound" },
			{ value = "lyrics", label = "Lyrics" },
		},
		default = "audio",
	})
	mixer:declare_option({
		name = "path_fix_size",
		label = "Paths take exactly the steps asked for",
		type = "boolean",
	})

	mixer:blend({
		seeds = { mix.SeedType.Track, mix.SeedType.Release, mix.SeedType.Artist },
		max_seeds = MAX_BLEND_SEEDS,
		exclude = true,
	}, function(ctx: MixBlendContext): MixResult
		return blend(config, ctx)
	end)

	mixer:path({ seeds = { mix.SeedType.Track } }, function(ctx: MixPathContext): MixResult
		return path(config, ctx)
	end)
end

return mixes
