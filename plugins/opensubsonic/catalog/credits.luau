--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local entities = require("@lyra/entities")

type JsonObject = { [string]: any }
type CreditedArtistProjectionInfo = entities.CreditedArtistProjectionInfo
type EntityProjectionInfo = entities.EntityProjectionInfo

export type ArtistRef = { id: string, name: string, sortName: string }

-- Credit types Subsonic files under the `performer` role, with the credit's detail as its sub-role.
local PERFORMER_TYPES: { [string]: boolean } = {
	vocalist = true,
	instrumentalist = true,
}

local module = {}

local function ref(artist: entities.Artist): ArtistRef
	return { id = artist.id, name = artist.artist_name, sortName = artist.sort_name or "" }
end

-- The projections of these entities the caller can see, keyed by id.
function module.projections(ids: { string }, include: { string }): { [string]: EntityProjectionInfo }
	if #ids == 0 then
		return {}
	end
	return entities.query_many({ ids = ids, include = include })
end

local function credits_of(projection: EntityProjectionInfo?): { CreditedArtistProjectionInfo }
	if projection == nil then
		return {}
	end
	return (projection :: any).includes.credits or {}
end

-- Lyra keeps no credit join phrases, so the names join with commas.
local function display(refs: { ArtistRef }): string?
	if #refs == 0 then
		return nil
	end
	local names: { string } = {}
	for _, ref in refs do
		table.insert(names, ref.name)
	end
	return table.concat(names, ", ")
end

-- The `ArtistID3` refs of the artists credited as `artist`, in credit order, and their display
-- name; nil for an entity without any.
function module.artists(projection: EntityProjectionInfo?): ({ ArtistRef }?, string?)
	local refs: { ArtistRef } = {}
	local seen: { [string]: boolean } = {}
	for _, credited in credits_of(projection) do
		local artist = credited.artist
		if credited.credit.type == "artist" and not seen[artist.id] then
			seen[artist.id] = true
			table.insert(refs, ref(artist))
		end
	end
	if #refs == 0 then
		return nil, nil
	end
	return refs, display(refs)
end

-- The `Contributor` list of every credit but `artist`, and the composers' display name.
function module.contributors(projection: EntityProjectionInfo?): ({ any }?, string?)
	local contributors: { any } = {}
	local composers: { ArtistRef } = {}
	for _, credited in credits_of(projection) do
		local credit_type = credited.credit.type
		if credit_type == "artist" then
			continue
		end
		local artist = credited.artist
		local contributor: JsonObject = {
			role = if PERFORMER_TYPES[credit_type] then "performer" else credit_type,
			artist = ref(artist),
		}
		if PERFORMER_TYPES[credit_type] then
			contributor.subRole = credited.credit.detail
		end
		table.insert(contributors, contributor)
		if credit_type == "composer" then
			table.insert(composers, ref(artist))
		end
	end
	return if #contributors > 0 then contributors else nil, display(composers)
end

return module
