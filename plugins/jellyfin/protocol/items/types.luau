--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local base = require("./common")

type JsonValue = api.JsonValue

export type UserData = base.UserData
export type MediaStreamInfo = { [string]: JsonValue }
export type MediaSourceInfo = { [string]: JsonValue }
export type PersonType =
	"Unknown"
	| "Actor"
	| "Artist"
	| "AlbumArtist"
	| "Composer"
	| "Writer"
	| "Producer"
	| "Conductor"
	| "Lyricist"
	| "Arranger"
	| "Engineer"
	| "Mixer"
	| "Remixer"

export type ArtistProjectionItem = {
	Id: string,
	Name: string,
}

export type ImageBlurHashes = {
	Primary: { [string]: string },
}

export type PersonItem = {
	Name: string,
	Id: string,
	Role: string?,
	Type: PersonType,
	PrimaryImageTag: string?,
	ImageBlurHashes: ImageBlurHashes?,
}

export type SerializableItemInfo = {
	Id: string?,
	Name: string?,
	Type: string?,
	CollectionType: string?,
	IsFolder: boolean?,
	SortName: string?,
	ChildCount: number?,
	AlbumCount: number?,
	SongCount: number?,
	ArtistCount: number?,
	DateCreated: string?,
	ImageTags: { [string]: string }?,
	LocationType: string?,
	MediaType: string?,
	UserData: UserData?,
	ProductionYear: number?,
	PremiereDate: string?,
	Genres: { string }?,
	GenreItems: { ArtistProjectionItem }?,
	AlbumArtist: string?,
	AlbumArtists: { ArtistProjectionItem }?,
	Artists: { string }?,
	ArtistItems: { ArtistProjectionItem }?,
	People: { PersonItem }?,
	RunTimeTicks: number?,
	ParentPrimaryImageItemId: string?,
	ParentPrimaryImageTag: string?,
	AlbumPrimaryImageTag: string?,
	ImageBlurHashes: ImageBlurHashes?,
	Container: string?,
	IndexNumber: number?,
	ParentIndexNumber: number?,
	Album: string?,
	AlbumId: string?,
	ParentId: string?,
	Artist: string?,
	HasLyrics: boolean?,
	NormalizationGain: number?,
	MediaSources: { MediaSourceInfo }?,
	MediaStreams: { MediaStreamInfo }?,
	CommunityRating: number?,
	PlaylistItemId: string?,
	ServerId: string?,
	Etag: string?,
	Tags: { string }?,
	ProviderIds: { [string]: string }?,
}

return {}
