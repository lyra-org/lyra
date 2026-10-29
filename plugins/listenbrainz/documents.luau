--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Submission documents for the ListenBrainz submit-listens API.

local listens = require("@lyra/listens")
local serde = require("@harmony/serde")

type JsonValue = serde.JsonValue
type ListenPayload = listens.ListenPayload

export type AdditionalInfo = {
	submission_client: string,
	submission_client_version: string,
	duration_ms: number?,
	tracknumber: string?,
	discnumber: string?,
	recording_mbid: string?,
	release_mbid: string?,
	release_group_mbid: string?,
	artist_mbids: { string }?,
}

export type TrackMetadata = {
	artist_name: string,
	track_name: string,
	release_name: string?,
	additional_info: AdditionalInfo,
}

-- A listen ready to submit: what was played, and when.
export type Submission = {
	listen: ListenPayload,
	track_metadata: TrackMetadata,
}

local documents = {}

-- Documents built from typed tables are JSON by construction; Luau can't see that through
-- `JsonValue`.
local function as_json(value: any): JsonValue
	return value
end

-- ListenBrainz refuses more listens than this in one request.
documents.MAX_LISTENS_PER_REQUEST = 1000

function documents.playing_now(track_metadata: TrackMetadata): JsonValue
	return {
		listen_type = "playing_now",
		payload = {
			{
				track_metadata = track_metadata,
			},
		},
	}
end

-- The requests that submit `submissions`: a `single` for one listen, `import` batches otherwise.
-- ListenBrainz takes `listened_at` as the play's start, in Unix seconds.
function documents.listens(submissions: { Submission }): { JsonValue }
	local requests: { JsonValue } = {}
	local listen_type = if #submissions == 1 then "single" else "import"
	for first = 1, #submissions, documents.MAX_LISTENS_PER_REQUEST do
		local payload: { JsonValue } = {}
		for index = first, math.min(first + documents.MAX_LISTENS_PER_REQUEST - 1, #submissions) do
			local submission = submissions[index]
			table.insert(
				payload,
				as_json({
					listened_at = math.floor(submission.listen.started_at_ms / 1000),
					track_metadata = submission.track_metadata,
				})
			)
		end
		table.insert(
			requests,
			as_json({
				listen_type = listen_type,
				payload = payload,
			})
		)
	end
	return requests
end

return documents
