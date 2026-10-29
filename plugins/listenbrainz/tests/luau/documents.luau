local documents = require("../../documents")

local track_metadata: documents.TrackMetadata = {
	artist_name = "Artist",
	track_name = "Track",
	additional_info = {
		submission_client = "Lyra ListenBrainz Plugin",
		submission_client_version = "0.0.1",
		duration_ms = 200000,
	},
}

local function submission(index: number): documents.Submission
	return {
		listen = {
			listen_id = `listen-{index}`,
			track_id = "track",
			started_at_ms = 1800000000999 + index * 1000,
			played_ms = 95500,
		},
		track_metadata = track_metadata,
	}
end

local single = documents.listens({ submission(0) }) :: any
assert(#single == 1, `one request, got {#single}`)
assert(single[1].listen_type == "single", `listen_type {single[1].listen_type}`)
local listen = single[1].payload[1]
assert(listen.listened_at == 1800000000, `listened_at is the play's start: {listen.listened_at}`)
assert(listen.track_metadata.track_name == "Track", "track metadata is carried")
assert(listen.track_metadata.additional_info.duration_played == nil, "no played duration is claimed")

local many: { documents.Submission } = {}
for index = 1, documents.MAX_LISTENS_PER_REQUEST + 1 do
	table.insert(many, submission(index))
end
local batches = documents.listens(many) :: any
assert(#batches == 2, `split at the request cap, got {#batches}`)
assert(batches[1].listen_type == "import", `listen_type {batches[1].listen_type}`)
assert(#batches[1].payload == documents.MAX_LISTENS_PER_REQUEST, `first batch {#batches[1].payload}`)
assert(#batches[2].payload == 1, `second batch {#batches[2].payload}`)
assert(batches[2].payload[1].listened_at == 1800000000 + documents.MAX_LISTENS_PER_REQUEST + 1, "order is kept")

assert(#documents.listens({}) == 0, "no listens, no request")

local playing_now = documents.playing_now(track_metadata) :: any
assert(playing_now.listen_type == "playing_now", `listen_type {playing_now.listen_type}`)
assert(playing_now.payload[1].listened_at == nil, "playing_now carries no listen time")
