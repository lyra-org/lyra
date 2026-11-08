local fixture = ...

local covers = require("@lyra/covers")

local ids = fixture.ids

-- Releases and artists have their own cover and a track shows its release's. What the caller can't
-- see, and what has no cover, is absent; an artist doesn't take its releases' covers.
local found = covers.get({
	ids.a,
	ids.ta,
	ids.singer,
	ids.secret,
	ids.tsecret,
	ids.bare,
	ids.tbare,
	"missing-target",
})
assert(found[ids.a].hash == "a" and found[ids.a].blurhash == "L00000fQfQfQ")
assert(found[ids.a].owner_id == ids.a and found[ids.a].mime_type == "image/png")
assert(found[ids.a].id ~= "" and (found[ids.a] :: any).path == nil, "a cover names its id, not its file")
assert(found[ids.ta].id == found[ids.a].id, "the id names the cover, whichever item shows it")
assert(found[ids.ta].owner_id == ids.a, "a track shows its release's cover")
assert(found[ids.singer] == nil, "an artist without a cover doesn't take its release's")
assert(found[ids.secret] == nil and found[ids.tsecret] == nil, "unseen releases and tracks are absent")
assert(found[ids.bare] == nil and found[ids.tbare] == nil and found["missing-target"] == nil)

-- A playlist shows a release the caller can see, even when the hidden one would win.
local playlists = covers.get({ ids.shared, ids.private })
assert(playlists[ids.shared].owner_id == ids.a, "the shared playlist shows the release the listener sees")
assert(playlists[ids.private] == nil, "another user's private playlist is absent")

-- A genre shows the caller's most played release by default, and only the instance's pick with
-- `listens = "instance"`; either way one the caller can see.
assert(covers.get({ ids.rock })[ids.rock].owner_id == ids.b, "the listener's own listens pick B")
local instance = covers.get({ ids.rock }, { listens = "instance" })[ids.rock].owner_id
assert(instance == ids.a or instance == ids.b, `the instance pick is a visible release, got {instance}`)
local ok, err = pcall(covers.get, { ids.rock }, { listens = "everyone" } :: any)
assert(not ok and string.find(tostring(err), '"caller", "instance", got "everyone"', 1, true), tostring(err))
ok, err = pcall(covers.get, { ids.rock }, { listen = "instance" } :: any)
assert(not ok and string.find(tostring(err), "options: unknown field 'listen'", 1, true), tostring(err))

-- Artwork reads as the system: every playlist, and genres from the instance's listens.
local artwork = covers.artwork({ ids.private, ids.secret, ids.rock })
assert(artwork[ids.private].owner_id == ids.a and artwork[ids.secret].owner_id == ids.secret)
local system_pick = artwork[ids.rock].owner_id
assert(system_pick == ids.a or system_pick == ids.b or system_pick == ids.secret)
