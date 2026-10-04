local playlists = require("@lyra/playlists")

-- A test may make more database calls than one tokio task budget allows.
for _ = 1, 500 do
	playlists.list_owned()
end
