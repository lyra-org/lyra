local fixture, harness = ...
local call = require("./_call")(harness)
local serde = require("@harmony/serde")

local ids = fixture.ids
local key = fixture.api_keys.listener_key
local friend = fixture.api_keys.friend_key

local function queue_of(api_key: string): any
	return call.ok("getPlayQueue", api_key).playQueue
end

-- A caller without a queue has none, in JSON and XML alike.
assert(queue_of(friend) == nil, "no queue")
local unsaved_xml = call.raw("getPlayQueue", { apiKey = friend }).body
assert(
	string.find(unsaved_xml, 'status="ok"', 1, true) and not string.find(unsaved_xml, "playQueue", 1, true),
	unsaved_xml
)

-- A queue played in Lyra's own client comes back too, of the songs still visible: a hidden current
-- song hands over to the next one, from its start.
local web = queue_of(key)
call.same(call.ids(web.entry), { ids.one, ids.two }, "visible entries")
assert(web.current == ids.two and web.position == 0, `handed to {web.current} at {web.position}`)
assert(web.changed == "2023-11-14T22:13:20Z" and web.changedBy == "Lyra Web", "the web client's change")

-- A saved queue keeps its order and repeats, with the client that saved it.
call.ok("savePlayQueue", key, { id = { ids.two, ids.one, ids.two }, current = ids.two, position = "1500", c = "Phone" })
local saved = queue_of(key)
call.same(call.ids(saved.entry), { ids.two, ids.one, ids.two }, "saved entries")
assert(saved.current == ids.two and saved.position == 1500, `current {saved.current} at {saved.position}`)
local username = saved.username
assert(type(username) == "string" and username ~= "", "the caller's username")
assert(saved.changedBy == "Phone", `changed by {saved.changedBy}`)
assert(string.match(saved.changed, "^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$"), `changed {saved.changed}`)
assert(saved.entry[1].title == "Two" and saved.entry[1].isDir == false and saved.entry[1].artist == "Amy", "songs")

-- Each user has queues of their own.
assert(queue_of(friend) == nil, "another user's queue")

-- The newest queue of any client comes back; `position` defaults to 0, and a form post saves too.
call.ok("savePlayQueue", key, { id = { ids.one, ids.three }, current = ids.three, c = "Desktop" })
local moved = queue_of(key)
call.same(call.ids(moved.entry), { ids.one, ids.three }, "the newest entries")
assert(moved.current == ids.three and moved.position == 0 and moved.changedBy == "Desktop", "no position")
local posted = call.raw("savePlayQueue", {}, {
	http_method = "POST",
	headers = { ["Content-Type"] = "application/x-www-form-urlencoded" },
	body = `apiKey={key}&f=json&v=1.16.1&c=Web&id={ids.three}&id={ids.two}&current={ids.two}&position=7`,
	bare = true,
})
assert((serde.json.decode(posted.body) :: any)["subsonic-response"].status == "ok", posted.body)
local from_form = queue_of(key)
call.same(call.ids(from_form.entry), { ids.three, ids.two }, "form entries")
assert(from_form.current == ids.two and from_form.position == 7 and from_form.changedBy == "Web", "form fields")

-- A queue needs a current song among its songs, each one the caller sees; nothing is saved.
local function fails(code: number, params: { [string]: any }, why: string)
	params.c = "Web"
	assert(call.error_code("savePlayQueue", key, params) == code, why)
end
fails(10, { id = ids.one }, "a queue needs current")
fails(0, { id = ids.one, current = ids.two }, "current outside the queue")
fails(70, { id = { ids.one, ids.hidden }, current = ids.one }, "hidden")
fails(70, { id = { ids.one, ids.album }, current = ids.one }, "an album")
fails(0, { id = ids.one, current = ids.one, position = "soon" }, "position")
fails(70, { id = "" }, "an empty id names no song")
fails(70, { id = { ids.one, "" }, current = ids.one }, "nor among others")
call.same(call.ids(queue_of(key).entry), { ids.three, ids.two }, "failed saves keep the queue")

-- A queue longer than Lyra keeps is cut to the songs around the current one.
local long = table.create(1200, ids.one)
long[601] = ids.two
call.ok("savePlayQueue", key, { id = long, current = ids.two, c = "Web" })
local cut = queue_of(key)
assert(#cut.entry == 1000, `kept {#cut.entry} songs`)
assert(cut.entry[401].id == ids.two and cut.current == ids.two, "100 songs before the current one stay")

-- Saving without songs deletes the client's queue; the newest of another client's comes back.
call.ok("savePlayQueue", key, { c = "Web" })
assert(queue_of(key).changedBy == "Desktop", "another client's queue")
call.ok("savePlayQueue", key, { c = "Desktop" })
call.ok("savePlayQueue", key, { c = "Phone" })
assert(queue_of(key).changedBy == "Lyra Web", "the web client's queue stays")

-- Once no client has a queue, there is none.
call.ok("savePlayQueue", friend, { id = ids.one, current = ids.one, c = "Phone" })
assert(queue_of(friend).current == ids.one, "the friend's queue")
call.ok("savePlayQueue", friend, { c = "Phone" })
assert(queue_of(friend) == nil, "no queue left")

-- By index, the queue names its current song by its place among the songs still visible.
local traveler = fixture.api_keys.traveler_key
local wanderer = fixture.api_keys.wanderer_key
local function indexed(api_key: string): any
	return call.ok("getPlayQueueByIndex", api_key).playQueueByIndex
end

assert(indexed(friend) == nil, "no indexed queue")
local unsaved_indexed_xml = call.raw("getPlayQueueByIndex", { apiKey = friend }).body
assert(not string.find(unsaved_indexed_xml, "playQueueByIndex", 1, true), unsaved_indexed_xml)

-- Hidden songs before the current one move it up, and it keeps its position.
local travel = indexed(traveler)
call.same(call.ids(travel.entry), { ids.one, ids.two, ids.one }, "visible travel entries")
assert(travel.currentIndex == 1 and travel.position == 5000, `travel at {travel.currentIndex}, {travel.position}`)
assert(travel.current == nil and travel.changedBy == "Car" and travel.username ~= nil, "travel fields")
local by_id = queue_of(traveler)
assert(by_id.current == ids.two and by_id.position == 5000 and by_id.currentIndex == nil, "the same queue by id")

-- A hidden current song hands over to the next one, or to the last when none follows, from its start.
local wander = indexed(wanderer)
call.same(call.ids(wander.entry), { ids.one, ids.two }, "visible wander entries")
assert(wander.currentIndex == 1 and wander.position == 0, `wander at {wander.currentIndex}, {wander.position}`)
assert(queue_of(wanderer).current == ids.two, "the last song by id")
local listener_indexed = indexed(key)
assert(listener_indexed.currentIndex == 1 and listener_indexed.position == 0, "the web queue, handed over")

-- An index tells repeats apart, which `current` can't.
call.ok("savePlayQueueByIndex", friend, {
	id = { ids.one, ids.two, ids.one },
	currentIndex = "2",
	position = "2500",
	c = "Phone",
})
local repeated = indexed(friend)
call.same(call.ids(repeated.entry), { ids.one, ids.two, ids.one }, "indexed entries")
assert(repeated.currentIndex == 2 and repeated.position == 2500, `at {repeated.currentIndex}`)
assert(repeated.changedBy == "Phone", "the saving client")
assert(queue_of(friend).current == ids.one, "by id, the repeated song")
-- A queue saved by id reads back by index, at the first of the songs `current` repeats.
call.ok("savePlayQueue", friend, { id = { ids.two, ids.one, ids.two }, current = ids.two, c = "Phone" })
assert(indexed(friend).currentIndex == 0 and indexed(friend).position == 0, "an id's first place")

-- The index is required with songs, within the queue, and an integer; nothing is saved otherwise.
local function index_fails(code: number, params: { [string]: any }, why: string)
	params.c = "Phone"
	assert(call.error_code("savePlayQueueByIndex", friend, params) == code, why)
end
index_fails(10, { id = ids.one }, "a queue needs currentIndex")
index_fails(10, { id = ids.one, currentIndex = "" }, "an empty currentIndex")
index_fails(10, { id = { ids.one, ids.two }, currentIndex = "2" }, "past the end")
index_fails(10, { id = ids.one, currentIndex = "-1" }, "before the start")
index_fails(0, { id = ids.one, currentIndex = "first" }, "an integer")
index_fails(70, { id = { ids.one, ids.hidden }, currentIndex = "0" }, "a hidden song")
index_fails(70, { id = { ids.one, "" }, currentIndex = "0" }, "an empty id")
index_fails(0, { id = ids.one, currentIndex = "0", position = "soon" }, "position")
call.same(call.ids(indexed(friend).entry), { ids.two, ids.one, ids.two }, "failed indexed saves keep the queue")

-- A long queue is cut around the current song, its index moving with the cut.
local long_indexed = table.create(1200, ids.one)
call.ok("savePlayQueueByIndex", friend, { id = long_indexed, currentIndex = "1100", c = "Phone" })
local cut_indexed = indexed(friend)
assert(#cut_indexed.entry == 1000, `kept {#cut_indexed.entry} songs`)
assert(cut_indexed.currentIndex == 900, `cut to {cut_indexed.currentIndex}`)
call.ok("savePlayQueueByIndex", friend, { id = long_indexed, currentIndex = "5", c = "Phone" })
assert(indexed(friend).currentIndex == 5, "a current song near the start stays")

-- A form post saves too.
local indexed_post = call.raw("savePlayQueueByIndex", {}, {
	http_method = "POST",
	headers = { ["Content-Type"] = "application/x-www-form-urlencoded" },
	body = `apiKey={friend}&f=json&v=1.16.1&c=Web&id={ids.three}&id={ids.three}&currentIndex=1&position=9`,
	bare = true,
})
assert((serde.json.decode(indexed_post.body) :: any)["subsonic-response"].status == "ok", indexed_post.body)
local posted_indexed = indexed(friend)
assert(posted_indexed.currentIndex == 1 and posted_indexed.position == 9 and posted_indexed.changedBy == "Web", "form")

-- Saving without songs deletes the client's queue, without a currentIndex.
call.ok("savePlayQueueByIndex", friend, { c = "Web" })
call.ok("savePlayQueueByIndex", friend, { c = "Phone" })
assert(indexed(friend) == nil, "no indexed queue left")

local indexed_xml = call.raw("getPlayQueueByIndex", { apiKey = traveler }).body
assert(string.find(indexed_xml, '<playQueueByIndex changed="', 1, true), indexed_xml)
assert(string.find(indexed_xml, 'currentIndex="1"', 1, true), indexed_xml)

local extensions = call.raw("getOpenSubsonicExtensions", { f = "json" }, { bare = true }).body
local advertised = false
for _, extension in (serde.json.decode(extensions) :: any)["subsonic-response"].openSubsonicExtensions do
	if extension.name == "indexBasedQueue" then
		advertised = #extension.versions == 1 and extension.versions[1] == 1
	end
end
assert(advertised, "indexBasedQueue version 1")
