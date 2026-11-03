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
