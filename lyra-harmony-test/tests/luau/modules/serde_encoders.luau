local serde = require("@harmony/serde")

assert(serde.MAX_BUFFERED_BYTES == 4 * 1024 * 1024, `MAX_BUFFERED_BYTES {serde.MAX_BUFFERED_BYTES}`)

-- A document written in pieces decodes as the whole value does.
local items = {}
for index = 1, 5 do
	table.insert(items, { id = `t{index}`, n = index, tags = { "a", "b" } })
end
local json = serde.json.encoder()
local pieces = {}
table.insert(pieces, "callback(")
json:begin_object()
json:begin_object("result")
json:begin_array("item")
for start = 1, #items, 2 do
	json:items({ items[start], items[start + 1] })
	table.insert(pieces, json:flush())
end
json:end_array()
json:end_object()
json:field("status", "ok")
json:end_object()
table.insert(pieces, json:finish())
table.insert(pieces, ");")
local written = table.concat(pieces)
local expected = `callback({serde.json.encode({ result = { item = items }, status = "ok" })});`
assert(written == expected, written)

-- An element left empty closes on its own, even after a flush.
local xml = serde.xml.encoder()
xml:begin_element("response", { status = "ok", version = "1.0" })
xml:begin_element("list")
local first = xml:flush()
xml:end_element()
xml:end_element()
local document = first .. xml:finish()
assert(document == '<response status="ok" version="1.0"><list/></response>', document)

local filled = serde.xml.encoder()
filled:begin_element("list")
filled:elements("item", { { ["@id"] = "a&b" }, { ["@id"] = "c" } })
filled:end_element()
local list = filled:finish()
assert(list == serde.xml.encode("list", { item = { { ["@id"] = "a&b" }, { ["@id"] = "c" } } }), list)

-- A failure leaves the encoder failed, so nothing can finish the document.
local broken = serde.json.encoder()
broken:begin_array()
assert(not pcall(broken.items, broken, { function() end }))
local ok, err = pcall(broken.end_array, broken)
assert(not ok and string.find(tostring(err), "failed earlier", 1, true), tostring(err))
