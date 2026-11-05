local serde = require("@harmony/serde")

assert(serde.MAX_ENCODED_BYTES == 16 * 1024 * 1024, `MAX_ENCODED_BYTES {serde.MAX_ENCODED_BYTES}`)

local function refused(expected: string, ok: boolean, err: unknown)
	assert(not ok, "encoding should fail")
	assert(string.find(tostring(err), expected, 1, true), tostring(err))
end

local too_large = `encodes to more than {serde.MAX_ENCODED_BYTES} bytes`
local overfull = "flush it more often"

-- Each level holds the one below twice, so the whole encodes to 2^40 leaves.
local chain: any = { "leaf" }
for _ = 1, 40 do
	chain = { a = chain, b = chain }
end

refused(too_large, pcall(serde.json.encode, chain))
refused(too_large, pcall(serde.yaml.encode, chain))
refused(too_large, pcall(serde.xml.encode, "root", chain))

-- One big string many times over is as large as its copies.
local big = string.rep("x", 1024 * 1024)
local copies = {}
for index = 1, 20 do
	copies[index] = big
end
refused(too_large, pcall(serde.json.encode, copies))

-- An encoder refuses a piece that would leave it holding more than it may.
local json = serde.json.encoder()
json:begin_object()
refused(overfull, pcall(json.field, json, "chain", chain))
local ok, err = pcall(json.end_object, json)
assert(not ok and string.find(tostring(err), "failed earlier", 1, true), tostring(err))

local xml = serde.xml.encoder()
xml:begin_element("root")
refused(overfull, pcall(xml.elements, xml, "chain", { chain }))
ok, err = pcall(xml.end_element, xml)
assert(not ok and string.find(tostring(err), "failed earlier", 1, true), tostring(err))

-- A table repeated in the output is still read once: the empty leaf's million slots are scanned
-- once, not once for each of its 2^20 places.
local sparse: any = table.create(1000000)
for _ = 1, 20 do
	sparse = { sparse, sparse }
end
local encoded_sparse = serde.json.encode(sparse)
assert(string.sub(encoded_sparse, 1, 22) == string.rep("[", 20) .. "[]", string.sub(encoded_sparse, 1, 22))

-- A string repeated in one table is copied once and refused as soon as its copies outgrow the
-- limit, on every path.
local wide = table.create(4096, big)
refused(too_large, pcall(serde.json.encode, wide))
refused(too_large, pcall(serde.yaml.encode, wide))
refused(too_large, pcall(serde.xml.encode, "root", { item = wide }))
local wide_json = serde.json.encoder()
wide_json:begin_array()
refused(overfull, pcall(wide_json.items, wide_json, wide))
local wide_xml = serde.xml.encoder()
wide_xml:begin_element("root")
refused(overfull, pcall(wide_xml.elements, wide_xml, "item", wide))

-- XML repeats a list's element name for every item, and writes the root name; both count.
local long_name = string.rep("x", 1024 * 1024)
refused(too_large, pcall(serde.xml.encode, "root", { [long_name] = table.create(4096, serde.empty_object()) }))
refused(too_large, pcall(serde.xml.encode, string.rep("r", serde.MAX_ENCODED_BYTES), "text"))

-- An element's attributes share one budget, names included, and must be scalars before any of
-- them is read.
local attributes = {}
for index = 1, 4096 do
	attributes[`a{index}`] = big
end
local attributed = serde.xml.encoder()
refused(overfull, pcall(attributed.begin_element, attributed, "item", attributes))
local nested = serde.xml.encoder()
refused(
	"attribute chain must be a string, number or boolean",
	pcall(nested.begin_element, nested, "item", { chain = chain })
)

-- Reading a value holds a bounded number of values before any writing starts, however few
-- bytes each would write.
local too_many = "holds more than 1048576 values"
local zeros = table.create(2000000, 0)
refused(too_many, pcall(serde.json.encode, zeros))
local zeros_json = serde.json.encoder()
zeros_json:begin_object()
refused(too_many, pcall(zeros_json.field, zeros_json, "zeros", zeros))

-- XML writes a number as its decimal, which a tiny one makes hundreds of bytes long.
local tiny: any = 1e-300
for _ = 1, 20 do
	tiny = { a = tiny, b = tiny }
end
refused(too_large, pcall(serde.xml.encode, "root", tiny))
local tiny_xml = serde.xml.encode("n", 1e-300)
assert(#tiny_xml > 300, tiny_xml)

-- Only `$text` and `$value` leave their key out of the output; any other key counts in full.
refused(too_large, pcall(serde.json.encode, { ["$" .. string.rep("x", serde.MAX_ENCODED_BYTES)] = 0 }))

-- Lists of empty lists write no XML, so what bounds them is the steps the encoder takes.
local hollow: any = {}
for _ = 1, 23 do
	hollow = { hollow, hollow }
end
local hollow_xml = serde.xml.encoder()
hollow_xml:begin_element("root")
refused("takes more than", pcall(hollow_xml.elements, hollow_xml, "item", { hollow }))

-- A table shared within the limit encodes in each place it appears.
local artist = { name = "Miles Davis" }
local tracks = { { title = "So What", artist = artist }, { title = "Freddie Freeloader", artist = artist } }
local decoded = serde.json.decode(serde.json.encode(tracks))
assert(decoded[1].artist.name == "Miles Davis" and decoded[2].artist.name == "Miles Davis")

local shared = serde.json.encoder()
shared:begin_array()
shared:items(tracks)
shared:end_array()
local written = shared:finish()
assert(written == serde.json.encode(tracks), written)

local shared_xml = serde.xml.encode("tracks", { track = tracks })
local _, artists = string.gsub(shared_xml, "<name>Miles Davis</name>", "")
assert(artists == 2, shared_xml)
local shared_yaml = serde.yaml.encode(tracks)
local _, yaml_artists = string.gsub(shared_yaml, "name: Miles Davis", "")
assert(yaml_artists == 2, shared_yaml)
