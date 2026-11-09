local item_ids = require("../../protocol/items/identifiers")

local first, second = string.rep("a", 21), "_-0123456789abcdefXYZ"
local first_guid, second_guid = item_ids.encode(first) :: string, item_ids.encode(second) :: string

-- A Lyra id and its GUID convert both ways.
assert(#first_guid == 32 and string.match(first_guid, "^[0-9a-f]+$"), first_guid)
assert(item_ids.decode(first_guid) == first and item_ids.decode(second_guid) == second)
assert(item_ids.encode("short") == nil and item_ids.encode(nil) == nil)
assert(item_ids.encode(string.rep("!", 21)) == nil, "a character outside the alphabet")

-- A GUID is read dashed or not, in either case, and written one way.
local dashed = string.format(
	"%s-%s-%s-%s-%s",
	string.sub(first_guid, 1, 8),
	string.sub(first_guid, 9, 12),
	string.sub(first_guid, 13, 16),
	string.sub(first_guid, 17, 20),
	string.sub(first_guid, 21, 32)
)
assert(item_ids.normalize_guid(dashed) == first_guid)
assert(item_ids.normalize_guid(` {string.upper(dashed)} `) == first_guid)
assert(item_ids.decode(string.upper(dashed)) == first)
assert(item_ids.normalize_guid(string.sub(first_guid, 1, 31)) == nil, "too short")
assert(item_ids.normalize_guid(string.rep("g", 32)) == nil, "not hex")
assert(item_ids.normalize_guid("") == nil and item_ids.normalize_guid(nil) == nil)
assert(item_ids.decode("not-an-id") == nil)

local guid = item_ids.new_guid()
assert(item_ids.normalize_guid(guid) == guid and guid ~= item_ids.new_guid())
assert(item_ids.EMPTY_GUID == string.rep("0", 32))

local function joined(ids: { string }?): string
	return if ids then table.concat(ids, ",") else "nil"
end

-- A lenient list skips what isn't an id and lists each id once.
assert(joined(item_ids.decode_list(`{first_guid},{second_guid}`)) == `{first},{second}`)
assert(joined(item_ids.decode_list(`{first_guid}, nope ,,{dashed},{second_guid},`)) == `{first},{second}`)
assert(joined(item_ids.decode_list(`"{first_guid}", " {second_guid} "`)) == `{first},{second}`, "quoted entries")
assert(joined(item_ids.decode_list({ second_guid, "nope", first_guid, second_guid })) == `{second},{first}`)
assert(#item_ids.decode_list({ `"{first_guid}"` }) == 0, "an array entry is taken as it is, quotes and all")
assert(#item_ids.decode_list(nil) == 0 and #item_ids.decode_list("") == 0 and #item_ids.decode_list({}) == 0)

-- An exact list keeps repeats and order, and one bad entry fails it.
assert(joined(item_ids.decode_list_exact(`{second_guid},{first_guid},{second_guid}`)) == `{second},{first},{second}`)
assert(joined(item_ids.decode_list_exact({ first_guid, `"{first_guid}"` })) == `{first},{first}`)
assert(item_ids.decode_list_exact(`{first_guid},nope`) == nil)
assert(item_ids.decode_list_exact(`{first_guid},,{second_guid}`) == nil, "an empty entry")
assert(item_ids.decode_list_exact({ first_guid, "" }) == nil)
assert(joined(item_ids.decode_list_exact("")) == "" and joined(item_ids.decode_list_exact({})) == "")
