local metadata = require("@lyra/metadata")

local provider = metadata.Provider.new("option-declaration-test")

local function rejects(config: any, message: string)
	local ok, err = pcall(function()
		provider:declare_option(config)
	end)
	assert(not ok, `expected a declaration error naming "{message}"`)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

-- A choice lists the values it takes, and defaults to one of them.
provider:declare_option({
	name = "space",
	label = "Space",
	type = "choice",
	options = {
		{ value = "audio", label = "Audio" },
		{ value = "lyrics", label = "Lyrics" },
	},
	default = "audio",
})

rejects({ name = "a", label = "A", type = "choice" }, "a choice needs options")
rejects({ name = "a", label = "A", type = "choice", options = {} }, "a choice needs options")
rejects({
	name = "a",
	label = "A",
	type = "choice",
	options = { { value = "x", label = "X" }, { value = "x", label = "Y" } },
}, "option value 'x' is listed twice")
rejects({ name = "a", label = "A", type = "choice", options = { { value = "x" } } }, "label must be a non-empty string")
rejects({
	name = "a",
	label = "A",
	type = "choice",
	options = { { value = "x", label = "X" } },
	default = "y",
}, "must be one of 'x'")
rejects(
	{ name = "a", label = "A", type = "boolean", options = { { value = "x", label = "X" } } },
	"options is only for a choice"
)
rejects({ name = "a", label = "A", type = "boolean", default = 1 }, "default for 'a' must be a boolean")
rejects({ name = "a", label = "A", type = "list" }, "unsupported type 'list'")
