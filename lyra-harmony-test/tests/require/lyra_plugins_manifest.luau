local plugins = require("@lyra/plugins")

local manifest = plugins.manifest()
local listed = plugins.list()
local by_id = plugins.get("test")

assert(manifest.id == "test")
assert(manifest.name == "Luau Test")
assert(manifest.version == "1.0.0")
assert(manifest.entrypoint == "require/lyra_plugins_manifest.luau")
assert(#listed == 1)
assert(listed[1].id == "test")
assert(by_id.id == "test")
assert(plugins.get("missing") == nil)
