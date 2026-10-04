local plugins = require("@lyra/plugins")

local config = plugins.declare_settings(function(builder)
	builder:group("connection", "Connection")
	builder:string("token", {
		label = "Token",
		default = "global-token",
	})
	builder:bool("enabled", {
		label = "Enabled",
		default = true,
	})
	builder:choice("mode", {
		label = "Mode",
		default = "fast",
		options = {
			{ value = "fast", label = "Fast" },
			{ value = "safe", label = "Safe" },
		},
	})
end)

assert(config.token == "global-token")
assert(config.enabled == true)
assert(config.mode == "fast")
