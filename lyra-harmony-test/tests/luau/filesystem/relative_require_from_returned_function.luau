local main = require("@self/luau/filesystem/lib/_returned_loader.luau")

assert(main.load() == 42)
