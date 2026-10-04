local ids = require("@lyra/ids")

assert(ids.get_id(999999) == nil)
assert(ids.get_db_id("missing-public-id") == nil)
assert(ids.get_ids({ 999999 })[999999] == nil)
assert(ids.get_db_ids({ "missing-public-id" })["missing-public-id"] == nil)
