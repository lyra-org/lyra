--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local dispatch = require("@self/dispatch")

api.get("/rest/{method}", dispatch.dispatch, { auth = "public" })
api.post("/rest/{method}", dispatch.dispatch, { auth = "public" })
