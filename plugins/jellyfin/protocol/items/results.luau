--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local module = {}

function module.build_query_result<T>(items: { T }, total_count: number, start_index: number): api.JsonValue
	return {
		Items = items,
		TotalRecordCount = total_count,
		StartIndex = start_index,
	}
end

-- Items a streamed listing realizes and writes at a time, so a listing of any size streams in
-- bounded memory.
module.BATCH_SIZE = 250

-- Hands `emit` the items a batch at a time, in order.
function module.in_batches<T>(items: { T }, emit: ({ T }) -> ())
	for start = 1, #items, module.BATCH_SIZE do
		emit(table.move(items, start, math.min(start + module.BATCH_SIZE - 1, #items), 1, {}))
	end
end

function module.paginate_items<T>(items: { T }, start_index: number?, limit: number?): ({ T }, number, number)
	local total = #items
	local safe_start = start_index or 0
	if safe_start > total then
		safe_start = total
	end

	local finish_exclusive = total
	if limit ~= nil then
		finish_exclusive = math.min(total, safe_start + limit)
	end

	local paged: { T } = {}
	local output_index = 1
	for index = safe_start + 1, finish_exclusive do
		paged[output_index] = items[index]
		output_index += 1
	end

	return paged, total, safe_start
end

return module
