--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- A client's device profile, read as upstream's `ContainerHelper` and `StreamBuilder` read it.

local module = {}

-- Whether a comma-separated profile list names any of the input's comma-separated values. An
-- empty profile list names everything; a negative one names what it doesn't list.
local function list_names(profile_list: string?, negative: boolean, input: string): boolean
	if profile_list == nil or profile_list == "" then
		return true
	end
	for input_value in string.gmatch(input, "[^,]+") do
		local lowered = string.lower(input_value)
		for profile_value in string.gmatch(profile_list, "[^,]+") do
			if lowered == string.lower(profile_value) then
				return not negative
			end
		end
	end
	return negative
end

-- Upstream's `ContainerHelper.ContainsContainer`: a leading `-` makes the profile list negative,
-- and an input naming nothing matches only a negative list.
function module.contains_container(profile_list: string?, input: string?): boolean
	local negative = false
	if profile_list ~= nil and string.sub(profile_list, 1, 1) == "-" then
		negative = true
		profile_list = string.sub(profile_list, 2)
	end
	if input == nil or input == "" then
		return negative
	end
	return list_names(profile_list, negative, input)
end

return module
