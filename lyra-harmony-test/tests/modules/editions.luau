local editions = require("@lyra/editions")

local function same(actual: { string }, expected: { string }): boolean
	if #actual ~= #expected then
		return false
	end
	for index, value in expected do
		if actual[index] ~= value then
			return false
		end
	end
	return true
end

assert(same(editions.media_formats("WEB"), { "digital" }))
assert(same(editions.media_formats("Digital Media"), { "digital" }))
assert(same(editions.media_formats("DIG"), { "digital" }))
assert(same(editions.media_formats("SHM-CD"), { "cd" }))
assert(same(editions.media_formats("CD/DD"), { "cd" }))
assert(same(editions.media_formats("Hybrid SACD (CD layer)"), { "sacd" }))
assert(same(editions.media_formats('12" Vinyl'), { "vinyl" }))
assert(same(editions.media_formats("Other"), { "other" }))
assert(same(editions.media_formats("CD / DVD-Video"), { "cd", "dvd" }))
assert(same(editions.media_formats("2xCD"), { "cd" }))
assert(same(editions.media_formats({ "Digital Media", "WEB", "CD" }), { "digital", "cd" }))
assert(same(editions.media_formats("digital"), { "digital" }))

assert(same(editions.media_formats(nil), {}))
assert(same(editions.media_formats("   "), {}))
assert(same(editions.media_formats("Piano roll"), {}))
assert(same(editions.media_formats("\255"), {}))

assert(editions.media_formats_match("WEB", "Digital Media"))
assert(editions.media_formats_match({ "digital" }, { "Digital Media", "Digital Media" }))
assert(editions.media_formats_match({ "cd", "dvd" }, "DVD-Video"))
assert(not editions.media_formats_match("WEB", { "Other", "Other" }))
assert(not editions.media_formats_match("WEB", "CD"))
assert(not editions.media_formats_match(nil, "CD"))
assert(not editions.media_formats_match("Piano roll", "Piano roll"))

assert(editions.barcode("0199957588430") == "00199957588430")
assert(editions.barcode("4988-0315 54531") == "04988031554531")
assert(editions.barcode("00012345") == "00000000012345")
assert(editions.barcodes_match("00012345", editions.barcode("00012345")))
assert(editions.barcode("1234567") == nil)
assert(editions.barcode("not a barcode") == nil)
assert(editions.barcode(nil) == nil)

assert(editions.barcodes_match("602455505934", "0602455505934"))
assert(not editions.barcodes_match("199957588430", "199957588416"))
assert(not editions.barcodes_match(nil, "199957588416"))
assert(not editions.barcodes_match("", ""))
