local LuauSignal = require("signals")

local tookDamage: LuauSignal.Identity<string, number> = LuauSignal()

tookDamage:connect(function(player: string, damage: number)
    print(player .. " took " .. damage .. " damage!")
end)

local signal2: LuauSignal.Identity<string> = LuauSignal()

signal2:connect(function(player: string)
    print(player .. " connected!")
end)

tookDamage:fire("Player1", 10)
signal2:fire("Player1")
tookDamage:fire("Player2", 20)
tookDamage:fire("Player3", 30)
signal2:fire("Player2")
signal2:fire("Player3")
tookDamage:fire("Player4", 40)
signal2:fire("Player4")