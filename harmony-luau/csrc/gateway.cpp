// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

// Luau API operations that can raise, each run under harmony_luau_protectedcall so that an
// error longjmps only within C++ frames. Every shim returns a lua_Status code; on error the
// stack is back at its height at the call with the error value pushed.

#include "harmony_luau.h"

#include <string.h>

namespace
{

template<typename Operation>
int protect(lua_State* L, Operation&& operation)
{
    return harmony_luau_protectedcall(
        L,
        [](lua_State* L, void* ud)
        {
            (*static_cast<Operation*>(ud))(L);
        },
        &operation
    );
}

// The standard libraries Harmony exposes.
const luaL_Reg libraries[] = {
    {"", luaopen_base},
    {LUA_COLIBNAME, luaopen_coroutine},
    {LUA_TABLIBNAME, luaopen_table},
    {LUA_STRLIBNAME, luaopen_string},
    {LUA_MATHLIBNAME, luaopen_math},
    {LUA_BITLIBNAME, luaopen_bit32},
};

} // namespace

extern "C"
{

int harmony_luau_gateway_createtable(lua_State* L, int narray, int nrec)
{
    return protect(L, [&](lua_State* L) { lua_createtable(L, narray, nrec); });
}

int harmony_luau_gateway_newbuffer(lua_State* L, size_t size, void** data)
{
    return protect(L, [&](lua_State* L) { *data = lua_newbuffer(L, size); });
}

int harmony_luau_gateway_newuserdatatagged(lua_State* L, size_t size, int tag, void** data)
{
    return protect(L, [&](lua_State* L) { *data = lua_newuserdatatagged(L, size, tag); });
}

int harmony_luau_gateway_newuserdatataggedwithmetatable(lua_State* L, size_t size, int tag, void** data)
{
    return protect(L, [&](lua_State* L) { *data = lua_newuserdatataggedwithmetatable(L, size, tag); });
}

int harmony_luau_gateway_newuserdatadtor(lua_State* L, size_t size, lua_Destructor dtor, void** data)
{
    return protect(L, [&](lua_State* L) { *data = lua_newuserdatadtor(L, size, dtor); });
}

int harmony_luau_gateway_newthread(lua_State* L, lua_State** thread)
{
    return protect(L, [&](lua_State* L) { *thread = lua_newthread(L); });
}

int harmony_luau_gateway_pushcclosurek(lua_State* L, lua_CFunction fn, const char* debugname, int nup, lua_Continuation cont)
{
    return protect(L, [&](lua_State* L) { lua_pushcclosurek(L, fn, debugname, nup, cont); });
}

int harmony_luau_gateway_pushlstring(lua_State* L, const char* s, size_t len)
{
    return protect(L, [&](lua_State* L) { lua_pushlstring(L, s, len); });
}

int harmony_luau_gateway_pushnil(lua_State* L)
{
    return protect(L, [&](lua_State* L) { lua_pushnil(L); });
}

int harmony_luau_gateway_pushboolean(lua_State* L, int b)
{
    return protect(L, [&](lua_State* L) { lua_pushboolean(L, b); });
}

int harmony_luau_gateway_pushnumber(lua_State* L, double n)
{
    return protect(L, [&](lua_State* L) { lua_pushnumber(L, n); });
}

int harmony_luau_gateway_pushinteger64(lua_State* L, int64_t n)
{
    return protect(L, [&](lua_State* L) { lua_pushinteger64(L, n); });
}

int harmony_luau_gateway_pushthread(lua_State* L)
{
    return protect(L, [&](lua_State* L) { lua_pushthread(L); });
}

int harmony_luau_gateway_pushvalue(lua_State* L, int idx)
{
    return protect(L, [&](lua_State* L) { lua_pushvalue(L, idx); });
}

int harmony_luau_gateway_rawgeti(lua_State* L, int idx, int n)
{
    return protect(L, [&](lua_State* L) { lua_rawgeti(L, idx, n); });
}

int harmony_luau_gateway_rawgetfield(lua_State* L, int idx, const char* k)
{
    return protect(L, [&](lua_State* L) { lua_rawgetfield(L, idx, k); });
}

int harmony_luau_gateway_getmetatable(lua_State* L, int idx, int* found)
{
    return protect(L, [&](lua_State* L) { *found = lua_getmetatable(L, idx); });
}

int harmony_luau_gateway_ref(lua_State* L, int idx, int* ref)
{
    return protect(L, [&](lua_State* L) { *ref = lua_ref(L, idx); });
}

int harmony_luau_gateway_rawsetfield(lua_State* L, int idx, const char* k)
{
    return protect(L, [&](lua_State* L) { lua_rawsetfield(L, idx, k); });
}

int harmony_luau_gateway_rawseti(lua_State* L, int idx, int n)
{
    return protect(L, [&](lua_State* L) { lua_rawseti(L, idx, n); });
}

int harmony_luau_gateway_rawset(lua_State* L, int idx)
{
    return protect(L, [&](lua_State* L) { lua_rawset(L, idx); });
}

int harmony_luau_gateway_setfield(lua_State* L, int idx, const char* k)
{
    return protect(L, [&](lua_State* L) { lua_setfield(L, idx, k); });
}

int harmony_luau_gateway_setmetatable(lua_State* L, int idx)
{
    return protect(L, [&](lua_State* L) { lua_setmetatable(L, idx); });
}

int harmony_luau_gateway_setreadonly(lua_State* L, int idx, int enabled)
{
    return protect(L, [&](lua_State* L) { lua_setreadonly(L, idx, enabled); });
}

int harmony_luau_gateway_next(lua_State* L, int idx, int* more)
{
    return protect(L, [&](lua_State* L) { *more = lua_next(L, idx); });
}

int harmony_luau_gateway_xmove(lua_State* L, lua_State* to, int n)
{
    return protect(L, [&](lua_State* L) { lua_xmove(L, to, n); });
}

int harmony_luau_gateway_tolstring(lua_State* L, int idx, const char** s, size_t* len)
{
    return protect(L, [&](lua_State* L) { *s = lua_tolstring(L, idx, len); });
}

int harmony_luau_gateway_load(lua_State* L, const char* chunkname, const char* data, size_t size, int* result)
{
    return protect(L, [&](lua_State* L) { *result = luau_load(L, chunkname, data, size, 0); });
}

int harmony_luau_gateway_resume(lua_State* L, lua_State* co, int nargs, int* status)
{
    return protect(L, [&](lua_State* L) { *status = lua_resume(co, L, nargs); });
}

int harmony_luau_gateway_resumeerror(lua_State* L, lua_State* co, int* status)
{
    return protect(L, [&](lua_State* L) { *status = lua_resumeerror(co, L); });
}

int harmony_luau_gateway_openlibrary(lua_State* L, const char* name)
{
    return protect(
        L,
        [&](lua_State* L)
        {
            for (const luaL_Reg& library : libraries)
            {
                if (strcmp(library.name, name) == 0)
                {
                    lua_pushcfunction(L, library.func, nullptr);
                    lua_pushstring(L, name);
                    lua_call(L, 1, 0);
                    return;
                }
            }
            lua_pushfstring(L, "unknown Luau library '%s'", name);
            lua_error(L);
        }
    );
}

int harmony_luau_gateway_sandbox(lua_State* L)
{
    return protect(L, [&](lua_State* L) { luaL_sandbox(L); });
}

int harmony_luau_gateway_sandboxthread(lua_State* L)
{
    return protect(L, [&](lua_State* L) { luaL_sandboxthread(L); });
}

// Never raises; it needs no protected call.
const void* harmony_luau_gateway_topointer(lua_State* L, int idx)
{
    return lua_topointer(L, idx);
}

} // extern "C"
