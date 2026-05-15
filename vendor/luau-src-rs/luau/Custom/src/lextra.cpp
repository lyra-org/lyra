#include "lua.h"
#include "lapi.h"
#include "lgc.h"
#include "lobject.h"
#include "lstate.h"
#include "lgc.h"
#include <stdexcept>

using namespace std;

extern "C" const void* lua_getmetatablepointer(lua_State* L, int objindex)
{
    const TValue* obj = luaA_toobject(L, objindex);
    if (!obj)
        return NULL;

    switch (ttype(obj))
    {
    case LUA_TTABLE:
        return hvalue(obj)->metatable;
    case LUA_TUSERDATA:
        return uvalue(obj)->metatable;
    default:
        return NULL;
    }
}

extern "C" const char* lua_gcstatename(int state)
{
    return luaC_statename(state);
}

extern "C" int64_t lua_gcallocationrate(lua_State* L) {
    return luaC_allocationrate(L);
}

extern "C" void lua_gcdump(lua_State* L, void* file, const char* (*categoryName)(lua_State* L, uint8_t memcat))
{
    luaC_dump(L, file, categoryName);
}

extern "C" struct RustCallbackRet {
    int status;
    void* ret;
};

// Helper to call Rust code that may throw exceptions from C++ code (e.g. if protect fails)
//
// Can also be a alternative to lua_pcall for performance critical sections
//
// Returns 0 on success, 1 on exception pushed to stack, 2 for exception that could not be pushed to stack
// (e.g. either stack failed or catch-all triggered)
using RustCallback = void* (*)(lua_State* L, void* data) noexcept(false);
extern "C" RustCallbackRet luau_try(lua_State* L, RustCallback func, void* data) {
    try { 
        void* a = func(L, data); 
        return RustCallbackRet{0, a}; 
    } catch (const std::exception& e) {
        try {
            if (lua_gettop(L) > 0) {
                lua_checkstack(L, 1);
                lua_pushstring(L, e.what()); 
                return RustCallbackRet{1, nullptr};
            } else {
                return RustCallbackRet{2, nullptr};
            }
        } catch (...) { 
            return RustCallbackRet{2, nullptr}; 
        }
    } catch (...) { 
        throw;
    }
}