rockspec_format = "3.0"
package = "omq"
version = "0.2.0-1"

source = {
   url = "git+https://github.com/paddor/omq.rs.git",
   tag = "omq-lua-v0.2.0",
}

description = {
   summary = "Lua 5.4 binding for OMQ.rs",
   detailed = [[
      OMQ.lua is a Lua 5.4 binding for OMQ.rs backed by the omq-libzmq
      C ABI. It exposes the OMQ socket API through a small Lua module and
      a Rust native module.
   ]],
   homepage = "https://github.com/paddor/omq.rs/tree/main/bindings/lua",
   issues_url = "https://github.com/paddor/omq.rs/issues",
   license = "ISC",
   labels = { "zeromq", "omq", "messaging", "networking" },
}

dependencies = {
   "lua >= 5.4, < 5.5",
}

external_dependencies = {
   CARGO = {
      program = "cargo",
   },
}

build = {
   type = "command",
   build_command = "cargo build --release --manifest-path bindings/lua/native/Cargo.toml",
   install_command = [[
      mkdir -p "$(LUADIR)" "$(LIBDIR)" &&
      cp bindings/lua/lua/omq.lua "$(LUADIR)/omq.lua" &&
      if [ -f bindings/lua/native/target/release/libomq_native.so ]; then
         cp bindings/lua/native/target/release/libomq_native.so "$(LIBDIR)/omq_native.so";
      elif [ -f bindings/lua/native/target/release/libomq_native.dylib ]; then
         cp bindings/lua/native/target/release/libomq_native.dylib "$(LIBDIR)/omq_native.so";
      elif [ -f bindings/lua/native/target/release/omq_native.dll ]; then
         cp bindings/lua/native/target/release/omq_native.dll "$(LIBDIR)/omq_native.dll";
      else
         echo "omq_native shared library not found" >&2;
         exit 1;
      fi
   ]],
   copy_directories = {},
}
