# OMQ.lua Changelog

## [Unreleased]

## [0.2.1] - 2026-08-23

- Preserve `SERVER` routing IDs across native receive and send calls.
- Allow `inproc://` sends across OMQ runtime threads.
- Bound exchange retries and preserve soak deadlines during shutdown.
- Handle LZ4 compressor epoch rollover without corrupting the stream.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.2.0] - 2026-08-18

- First LuaRocks release of OMQ.lua.
