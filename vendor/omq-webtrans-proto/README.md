# omq-webtrans-proto

Based on <https://github.com/foctal/webtrans>, release 0.6.0. License: MIT;
see `LICENSE-MIT`.

OMQ patches this crate to negotiate the current draft-16 WebTransport wire
format, the draft-02 format used by Chromium 154 and Firefox 156, and the
draft-07 format used by Safari 26. It also exposes CONNECT Origin for
listener authorization. The vendored code is limited to the QUIC profile
used by OMQ.
