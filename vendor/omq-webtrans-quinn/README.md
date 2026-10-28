# omq-webtrans-quinn

Based on <https://github.com/foctal/webtrans>, release 0.6.0. License: MIT;
see `LICENSE-MIT`.

OMQ trims this crate to WebTransport sessions over a Quinn connection that
OMQ has already set up. It adds draft-02 and draft-07
WebTransport-over-HTTP/3 negotiation and CONNECT Origin handling. Quinn 0.11
has no `RESET_STREAM_AT`, so draft-16 stays off until Quinn ships it.
