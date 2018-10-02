# sled-web-table [![Build Status](https://travis-ci.org/mitchmindtree/sled-web-table.svg?branch=master)](https://travis-ci.org/mitchmindtree/sled-web-table)

A typed table API for the sled-web crate.

Uses the `bytekey` crate for serialization of keys and the `bincode` crate for
serialization of values.

Also provides a `Reversible` trait to support bi-directional mappings.

*Publication to crates.io is currently pending [danburkert/bytekey#4](https://github.com/danburkert/bytekey/pull/4)*.
