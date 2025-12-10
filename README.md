# 1 Billion Row Challenge

The [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc) is a challenge of code optimization: "How fast can you parse 1 billion lines and compute some (simple) statistics on the contents of the file?"

This repo is intended to be a comparison of different approaches: a simple, stupid approach (`plain`)  and an optimized, performant approach (`performant`).

# Performance differences across platforms

Most of the development for this project was done in WSL, and some of it on a mac M1. Right now there's not a lot of target/OS specific optimizations done, but it's pretty crazy that it takes 10s on WSL and less than 200ms on my Mac. It does make it hard to be sure whether a change is an actual optimization between both setups, but it's a good challenge.
