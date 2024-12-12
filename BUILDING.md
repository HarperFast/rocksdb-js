# LINUX

- `./build.sh`

# OSX

- `brew install zstd`
- `JOBS=16 npx prebuildify -t 20.11.1 -t 21.6.2 --napi --strip --arch arm64`

Or

- `git submodule update --init --recursive`
- `JOBS=16 npx prebuildify -t 20.11.1 -t 21.6.2 --napi --strip --arch arm64`
