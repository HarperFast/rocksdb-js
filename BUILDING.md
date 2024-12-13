# LINUX

- `./build.sh`

# OSX

- `brew install zstd`
- `JOBS=16 npx prebuildify -t 20.11.1 -t 21.6.2 --napi --strip --arch arm64`

Or

```
git rm --cached deps/folly deps/zstd
git submodule update --init --recursive
npm i
JOBS=16 npx prebuildify --napi --strip --arch arm64
```
