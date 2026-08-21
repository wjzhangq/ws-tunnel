#!/usr/bin/env bash
# 交叉编译发布产物。
#
# server 只出 linux(它是被连接的一端,跑在服务器上);client 要出
# linux / windows / darwin,因为它跑在各种边缘机上。
#
# VERSION 未设置时回落到 git describe,所以本地直接跑也能得到和 CI 一致的产物。
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
DIST="${DIST:-dist}"
LDFLAGS="-s -w -X main.version=${VERSION}"

SERVER_TARGETS=(linux/amd64 linux/arm64)
CLIENT_TARGETS=(
  linux/amd64 linux/arm64
  darwin/amd64 darwin/arm64
  windows/amd64 windows/arm64
)

rm -rf "$DIST"
mkdir -p "$DIST"

# sha256sum 在 linux 上有,darwin 上只有 shasum,本地和 CI 都要能跑。
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  else
    shasum -a 256 "$@"
  fi
}

# build <server|client> <goos> <goarch>
# 产物打包成压缩包而不是裸二进制:tar 保留可执行位,zip 是 windows 上的惯例。
build() {
  local kind=$1 goos=$2 goarch=$3
  local name="tunnel-${kind}"
  local ext=""
  [[ $goos == windows ]] && ext=".exe"

  local base="${name}_${VERSION}_${goos}_${goarch}"
  local stage="${DIST}/${base}"
  mkdir -p "$stage"

  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -trimpath -ldflags "$LDFLAGS" -o "${stage}/${name}${ext}" "./cmd/${name}"

  cp README.md "$stage/"
  # 只有 server 读配置文件,client 全靠命令行参数。
  [[ $kind == server ]] && cp config.example.yaml "$stage/"

  if [[ $goos == windows ]]; then
    (cd "$DIST" && zip -qr "${base}.zip" "$base")
  else
    tar -czf "${DIST}/${base}.tar.gz" -C "$DIST" "$base"
  fi
  rm -rf "$stage"
  echo "  ${base}"
}

echo "version: ${VERSION}"
echo "server (linux only):"
for t in "${SERVER_TARGETS[@]}"; do
  build server "${t%/*}" "${t#*/}"
done
echo "client:"
for t in "${CLIENT_TARGETS[@]}"; do
  build client "${t%/*}" "${t#*/}"
done

# 校验和文件里只放文件名,方便下载者在自己目录里直接 -c 校验。
(cd "$DIST" && sha256 ./*.tar.gz ./*.zip | sed 's#\./##' > SHA256SUMS)
echo "checksums:"
sed 's/^/  /' "${DIST}/SHA256SUMS"
