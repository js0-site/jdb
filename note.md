## 如何编译？

本库依赖于硬件加速的哈希库 `gxhash`。

`gxhash` 在不同硬件上启用了不同的加速指令。

- `macos` 等 `arm` 芯片上可以直接编译
- `x86_64` 上编译需要启用现代 CPU 基本都支持的特性 `aes` 和 `sse2`

你可以在你的编译脚本中配置如下。

```
if [[ "$(uname -m)" == "x86_64" ]]; then
  export RUSTFLAGS="$RUSTFLAGS -C target-feature=+aes,+sse2"
fi
```

如果你是部署到自己的机器（不是给第三方使用），可以更加激进一点：

```
export RUSTFLAGS="-C target-cpu=native"
```