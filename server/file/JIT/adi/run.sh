#!/bin/bash

# 开启 nullglob 防止无匹配时通配符返回字面量
shopt -s nullglob

# 定义优先级：wasm > cwasm > wasmu
check_and_run() {
    # 检查第一个匹配的文件
    case $1 in
        "wasm")
            files=( *.wasm )
            if [ ${#files[@]} -gt 0 ]; then
                echo "执行 .wasm 文件: ${files[0]}"
                /usr/bin/time -v wasmtime "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
        "cwasm")
            files=( *.cwasm )
            if [ ${#files[@]} -gt 0 ]; then
                echo "执行 .cwasm 文件: ${files[0]}"
                /usr/bin/time -v wasmtime --allow-precompiled "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
        "wasmu")
            files=( *.wasmu )
            if [ ${#files[@]} -gt 0 ]; then
                echo "执行 .wasmu 文件: ${files[0]}"
                /usr/bin/time -v wasmer "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
    esac
    return 1
}

# 按优先级顺序执行
check_and_run "wasm" || \
check_and_run "cwasm" || \
check_and_run "wasmu" || \
echo "未找到 .wasm、.cwasm 或 .wasmu 文件"

# 关闭 nullglob
shopt -u nullglob
