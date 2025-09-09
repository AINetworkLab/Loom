#!/bin/bash


shopt -s nullglob


check_and_run() {

    case $1 in
        "wasm")
            files=( *.wasm )
            if [ ${#files[@]} -gt 0 ]; then
                echo "run .wasm file: ${files[0]}"
                /usr/bin/time -v wasmtime "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
        "cwasm")
            files=( *.cwasm )
            if [ ${#files[@]} -gt 0 ]; then
                echo "run .cwasm file: ${files[0]}"
                /usr/bin/time -v wasmtime --allow-precompiled "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
        "wasmu")
            files=( *.wasmu )
            if [ ${#files[@]} -gt 0 ]; then
                echo "run .wasmu file: ${files[0]}"
                /usr/bin/time -v wasmer "${files[0]}" > data 2>&1
                return 0
            fi
            ;;
    esac
    return 1
}


check_and_run "wasm" || \
check_and_run "cwasm" || \
check_and_run "wasmu" || \
echo "can not find .wasm、.cwasm or .wasmu file"


shopt -u nullglob
