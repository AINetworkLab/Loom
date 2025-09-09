This benchmark suite comprises 30 numerical computation programs with static control flow, designed to evaluate the performance disparities across different WebAssembly (Wasm) runtimes under varying compilation and execution modes (JIT, AOT, and interpreter) when processing identical tasks. The benchmarks assess key computational metrics, including numerical processing speed, file I/O throughput, and memory access efficiency. Among them, 16 benchmarks are sourced from PolyBench, while the remaining 14 are custom-developed. The suite covers a diverse range of application domains, such as linear algebra computations, image processing, physical simulations, dynamic programming, and statistical operations. Currently, WebAssembly (Wasm) implementation is provided.

Available benchmarks
Benchmark	Description
2mm		2 Matrix Multiplications (alpha * A * B * C + beta * D)
adi		Alternating Direction Implicit solver
correlation	Correlation Computation
covariance	Covariance Computation
durbin		Toeplitz system solver
fdtd-2d		2-D Finite Different Time Domain Kernel
gramschmidt	Gram-Schmidt decomposition
lu		LU decomposition
seidel-2d	2-D Seidel stencil computation
hash		hash table implementation
QuickSort	Large arrays for quick sorting
sieve-eratos	Sieve of Eratosthenes
monte_carlo_pi	The value of π is estimated by random sampling
fibonacci	Fibonacci series calculation
TSP		Held-Karp dynamic programming algorithm
SAT		DPLL backtracking algorithm
floyd		Floyd-Warshall algorithm
fft		Fast Fourier transform
PCA		Large-scale matrix decomposition
string-sort	Sort strings in a large array
envelop		Longest Increasing Subsequence
sobelpp		sobel Edge detection
perceptron	Perceptron Algorithm
