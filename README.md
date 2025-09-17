# Alloc
Lock-Free Object Allocator with QSBR Memory Reclamation

## Benchmarks

### Single-threaded
| Method             | Time (μs) | Speedup |
|-------------------|-----------|---------|
| Naive new/delete   | 632       | 1x      |
| Custom allocator   | 271       | 2.33x   |

### Multi-threaded
| Method             | Time (μs) | Speedup |
|-------------------|-----------|---------|
| Naive new/delete   | 5039      | 1x      |
| Custom allocator   | 2369      | 2.13x   |

### Stress Test
- Total time: 49 ms
- Objects created/destroyed: 200,566
