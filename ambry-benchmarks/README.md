# Ambry Benchmarks

JMH (Java Microbenchmark Harness) performance benchmarks for Ambry components.

## Running Benchmarks

```bash
### Run all benchmarks
./gradlew jmh

### Run specific benchmarks
./gradlew jmh -Pjmh.includes='.*java8.*'
```
## Output

Results saved to: `ambry-benchmarks/build/reports/jmh/results.txt`
