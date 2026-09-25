# Approximate Frequent Items on a Data Stream (Spark Streaming)

Big Data Computing, University of Padua.

A Spark Streaming program that processes a stream of 32-bit integers and compares two sublinear-space methods for finding the approximate frequent items: **Sticky Sampling** and a method based on a **Count-Min sketch**. Both are evaluated against the exact frequencies computed on the same prefix of the stream.

## Problem

Given a stream of `n` items and a frequency threshold `phi` in `(0,1)`, an item is a **true frequent item** if it occurs at least `phi * n` times in the stream. Computing the exact counts requires storing one counter per distinct item, which is not feasible when the domain is the whole 32-bit integer range. Both methods below trade accuracy for space and return a superset of the true frequent items, using memory that does not depend on the number of distinct items.

The program reports three sets on the same prefix of `n` items:

| Set | Method |
|------|--------|
| exact | one counter per distinct item (ground truth, used only to evaluate the other two) |
| `F_SS` | Sticky Sampling with parameters `phi`, `epsilon`, `delta` |
| `F_CM` | Count-Min sketch with `d` rows and `w` columns |

## Algorithms

### Sticky Sampling

With accuracy `epsilon` in `(0,phi)` and confidence `delta` in `(0,1)`, the sampling rate is

```
r = ln(1 / (delta * phi)) / epsilon
```

Each incoming item is looked up in a dictionary. If it is already present its counter is incremented; otherwise it is inserted with counter `1` with probability `p = r / n`. At the end, the items whose estimated count is at least `(phi - epsilon) * n` are returned as `F_SS`.

The guarantee is one-sided: every true frequent item is returned with probability at least `1 - delta`, and no item with true frequency below `(phi - epsilon) * n` is returned. The size of the dictionary, not the number of distinct items, is what the method actually pays for, and it is reported in the output.

### Count-Min sketch

The sketch is a `d x w` integer matrix. Each row `j` has its own hash function drawn from the 2-universal family

```
h_j(x) = ((a_j * x + b_j) mod p) mod w,     p = 8191,  a_j in [1, p-1],  b_j in [0, p-1]
```

For every incoming item `x` the `d` counters `C[j][h_j(x)]` are incremented, and the estimated frequency of `x` is the minimum of those counters, which never underestimates the true frequency. An item enters `F_CM` the first time its estimate reaches `phi * n`.

Since the estimate is always an overestimate, `F_CM` contains all true frequent items plus a number of false positives that depends on `w`: the narrower the sketch, the more collisions inflate the counters of rare items.

## Stream source

The items are read from a socket opened by the course server `algo.dei.unipd.it`, which emits 32-bit integers as strings on four ports:

| Port | Stream |
|------|--------|
| 8887 | a few very frequent items, the rest uniform over the 32-bit domain |
| 8889 | a few very frequent items, some moderately frequent, the rest uniform |
| 8886 | deterministic version of 8887 (same stream at every connection) |
| 8888 | deterministic version of 8889 (same stream at every connection) |

The deterministic ports are the ones to use for testing, since two runs with the same parameters are directly comparable.

## Project structure

```
src/main/java/G35HW2.java     # Sticky Sampling + Count-Min sketch + streaming driver
build.gradle                  # Gradle build, produces a fat JAR
```

## Build

The build produces a self-contained JAR with the Spark dependencies marked `compileOnly`, since they are provided at runtime by the Spark installation.

```bash
./gradlew jar          # Linux / macOS
gradlew.bat jar        # Windows
```

The JAR is written to `build/libs/G35HW2.jar`.

## Run

```
spark-submit --master "local[*]" --class G35HW2 build/libs/G35HW2.jar <n> <phi> <epsilon> <delta> <d> <w> <portExp>
```

| Argument  | Meaning                                              |
|-----------|------------------------------------------------------|
| `n`       | number of items of the stream to process             |
| `phi`     | frequency threshold, in `(0,1)`                      |
| `epsilon` | accuracy parameter of Sticky Sampling, in `(0,phi)`  |
| `delta`   | confidence parameter of Sticky Sampling, in `(0,1)`  |
| `d`       | number of rows of the Count-Min sketch               |
| `w`       | number of columns of the Count-Min sketch            |
| `portExp` | port of `algo.dei.unipd.it` to connect to            |

**Example:**

```bash
spark-submit --master "local[*]" --class G35HW2 build/libs/G35HW2.jar 1000000 0.07 0.02 0.05 5 30 8888
```

The master must be `local[*]`: the program runs on a single machine and the receiver occupies one core, so at least two are needed.

## Output

```
INPUT PARAMETERS
n = 1000000
phi = 0.07
epsilon = 0.02
delta = 0.05
d = 5
w = 30
port = 8888

TRUE FREQUENT ITEMS
Item = 1 True Freq = 70123
...

STICKY SAMPLING
Size of dictionary = 23
Item = 1 True Freq = 70123
...

COUNT-MIN SKETCH
Size of F_CM = 24
Item = 1 True Freq = 70123
...
```

For the items of `F_SS` and `F_CM` the **true** frequency is printed, not the estimated one: this is what shows how far a false positive actually is from the threshold `phi * n`.

## Results

Measurements on the deterministic stream of port 8888, `n = 1000000`, `phi = 0.07`, average of three runs. An item is *frequent* if its true frequency is at least `n * phi`, *almost frequent* if it falls in `[n * (phi - epsilon), n * phi)`, *rare* otherwise. The stream contains 10 true frequent items.

**Sticky Sampling** (`delta = 0.05`):

| `epsilon` | frequent | almost frequent | rare | dictionary size |
|-----------|----------|-----------------|------|-----------------|
| 0.01      | 10       | 0               | 0    | 36              |
| 0.02      | 10       | 0               | 0    | 23              |
| 0.04      | 10       | 3               | 0    | 19              |

All 10 true frequent items are always found and no rare item is ever returned. A larger `epsilon` shrinks the dictionary, since fewer items are sampled, but lowers the output threshold to `(phi - epsilon) * n` and lets the almost frequent items through.

**Count-Min sketch** (`d = 5`, false positives classified with respect to `epsilon = 0.04`):

| `w` | frequent | almost frequent | rare | total returned |
|-----|----------|-----------------|------|----------------|
| 15  | 10       | 0               | 214  | 224            |
| 30  | 10       | 0               | 14   | 24             |
| 60  | 10       | 0               | 2    | 12             |

Here too all true frequent items are found, as expected from a method that never underestimates. The number of false positives collapses as `w` grows: doubling the width roughly halves the collision probability of each row, and since the estimate is the minimum over `d = 5` rows, a rare item needs to collide with a heavy item in every row to be reported.

Comparing the two: at a comparable output size Sticky Sampling is far more precise, but it needs a dictionary of items and a random draw per new item, while the Count-Min sketch works in fixed `d * w` space decided in advance and independent of the data.

## Implementation notes

- The batch interval is 100 ms. Each batch arrives as an RDD of strings and is processed inside `foreachRDD`, which updates the data structures kept in the driver's local memory.
- Only the first `n` items are processed: a batch that would cross the threshold is truncated with `batch.take(toProcess)`, so the counts refer to exactly `n` items and not to "a bit more than `n`".
- Termination uses a `Semaphore`, acquired before `start()` and released by the batch that reaches `n`. The main thread blocks on a second `acquire()` and then calls `sc.stop(false, false)`, which stops the streaming context without shutting down the JVM.
- The Count-Min update calls each hash function once per row and keeps the running minimum in the same loop, so the item is inserted into `F_CM` without a second pass over the matrix.
- `Math.floorMod` is used instead of `%` in the hash function, and the product `a * x` is computed as a `long`: the items span the whole 32-bit range, so both negative values and intermediate overflow are possible.
- Spark's log4j output is silenced so that the results are not buried in the driver's logs.

## Notes

The class name `G35HW2` and the output format follow the submission requirements of the course (group 35). The `ERROR ReceiverTracker` messages that Spark occasionally prints come from temporary hiccups of the socket connection and do not affect the result.

## Author

Francesco Pizzato, University of Padua.
