# Billion Row Challenge in Rust

Inspired by https://github.com/gunnarmorling/1brc

The challenge is to write a Go program which reads `measurements.txt`,
calculates the min, mean, and max temperature value per weather station, and
emits the results on stdout in the format `<station>=<min>/<mean>/<max>` for
each station, separated by a newline. The stations must be ordered
alphabetically.

The following shows an example of `measurements.txt`:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

You can generate a sample `measurements.txt` with
`cargo run -p generate <num-measurement>`.

Once a `measurements.txt` file is created, you can run the sample submission .

# Rules

* No external library dependencies may be used
* The computation must happen at application runtime, i.e. you cannot process
  the measurements file at build time and just bake the result into the binary
* Input value ranges are as follows:
* Station name: non null UTF-8 string of min length 1 character and max
  length 100 characters
* Temperature value: non null double between -99.9 (inclusive) and 99.9
  (inclusive), always with one fractional digit
* There is a maximum of 10,000 unique station names
* Implementations must not rely on specifics of a given data set, e.g. any
  valid station name as per the constraints above and any data distribution
  (number of measurements per station) must be supported
