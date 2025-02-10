from(bucket: "fnodatabase")
  |> range(start: -365d)  // Adjust the time range as needed
  |> filter(fn: (r) => r._measurement == "nifty50_1d" and r._field == "close")
  |> exponentialMovingAverage(n: 10)  // Calculate 10-period EMA
  |> yield(name: "EMA")
