# 2025.1.25

1. BinanceLinearGateway replace REST API with Websocket API for sending orders

# 2024.12.16

1. write log (event) when exception raised by websocket client

# 2024.9.16

1. add more detail to TickData.extra including: active_volume/active_turnover/trade_count
2. BinanceLinearGateway upgrade to v3 api for querying account and position

# 2024.9.4

1. add extra data dict for BarData

# 2024.9.3

1. use vnpy_evo for rest and websocket client

# 2024.8.10

1. fix the problem of datetime timezone
2. fix the problem of account data receiving no update when the balance is all sold out
3. only keep user stream when key is provided

# 2024.5.7

1. use numpy.format_float_positional to improve float number precisio…
2. output log message of time offse
3. query private data after time offset is calculated
