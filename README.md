# Data Collector

This data collector aims to collect a higher granularity (30 seconds) database that provides greater data depth than 1min OHLCV data. Depth is provided by retrieving OHLCV, orderbook and historical trade data as well as relevant security statistics that are generally only available to traders on the live chart. Data is collected and written to a csv locally. This would be best launched on a cloud instance to ensure uniterrupted collection. 
