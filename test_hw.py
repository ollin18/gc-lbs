import pandas as pd

df1 = pd.read_parquet("/home/ollin/Downloads/stops_HWcoord_MX_part-000000000000.parquet")
df1
df1.dropna()

df1.to_csv("/home/ollin/Downloads/echo_cdmx_coords_hw.csv", index=False)

df = pd.read_parquet("/home/ollin/Downloads/all_stops_hw.parquet")
df
df.dropna()
df.to_csv("/home/ollin/Downloads/echo_cdmx_coords_hw_all.csv", index=False)
