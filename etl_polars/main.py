import polars as pl
import pyarrow.dataset as ds


def cleaning_data():
  # Establecemos el nombre del bucket y el path para leer el archivo parquet directamente desde polars

  bucket = "project-oi-bucket"
  path_input = "input_data"
  path_output = "output_data"

  # Leemos el archivo parquet y lo convertimos en un lazyframe

  s3_endpoint = f"s3://{bucket}/{path_input}/yellow_tripdata_2023-02.parquet"
  df = pl.read_parquet(s3_endpoint).lazy()


  # Creamos otro Lf con el simple motivo de agregar datos que no estan presentes y lograr una mejor lectura y entendimiento

  data = pl.LazyFrame(
    [(1, 'Standar rate'), (2, 'JFK'),(3, 'Newark'),(4, 'Nassau o Westchester'),(5, 'Negotiated fare'),(6, 'Group ride')],
    schema=['RatecodeID', 'Area_name']
  )

  # Hacemos el join de los lf, creamos una columna date para posteriormente particionar, seleccionamos las columnas de interes, filtramos datos y luego seleccionamos una cantidad desea, ya que nuestra instancia EC2 posee ciertas limitaciones
  
  df_clean_data = df.join(data, how='left', on='RatecodeID') \
                    .with_columns(pl.col('tpep_pickup_datetime').dt.date().alias('p_date')) \
                    .select(
                      pl.col('RatecodeID'),
                      pl.col('Area_name'),
                      pl.col('p_date')
                    ) \
                    .filter(pl.col('p_date').dt.year() == 2023) \
                    .collect() \
                    .limit(1500000)
  
  print('Successfully')
                
  # Creamos particiones para posteriormente facilitar la lectura de los mismos en terminos de eficiencia y claridad

  ds.write_dataset(df_clean_data.to_arrow()
                  ,f's3://{bucket}/{path_output}'
                  ,format='parquet'
                  ,existing_data_behavior='overwrite_or_ignore'
                  ,partitioning=['p_date'])

if __name__ == '__main__':
  cleaning_data()
