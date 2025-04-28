from datetime import datetime

def save_dataframe(df, layer, format="parquet", partition_by=None):
    """
    Save dataframe to the medallion data architecture.
    """
    path = f"data/{layer}/{datetime.now().strftime('%Y%m%d')}"
    writer = df.write.mode("overwrite")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
        
    writer.format(format).save(path)
    print(f"Saved {layer} data to {path}")