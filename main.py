#############################################################
# 1. Import libraries
#############################################################

from utils import load_sql_query, Engine, logger

#############################################################
# 2. Main code
#############################################################

def main():
    engine = Engine()
    query = load_sql_query('contracts_ts_v2.sql')
    
    logger.info("Downloading data...")
    df_contracts = engine.execute(query)

    logger.info("Creating and populating table...")
    df_contracts.to_sql(
        name='etl_biDimContractTimeSeries',
        schema='bi_assetplan',
        con=engine.engine,
        if_exists='replace',   # ✅ creates table if it doesn't exist, replaces if it does
        index=False,
        chunksize=10_000,
        method='multi'
    )
    logger.info("Table created successfully!")

if __name__ == "__main__":
    main()