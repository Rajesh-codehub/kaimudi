from cme_scraper.extract import get_commodity_data
from cme_scraper.transform import clean_data,transform_data,clean_dataframe
from cme_scraper.loader import df_to_mysql,load_config,get_mysql_connection,load_data




# Example usage
if __name__ == "__main__":
    

    url = "https://www.cmegroup.com/markets/products.html#subGroups=1&sortDirection=desc&sortField=oi"

    config_path = "config.json"

    
    # Scrape data
    df = get_commodity_data(url)
    
    if df is not None:
        # Clean data
        cleaned_df = clean_data(df)

        table_name = "cme_data"

        

        
        
        # Transform data
        final_df = transform_data(cleaned_df)

        clean_df = clean_dataframe(final_df)

        load_data(clean_df)




        #uncomment these three lines if you have mysql database
        
        # config = load_config(config_path)

        # conn = get_mysql_connection(config)



        # df_to_mysql(clean_df,table_name,conn)
        
        