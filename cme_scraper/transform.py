# cleaning.py
import pandas as pd
import requests
import numpy as np
from io import StringIO

def clean_data(df):
    """
    Basic cleaning of commodity data
    
    Parameters:
    df (pd.DataFrame): Raw DataFrame
    
    Returns:
    pd.DataFrame: Cleaned DataFrame
    """
    # Create a copy to avoid modifying original data
    cleaned_df = df.copy()
    
    # Remove * from column names
    cleaned_df.columns = cleaned_df.columns.str.replace('*', '')
    
    # Replace '-' with NaN
    cleaned_df = cleaned_df.replace('-', pd.NA)
    
    # Remove commas and convert numeric columns
    numeric_cols = ['VOLUME', 'OPEN_INTEREST']
    for col in numeric_cols:
        cleaned_df[col] = cleaned_df[col].str.replace(',', '').astype(float)
    
    # Strip whitespace from string columns
    string_cols = cleaned_df.select_dtypes(include=['object']).columns
    cleaned_df[string_cols] = cleaned_df[string_cols].apply(lambda x: x.str.strip())
    
    return cleaned_df


def clean_dataframe(df):
    """
    Clean DataFrame by handling NA values appropriately based on data type
    """
    df_clean = df.copy()
    
    for column in df_clean.columns:
        dtype = df_clean[column].dtype
        
        # Handle different data types
        if np.issubdtype(dtype, np.number):  # Numeric types
            df_clean[column] = df_clean[column].fillna(0)
        elif dtype == 'datetime64[ns]':  # Datetime
            df_clean[column] = df_clean[column].fillna(pd.NaT)
        elif dtype == 'bool':  # Boolean
            df_clean[column] = df_clean[column].fillna(False)
        else:  # Strings and others
            df_clean[column] = df_clean[column].fillna('')
            
    return df_clean

def transform_data(df):
    """
    Transform the cleaned data into the desired format with additional features.
    
    Parameters:
    df (pd.DataFrame): Cleaned DataFrame
    
    Returns:
    pd.DataFrame: Transformed DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    transformed_df = df.copy()
    
    # Create a binary column for BTIC products
    transformed_df['IS_BTIC'] = transformed_df['PRODUCT_NAME'].str.contains('BTIC').astype(int)
    
    # Create a market type category
    transformed_df['MARKET_TYPE'] = transformed_df.apply(
        lambda x: 'Futures' if 'Futures' in x['CLEARED_AS']
        else 'Swaps' if 'Swaps' in x['CLEARED_AS']
        else 'Options' if 'Options' in x['CLEARED_AS']
        else 'Other',
        axis=1
    )
    
    # Calculate market share based on volume
    total_volume = transformed_df['VOLUME'].sum()
    transformed_df['VOLUME_MARKET_SHARE'] = (transformed_df['VOLUME'] / total_volume * 100).round(2)
    
    # Calculate market share based on open interest
    total_oi = transformed_df['OPEN_INTEREST'].sum()
    transformed_df['OI_MARKET_SHARE'] = (transformed_df['OPEN_INTEREST'] / total_oi * 100).round(2)
    
    # Create activity status based on volume and open interest
    transformed_df['ACTIVITY_STATUS'] = transformed_df.apply(
        lambda x: 'Active' if x['VOLUME'] > 0 or x['OPEN_INTEREST'] > 0
        else 'Inactive',
        axis=1
    )
    
    # Reorder columns for better readability
    column_order = [
        'PRODUCT_NAME', 'EXCH', 'MARKET_TYPE', 'CLEARING', 'GLOBEX', 'CLEARPORT',
        'VOLUME', 'VOLUME_MARKET_SHARE', 'OPEN_INTEREST', 'OI_MARKET_SHARE',
        'IS_BTIC', 'ACTIVITY_STATUS', 'ASSET_CLASS', 'PRODUCT_GROUP'
    ]
    
    transformed_df = transformed_df[column_order]
    
    return transformed_df