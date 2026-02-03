"""
EPWRF Data Processing Script
Parse, clean, and merge downloaded XLS files

The downloaded XLS files are HTML tables saved as .xls format.
This script:
1. Parses HTML tables from XLS files
2. Cleans the data (handle empty cells, convert types)
3. Reshapes from wide to long format
4. Merges multiple files into a single dataset
"""

import pandas as pd
import numpy as np
import os
import re
from bs4 import BeautifulSoup
import glob

# Configuration
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
OUTPUT_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_xls_html(filepath):
    """
    Read an EPWRF XLS file (which is actually HTML)
    Returns the raw HTML content
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    return content


def extract_metadata(html_content):
    """
    Extract metadata from the HTML header rows
    Returns: dict with title, series, nic_code, variable
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    metadata = {
        'title': None,
        'series': None,
        'nic_code': None,
        'nic_description': None,
        'variable': None
    }
    
    # Find table rows
    rows = soup.find_all('tr')
    
    for row in rows[:10]:  # Check first 10 rows for metadata
        text = row.get_text(strip=True)
        
        if 'ASI' in text and 'India Time Series' in text:
            metadata['title'] = text
            
        if 'Concorded Series' in text or 'CSO Series' in text:
            metadata['series'] = text
            
        # NIC code pattern: "14 - manufacture of wearing apparel"
        nic_match = re.search(r'(\d+)\s*-\s*(.+)', text)
        if nic_match:
            metadata['nic_code'] = nic_match.group(1)
            metadata['nic_description'] = nic_match.group(2).strip()
            
    return metadata


def parse_data_table(html_content):
    """
    Parse the data table from HTML content
    Returns: DataFrame with Year and State columns
    """
    from io import StringIO
    
    # Use pandas read_html directly - it handles the HTML tables well
    try:
        tables = pd.read_html(StringIO(html_content))
        
        # Find the data table (one with year columns)
        for df in tables:
            if len(df) < 5:
                continue
                
            # Check first column for year pattern
            first_col = df.iloc[:, 0].astype(str)
            year_matches = first_col.str.match(r'\d{4}\s*-\s*\d{4}', na=False)
            
            if year_matches.sum() > 5:  # Has multiple year rows
                # This is likely our data table
                
                # Find where data actually starts (skip header rows)
                data_start_idx = year_matches.idxmax()
                
                # Get state headers from the row before data
                # The columns at index 0 should be state names after the first few rows
                # Check column headers from the DataFrame
                col_names = list(df.columns)
                
                # If columns are just integers, need to find state names in data
                if isinstance(col_names[0], int):
                    # First row with year data starts the actual data
                    # Headers should be in earlier rows
                    header_rows = df.iloc[:data_start_idx]
                    
                    # Find row with state names
                    for idx, row in header_rows.iterrows():
                        row_vals = row.dropna().astype(str).tolist()
                        if any('Pradesh' in v or 'Tamil' in v or 'Kerala' in v for v in row_vals):
                            col_names = row.tolist()
                            break
                
                # Extract data rows only
                data_df = df.iloc[data_start_idx:].copy()
                data_df.columns = ['Year'] + [str(c) for c in col_names[1:]]
                
                # Reset index
                data_df = data_df.reset_index(drop=True)
                
                return data_df
                
    except Exception as e:
        print(f"  pandas read_html error: {e}")
    
    # Fallback: manual parsing with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        
        # Look for the data table (has years in first column)
        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            if cells:
                first_cell_text = cells[0].get_text(strip=True)
                # Check if this looks like a year (1980-1981 format)
                if re.match(r'\d{4}\s*-\s*\d{4}', first_cell_text):
                    # Found the data table, now extract headers and data
                    
                    # Get state headers from previous rows
                    state_row = rows[i-2] if i > 1 else (rows[i-1] if i > 0 else None)
                    states = ['Year']
                    if state_row:
                        state_cells = state_row.find_all(['td', 'th'])
                        for cell in state_cells[1:]:  # Skip first cell
                            text = cell.get_text(strip=True)
                            if text and text != '\xa0':
                                states.append(text)
                            
                    # Extract data rows
                    data = []
                    for data_row in rows[i:]:
                        cells = data_row.find_all(['td', 'th'])
                        if cells:
                            row_data = []
                            for cell in cells:
                                text = cell.get_text(strip=True)
                                # Convert &nbsp; and empty to None
                                if text in ['', '\xa0', '&nbsp;', '-']:
                                    row_data.append(None)
                                else:
                                    row_data.append(text)
                            if row_data and row_data[0] and re.match(r'\d{4}', str(row_data[0])):
                                data.append(row_data)
                    
                    # Create DataFrame
                    if data:
                        max_cols = max(len(row) for row in data)
                        # Pad states list
                        while len(states) < max_cols:
                            states.append(f'Col_{len(states)}')
                        # Pad data rows
                        data = [row + [None] * (max_cols - len(row)) for row in data]
                        
                        df = pd.DataFrame(data, columns=states[:max_cols])
                        return df
        
    return None


def clean_dataframe(df, variable_name=None):
    """
    Clean the extracted DataFrame
    - Rename columns
    - Convert data types
    - Handle missing values
    """
    if df is None or df.empty:
        return None
    
    # Make a copy
    df = df.copy()
    
    # Rename first column to Year
    df.columns = ['Year'] + list(df.columns[1:])
    
    # Clean Year column
    df['Year'] = df['Year'].astype(str).str.strip()
    
    # Remove any non-year rows
    df = df[df['Year'].str.match(r'\d{4}-\d{4}', na=False)]
    
    # Convert numeric columns
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Add variable name if provided
    if variable_name:
        df.insert(1, 'Variable', variable_name)
    
    return df


def wide_to_long(df):
    """
    Convert from wide format (states as columns) to long format
    Wide: Year, State1, State2, ...
    Long: Year, State, Value
    """
    if 'Variable' in df.columns:
        id_vars = ['Year', 'Variable']
    else:
        id_vars = ['Year']
    
    df_long = df.melt(
        id_vars=id_vars,
        var_name='State',
        value_name='Value'
    )
    
    return df_long


def process_single_file(filepath, output_format='wide'):
    """
    Process a single XLS file
    
    Args:
        filepath: Path to XLS file
        output_format: 'wide' or 'long'
        
    Returns:
        DataFrame (wide or long format)
    """
    print(f"Processing: {os.path.basename(filepath)}")
    
    # Read HTML content
    html_content = read_xls_html(filepath)
    
    # Extract metadata
    metadata = extract_metadata(html_content)
    print(f"  Series: {metadata.get('series', 'Unknown')}")
    print(f"  NIC: {metadata.get('nic_code', 'Unknown')} - {metadata.get('nic_description', 'Unknown')}")
    
    # Parse data table
    df = parse_data_table(html_content)
    
    if df is None:
        print("  ERROR: Could not parse data table")
        return None
    
    # Clean data
    df = clean_dataframe(df)
    
    # Add metadata columns
    if metadata['nic_code']:
        df.insert(1, 'NIC_Code', metadata['nic_code'])
    if metadata['nic_description']:
        df.insert(2, 'NIC_Description', metadata['nic_description'][:50])
    
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    
    if output_format == 'long':
        df = wide_to_long(df)
        
    return df


def process_all_files(input_dir=DOWNLOAD_DIR, pattern="*.xls"):
    """
    Process all XLS files in directory
    
    Returns:
        List of DataFrames
    """
    files = glob.glob(os.path.join(input_dir, pattern))
    print(f"Found {len(files)} files to process")
    
    results = []
    for filepath in files:
        df = process_single_file(filepath)
        if df is not None:
            results.append({
                'file': os.path.basename(filepath),
                'data': df
            })
            
    return results


def merge_files(file_list, output_path=None):
    """
    Merge multiple processed files into a single dataset
    
    Args:
        file_list: List of {'file': name, 'data': DataFrame}
        output_path: Path to save merged CSV (optional)
        
    Returns:
        Merged DataFrame
    """
    all_dfs = []
    
    for item in file_list:
        df = item['data'].copy()
        df['Source_File'] = item['file']
        
        # Convert to long format for merging
        df_long = wide_to_long(df) if 'State' not in df.columns else df
        all_dfs.append(df_long)
    
    merged = pd.concat(all_dfs, ignore_index=True)
    
    if output_path:
        merged.to_csv(output_path, index=False)
        print(f"Saved merged data to: {output_path}")
        
    return merged


def extract_variable_from_filename(filename):
    """
    Extract variable name from standardized filename
    Format: ASI_NIC{code}_{variable}_{start}_{end}.xls
    """
    match = re.search(r'ASI_NIC\d+_(.+)_\d{4}_\d{4}\.xls', filename)
    if match:
        return match.group(1).replace('_', ' ')
    return None


def summary_statistics(df):
    """
    Generate summary statistics for processed data
    """
    print("\n" + "="*60)
    print("DATA SUMMARY")
    print("="*60)
    
    if 'Year' in df.columns:
        years = df['Year'].unique()
        print(f"Year Range: {min(years)} to {max(years)} ({len(years)} years)")
        
    if 'State' in df.columns:
        states = df['State'].nunique()
        print(f"States: {states}")
        
    if 'Variable' in df.columns:
        variables = df['Variable'].nunique()
        print(f"Variables: {variables}")
        
    if 'Value' in df.columns:
        print(f"\nValue Statistics:")
        print(f"  Count: {df['Value'].count():,}")
        print(f"  Missing: {df['Value'].isna().sum():,} ({df['Value'].isna().mean()*100:.1f}%)")
        print(f"  Mean: {df['Value'].mean():,.2f}")
        print(f"  Min: {df['Value'].min():,.2f}")
        print(f"  Max: {df['Value'].max():,.2f}")


# ============================================================================
# MAIN - Example usage
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("EPWRF Data Processing")
    print("="*60)
    
    # Find all downloaded files
    download_dir = os.path.join(os.getcwd(), "downloads")
    
    if os.path.exists(download_dir):
        files = glob.glob(os.path.join(download_dir, "*.xls"))
        print(f"\nFound {len(files)} XLS files in downloads/")
        
        if files:
            # Process first file as demo
            df = process_single_file(files[0], output_format='wide')
            
            if df is not None:
                print("\n" + "-"*40)
                print("SAMPLE DATA (first 5 rows, first 5 cols):")
                print("-"*40)
                print(df.iloc[:5, :5].to_string())
                
                # Save cleaned version
                output_file = os.path.join(OUTPUT_DIR, "cleaned_" + os.path.basename(files[0]).replace('.xls', '.csv'))
                df.to_csv(output_file, index=False)
                print(f"\nSaved cleaned data to: {output_file}")
    else:
        print(f"Downloads directory not found: {download_dir}")
        print("Run the download script first!")
