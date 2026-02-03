"""
EPWRF XLS Parser - Clean Implementation
Parse HTML-table XLS files downloaded from EPWRF
"""

import pandas as pd
import numpy as np
import os
import re
from bs4 import BeautifulSoup
import glob

# Configuration
DOWNLOAD_DIR = "downloads"
OUTPUT_DIR = "data"

# State list (in order they appear in the file)
STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir",
    "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra",
    "Manipur", "Meghalaya", "Nagaland", "Orissa", "Punjab", "Rajasthan",
    "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh",
    "Uttarakhand", "West Bengal", "Andaman & Nicobar Islands", "Chandigarh",
    "Dadra & Nagar Haveli", "Daman & Diu", "Goa, Daman & Diu", "Delhi",
    "Puducherry", "Mizoram", "Ladakh", "Dadra & Nagar Haveli & Daman & Diu",
    "Lakshadweep"
]


def parse_epwrf_xls(filepath):
    """
    Parse EPWRF XLS file (HTML format)
    
    Returns:
        dict with 'metadata' and 'data' keys
    """
    print(f"Parsing: {os.path.basename(filepath)}")
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    soup = BeautifulSoup(content, 'html.parser')
    tables = soup.find_all('table')
    
    if len(tables) < 2:
        print("  ERROR: Expected 2 tables (header + data)")
        return None
    
    # =====================================
    # Parse Metadata from first table
    # =====================================
    header_table = tables[0]
    header_rows = header_table.find_all('tr')
    
    metadata = {
        'title': 'Annual Survey of Industries',
        'subtitle': 'Major Industry Group for States (2-Digit)',
        'series': None,
        'nic_code': None,
        'nic_description': None,
        'variable': None
    }
    
    # Extract series, NIC, variable from header rows
    for row in header_rows:
        cells = row.find_all(['td', 'th'])
        for cell in cells:
            text = cell.get_text(strip=True)
            
            if 'Concorded Series' in text or 'CSO Series' in text or 'NIC-' in text:
                if not metadata['series']:
                    metadata['series'] = text
                    
            # NIC code pattern: "14 - manufacture of wearing apparel"
            nic_match = re.match(r'^(\d+)\s*-\s*(.+)$', text)
            if nic_match:
                metadata['nic_code'] = nic_match.group(1)
                metadata['nic_description'] = nic_match.group(2).strip()
    
    # Get variable name from the last row of header table
    last_header_row = header_rows[-1]
    cells = last_header_row.find_all(['td', 'th'])
    for cell in cells:
        text = cell.get_text(strip=True)
        if text and text not in ['', '-', 'Number of Factories']:
            continue
        if text and text != '-':
            metadata['variable'] = text
            break
    
    # If variable still not found, use first non-empty cell from last row
    if not metadata['variable']:
        for cell in cells:
            text = cell.get_text(strip=True)
            if text and text != '-':
                metadata['variable'] = text
                break
    
    print(f"  Series: {metadata['series']}")
    print(f"  NIC: {metadata['nic_code']} - {metadata['nic_description']}")
    print(f"  Variable: {metadata['variable']}")
    
    # =====================================
    # Parse Data from second table
    # =====================================
    data_table = tables[1]
    data_rows = data_table.find_all('tr')
    
    # First row is header (Year, -, -, ...)
    # Remaining rows are data
    
    data = []
    for row in data_rows[1:]:  # Skip header row
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
            
        # First cell is Year
        year_text = cells[0].get_text(strip=True)
        if not re.match(r'\d{4}\s*-\s*\d{4}', year_text):
            continue
        
        row_data = {'Year': year_text}
        
        # Remaining cells are state values (39 states)
        for i, state in enumerate(STATES):
            cell_idx = i + 1  # +1 because first cell is Year
            if cell_idx < len(cells):
                value_text = cells[cell_idx].get_text(strip=True)
                # Convert to numeric
                if value_text in ['', '-', '\xa0']:
                    row_data[state] = np.nan
                else:
                    try:
                        row_data[state] = float(value_text.replace(',', ''))
                    except ValueError:
                        row_data[state] = np.nan
            else:
                row_data[state] = np.nan
        
        data.append(row_data)
    
    df = pd.DataFrame(data)
    
    print(f"  Parsed {len(df)} years of data")
    
    return {
        'metadata': metadata,
        'data': df
    }


def save_cleaned_data(result, output_dir=OUTPUT_DIR, output_format='csv'):
    """
    Save cleaned data with metadata-based filename
    """
    os.makedirs(output_dir, exist_ok=True)
    
    metadata = result['metadata']
    df = result['data']
    
    # Create filename
    var_name = (metadata['variable'] or 'Unknown').replace(' ', '_')[:30]
    nic = metadata['nic_code'] or 'XX'
    filename = f"ASI_NIC{nic}_{var_name}.{output_format}"
    filepath = os.path.join(output_dir, filename)
    
    # Add metadata columns
    df_out = df.copy()
    df_out.insert(0, 'NIC_Code', metadata['nic_code'])
    df_out.insert(1, 'NIC_Description', metadata['nic_description'])
    df_out.insert(2, 'Variable', metadata['variable'])
    
    if output_format == 'csv':
        df_out.to_csv(filepath, index=False)
    else:
        df_out.to_excel(filepath, index=False)
    
    print(f"  Saved: {filename}")
    return filepath


def to_long_format(df, metadata=None):
    """
    Convert wide data (states as columns) to long format
    """
    id_cols = ['Year']
    if 'NIC_Code' in df.columns:
        id_cols = ['NIC_Code', 'NIC_Description', 'Variable', 'Year']
    
    df_long = df.melt(
        id_vars=id_cols,
        var_name='State',
        value_name='Value'
    )
    
    return df_long


def process_all_files(download_dir=DOWNLOAD_DIR, output_dir=OUTPUT_DIR):
    """
    Process all downloaded XLS files
    """
    files = glob.glob(os.path.join(download_dir, "*.xls"))
    print(f"\nFound {len(files)} XLS files")
    
    results = []
    for filepath in files:
        print()
        result = parse_epwrf_xls(filepath)
        if result:
            saved_path = save_cleaned_data(result, output_dir)
            results.append({
                'source': filepath,
                'output': saved_path,
                'metadata': result['metadata'],
                'rows': len(result['data'])
            })
    
    return results


def merge_all_files(output_dir=OUTPUT_DIR, output_file='ASI_All_Variables_Combined.csv'):
    """
    Merge all processed CSV files into one combined dataset
    """
    csv_files = glob.glob(os.path.join(output_dir, "ASI_NIC*.csv"))
    print(f"\nMerging {len(csv_files)} CSV files...")
    
    all_dfs = []
    for filepath in csv_files:
        df = pd.read_csv(filepath)
        # Convert to long format for merging
        df_long = to_long_format(df)
        all_dfs.append(df_long)
    
    combined = pd.concat(all_dfs, ignore_index=True)
    
    # Save
    output_path = os.path.join(output_dir, output_file)
    combined.to_csv(output_path, index=False)
    print(f"Saved combined data: {output_path}")
    print(f"Total rows: {len(combined):,}")
    
    return combined


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("EPWRF XLS Parser")
    print("="*60)
    
    # Process all files
    results = process_all_files()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    if results:
        for r in results:
            print(f"  {os.path.basename(r['source'])} -> {os.path.basename(r['output'])} ({r['rows']} rows)")
        
        # Also merge into combined file
        print()
        merge_all_files()
