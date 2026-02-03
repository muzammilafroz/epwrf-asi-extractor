# EPWRF ASI Data Extractor

Automated data extraction tool for EPWRF India Time Series - Annual Survey of Industries (ASI) manufacturing data.

## Overview

This tool extracts manufacturing industry data (NIC codes 10-33) from the [EPWRF India Time Series](https://epwrfits.in) database, focusing on the EPWRF Concorded Series which provides harmonized long-term data across different NIC classification versions.

## Research Focus

- **GVA & Output**: Gross Value Added, Net Value Added, Total Output
- **Employment**: Workers, Employees, Gender-disaggregated, Man-hours
- **Capital**: Fixed Capital, Working Capital, Capital Formation
- **Wages**: Emoluments, Wages to Workers, PF contributions
- **Inputs & Costs**: Materials, Fuels, Depreciation

## Data Sources

| Subsection | Description | Batch Size |
|------------|-------------|------------|
| 2 | All India 2-Digit Industries | 25 industries |
| 7 | States × 2-Digit Industries | 3 industries |
| 10 | Employment Characteristics | 25 industries |
| 11 | Employment by States | 3 industries |

## Installation

```bash
pip install -r requirements.txt
```

Requires Chrome browser and ChromeDriver.

## Usage

```bash
# Extract All India 2-digit industry data
python asi_research_extractor.py --subsection 2 --output ./asi_data

# Extract state-wise data
python asi_research_extractor.py --subsection 7 --output ./asi_data

# Extract employment data
python asi_research_extractor.py --subsection 10 --output ./asi_data

# Extract all research subsections
python asi_research_extractor.py --all --output ./asi_data

# Test mode (limited extraction for validation)
python asi_research_extractor.py --subsection 2 --test --output ./asi_test
```

## Output Structure

```
asi_data/
├── raw/
│   ├── sub02_allIndia_2digit/
│   │   └── epwrf_concorded/
│   │       ├── sub02_var01_gross_value_added_batch01_nic10-12.xls
│   │       └── ...
│   ├── sub07_states_2digit/
│   ├── sub10_employment/
│   └── sub11_employment_states/
├── logs/
├── extraction_log.json      # Full metadata for ETL
├── manifest.json            # Extraction summary
└── database/
    └── asi_research.db      # (After ETL processing)
```

## File Naming Convention

```
sub{SS}_var{VV}_{variable_name}_batch{BB}_nic{XX-YY}.xls
```

- `{SS}`: Subsection number (02, 07, 10, 11)
- `{VV}`: Variable index (01, 02, ...)
- `{variable_name}`: Cleaned variable name
- `{BB}`: Batch number
- `{XX-YY}`: NIC code range in batch

## Technical Notes

- Uses Selenium for browser automation
- Downloaded XLS files are HTML tables (not real Excel format)
- State-level subsections use smaller batches (3 industries) due to website performance
- Display page timeout: 180 seconds (pages can take 1-2 minutes)
- Empty data detection: Script skips variables with no available data

## License

MIT
