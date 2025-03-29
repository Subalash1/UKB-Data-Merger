# UK Biobank Data Integration Tool

This is a Python tool for processing UK Biobank data, capable of querying relevant data based on field names or field IDs, and generating data mapping tables and extracting required data.

## Features

- Query data locations based on field names or field IDs
- Automatically parse UKB data dictionary
- Generate field mapping tables
- Extract data for specified fields and automatically merge
- Support for large file processing and intelligent column matching
- Automatic ID column recognition and handling of different CSV formats
- Command line arguments and interactive operation

## Usage

### Basic Usage

```bash
# Interactive use
python ukb_data_integration_eng.py

# Specify fields using command line arguments
python ukb_data_integration_eng.py --input "21022,Age at recruitment,31,Sex"

# Read field list from file
python ukb_data_integration_eng.py --file field_list.txt

# Direct data extraction
python ukb_data_integration_eng.py --input "21022,31" --extract

# Specify output directory
python ukb_data_integration_eng.py --input "21022,31" --output my_extracted_data --extract
```

### Parameter Description

- `--input`, `-i`: Input field list, comma separated
- `--file`, `-f`: File containing field list, one field per line
- `--output`, `-o`: Output directory for extracted data, default is 'extracted_data'
- `--extract`, `-e`: Whether to extract data, add this parameter for automatic extraction
- `--mapping`, `-m`: Mapping table output filename, default is 'ukb_field_mapping.csv'

## Input Format

- Can use field names (field) or field IDs (fieldid) for queries
- Multiple query items separated by commas
- Field names use English names, such as "Age at recruitment", "Sex", etc.
- Field IDs use numbers, such as "21022", "31", etc.

## Output Content

1. **Query Table** - Contains file paths and corresponding field IDs
2. **Mapping Table** - CSV file containing original paths, file paths, field IDs, and field names
3. **Merged Data** - Contains data for all fields, merged into a single CSV file by eid

## Data Extraction and Merging Features

The data extraction functionality in the new version has the following features:

- Automatic recognition of ID columns in different files (eid, participant_id, etc.)
- Support for CSV files with various separators (comma, tab, semicolon, etc.)
- Intelligent matching of field IDs and actual column names
- Large file chunked processing to reduce memory usage
- Automatic merging of data from multiple files by ID column (eid)
- Records with the same ID are merged into a single row
- Output file has ID as the first column, followed by queried fields
- Support for handling duplicate column names and conflict resolution

## Directory Structure

- Raw data directory is set to `D:\ukb\raw`
- Data dictionary file is `D:\ukb\raw\Data_Dictionary_Showcase.csv`
- Output data is saved by default in the `extracted_data` folder in the current directory

## Examples

Query example:
```python
sample_input = ["21022", "Age at recruitment", "31", "Sex"]
result_dict, mapping_file = ukb_data_integration(sample_input)
```

Data extraction example:
```python
output_dir = "extracted_data"
merged_data = extract_ukb_data(result_dict, output_dir)
```

## System Requirements

- Python 3.6+
- Dependencies: pandas, pathlib

## Installing Dependencies

```bash
pip install pandas pathlib
```

## Notes

- If multiple files contain records with the same ID, these records will be merged into a single row
- Duplicate fields will use the original value; in case of conflicts, the first occurring value will be kept
- Processing large files may require significant memory, it's recommended to run on a machine with sufficient resources
