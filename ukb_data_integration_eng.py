import os
import pandas as pd
import csv
import re
import sys
from pathlib import Path
import argparse

def ukb_data_integration(input_list):
    """
    Process UKB data, query relevant data based on input fields or ID list
    
    Parameters:
    input_list (list): List containing field names (field) or field IDs (fieldid)
    
    Returns:
    tuple: (query table dictionary, mapping table file path)
    """
    # Define directory addresses
    base_dir = r"D:\ukb\raw"
    dict_file = os.path.join(base_dir, "Data_Dictionary_Showcase.csv")
    
    # Check if data dictionary file exists
    if not os.path.exists(dict_file):
        print(f"Error: Data dictionary file not found {dict_file}")
        return None, None
    
    # Create result data structures
    query_dict = {}  # Query table dictionary
    mapping_data = []  # Mapping table data
    
    # Process input parameters
    field_inputs = []
    fieldid_inputs = []
    
    # Check input list
    if not input_list:
        print("Error: Input list is empty")
        return None, None
    
    for item in input_list:
        if item is None:
            continue
            
        item = str(item).strip()
        if not item:
            continue
            
        if item.isdigit():
            fieldid_inputs.append(item)
        else:
            field_inputs.append(item)
    
    if not field_inputs and not fieldid_inputs:
        print("Error: No valid field names or field IDs in the input list")
        return None, None
    
    # Read data dictionary file
    print("Reading data dictionary file...")
    
    try:
        with open(dict_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            try:
                header = next(reader)  # Skip header
            except StopIteration:
                print("Error: Data dictionary file is empty or incorrectly formatted")
                return None, None
            
            # Confirm column indices
            path_idx = 0  # First column, relative path
            fieldid_idx = 2  # Third column, fieldid
            field_idx = 3  # Fourth column, field name
            
            # Check file format
            if len(header) <= max(path_idx, fieldid_idx, field_idx):
                print(f"Error: Data dictionary file format is incorrect, requires at least {max(path_idx, fieldid_idx, field_idx) + 1} columns")
                return None, None
            
            # Traverse data dictionary
            row_count = 0
            match_count = 0
            
            for row in reader:
                row_count += 1
                
                if not row or len(row) <= max(path_idx, fieldid_idx, field_idx):
                    continue
                    
                # Check if matches input conditions
                fieldid = row[fieldid_idx].strip()
                field = row[field_idx].strip() if len(row) > field_idx else ""
                
                if (fieldid in fieldid_inputs) or (field in field_inputs):
                    match_count += 1
                    
                    # Get relative path and convert to file path
                    rel_path = row[path_idx].strip()
                    
                    if not rel_path:
                        print(f"Warning: Path is empty for row {row_count}")
                        continue
                    
                    # Convert path format from "A > B > C" to "A/B/C.csv"
                    file_path = rel_path.replace(" > ", "/") + ".csv"
                    full_path = os.path.join(base_dir, file_path)
                    
                    # Add to mapping data
                    mapping_data.append({
                        "path": rel_path,
                        "file_path": file_path, 
                        "fieldid": fieldid,
                        "field": field
                    })
                    
                    # Add to query dictionary
                    if file_path not in query_dict:
                        query_dict[file_path] = []
                    
                    if fieldid not in query_dict[file_path]:
                        query_dict[file_path].append(fieldid)
            
            print(f"Processing complete, read {row_count} rows of data")
            
            if match_count == 0:
                print("Warning: No matching fields or IDs found")
                
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None, None
    
    # Save mapping table
    try:
        mapping_file = "ukb_field_mapping.csv"
        if mapping_data:
            mapping_df = pd.DataFrame(mapping_data)
            mapping_df.to_csv(mapping_file, index=False)
            print(f"Mapping table saved to {mapping_file}")
        else:
            print("No data to save to mapping table")
            mapping_file = None
    except Exception as e:
        print(f"Error saving mapping table: {str(e)}")
        mapping_file = None
    
    print(f"Found {len(mapping_data)} matches, involving {len(query_dict)} files")
    
    return query_dict, mapping_file

def parse_input_string(input_string):
    """
    Parse input string, convert comma-separated string to list
    
    Parameters:
    input_string (str): Comma-separated input string
    
    Returns:
    list: Parsed list
    """
    if not input_string:
        return []
    
    # Replace Chinese commas with English commas
    input_string = input_string.replace('，', ',')
        
    # Split input string by commas and remove leading/trailing spaces for each item
    items = [item.strip() for item in input_string.split(',')]
    # Filter out empty strings
    items = [item for item in items if item]
    
    return items

def find_id_column_in_directory(file_path, id_columns, base_dir):
    """
    Recursively search for files containing ID columns in specified directory
    
    Parameters:
    file_path (str): Target file path
    id_columns (list): List of possible ID column names
    base_dir (str): Base directory path
    
    Returns:
    tuple: (file path with ID column, ID column name, ID column data)
    """
    # Get directory and filename (without extension) of target file
    target_dir = os.path.dirname(file_path)
    target_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # Build path to folder with same name
    folder_path = os.path.join(target_dir, target_name)
    
    if not os.path.exists(folder_path):
        return None, None, None
    
    # Get number of rows in target file
    try:
        target_df = pd.read_csv(file_path, nrows=1)
        target_rows = sum(1 for _ in open(file_path, 'r')) - 1  # Subtract header row
    except:
        return None, None, None
    
    # Recursively search for files
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Try to read file
                    df = pd.read_csv(file_path)
                    
                    # Check if it contains ID column
                    for id_col in id_columns:
                        if id_col in df.columns:
                            # Check if row count matches
                            if len(df) == target_rows:
                                print(f"Found matching ID column {id_col} in file {file_path}")
                                return file_path, id_col, df[id_col]
                except:
                    continue
    
    return None, None, None

def extract_ukb_data(query_dict, output_dir=None, output_file="ukb_extracted_data.csv"):
    """
    Extract data for specified fields from query table and merge into one file
    
    Parameters:
    query_dict (dict): Query table dictionary, keys are file paths, values are field ID lists
    output_dir (str): Output directory, defaults to current directory
    output_file (str): Output filename, defaults to 'ukb_extracted_data.csv'
    
    Returns:
    pandas.DataFrame: Merged data table
    """
    if not query_dict:
        print("Error: Query table is empty")
        return pd.DataFrame()
    
    # Set output directory
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = "."
    
    base_dir = r"D:\ukb\raw"
    
    # Create empty DataFrame to store all data
    all_data = pd.DataFrame()
    
    # Define possible ID column names
    id_columns = ['participant.eid', 'eid']
    
    # Read data dictionary to get Field mappings
    dict_file = os.path.join(base_dir, "Data_Dictionary_Showcase.csv")
    fieldid_to_field = {}
    try:
        dict_df = pd.read_csv(dict_file)
        for _, row in dict_df.iterrows():
            fieldid_to_field[str(row['FieldID']).strip()] = row['Field'].strip()
    except Exception as e:
        print(f"Error reading data dictionary: {str(e)}")
    
    # Process each file
    print("Starting file processing...")
    total_files = len(query_dict)
    processed_files = 0
    
    def _read_csv_with_sep(file_path, nrows=None, usecols=None, chunksize=None):
        """
        Generic CSV reading function, automatically tries different separators
        
        Parameters:
        file_path (str): File path
        nrows (int, optional): Number of rows to read
        usecols (list, optional): Columns to read
        chunksize (int, optional): Chunk size
        
        Returns:
        tuple: (DataFrame or TextFileReader, separator used)
        """
        for sep in [',', '\t', ';', '|', ' ']:
            try:
                df = pd.read_csv(file_path, nrows=nrows, usecols=usecols, 
                               chunksize=chunksize, sep=sep)
                if isinstance(df, pd.DataFrame) and len(df.columns) > 1:
                    return df, sep
                elif hasattr(df, 'columns') and len(df.columns) > 1:
                    return df, sep
            except:
                continue
        return None, None

    def _handle_duplicate_columns(df, existing_cols=None):
        """
        Handle duplicate column names in DataFrame
        
        Parameters:
        df (pd.DataFrame): DataFrame to process
        existing_cols (set, optional): Set of existing column names and their corresponding DataFrame, to avoid naming conflicts
        
        Returns:
        tuple: (Processed DataFrame, rename mapping dictionary)
        """
        rename_map = {}
        #if existing_cols is None:
        # Handle duplicate columns within df
        dup_cols = df.columns[df.columns.duplicated()].unique()
        if len(dup_cols) > 0:
            for col in dup_cols:
                # Get list of all columns with the same name
                same_name_cols = [c for c in df.columns if c == col]
                # Keep first column, check if other columns are identical to the first
                first_col_data = df[same_name_cols[0]]
                for other_col in same_name_cols[1:]:
                    if df[other_col].equals(first_col_data):
                        # If content is identical, delete duplicate column
                        df = df.drop(columns=[other_col])
                        print(f"Column {other_col} has identical content to {same_name_cols[0]}, deleting duplicate column")
                    else:
                        # If content differs, rename
                        suffix = 'A'
                        new_col_name = f"{col}_{suffix}"
                        while new_col_name in rename_map.values() or new_col_name in df.columns:
                            suffix = chr(ord(suffix) + 1)
                            new_col_name = f"{col}_{suffix}"
                        rename_map[other_col] = new_col_name
                        print(f"Column {other_col} has different content from {same_name_cols[0]}, renaming to {new_col_name}")
        if existing_cols is not None:
            # Handle conflicts with existing columns
            common_cols = set(df.columns) & existing_cols - {'participant.eid'}
            for col in common_cols:
                suffix = 'A'
                new_col_name = f"{col}_{suffix}"
                while new_col_name in existing_cols or new_col_name in rename_map.values():
                    suffix = chr(ord(suffix) + 1)
                    new_col_name = f"{col}_{suffix}"
                rename_map[col] = new_col_name
                print(f"Column {col} conflicts with existing data, renaming to {new_col_name}")
    
        if rename_map:
            df = df.rename(columns=rename_map)
    
        return df, rename_map

    def _standardize_column_names(df):
        """
        Standardize column name format, convert "Name | Instance X | Array Y" to "Name_iX_aY"
        
        Parameters:
        df (pd.DataFrame): DataFrame to process
        
        Returns:
        pd.DataFrame: Processed DataFrame
        """
        rename_map = {}
        for col in df.columns:
            if '| Instance' in col:
                parts = col.split('|')
                base_name = parts[0].strip()
                instance_part = parts[1].strip()
                instance = instance_part.split()[-1]
                
                if len(parts) > 2 and 'Array' in parts[2]:
                    array_part = parts[2].strip()
                    array = array_part.split()[-1]
                    new_col = f"{base_name}_i{instance}_a{array}"
                else:
                    new_col = f"{base_name}_i{instance}"
                
                rename_map[col] = new_col
        
        if rename_map:
            df = df.rename(columns=rename_map)
            print(f"Column name format standardization complete, processed {len(rename_map)} column names")
        
        return df

    for file_path, fieldids in query_dict.items():
        processed_files += 1
        print(f"Processing file {processed_files}/{total_files}: {file_path}")
        
        full_path = os.path.join(base_dir, file_path)
        
        if not os.path.exists(full_path):
            print(f"Warning: File {full_path} does not exist")
            continue
        
        try:
            # Check file size
            file_size = os.path.getsize(full_path)
            file_size_mb = file_size / (1024 * 1024)
            print(f"File size: {file_size_mb:.2f} MB")
            
            # First try to read file header to get column names
            header_df, sep = _read_csv_with_sep(full_path, nrows=5)
            if header_df is None:
                print(f"Cannot read file {full_path}, skipping")
                continue
                
            columns = header_df.columns.tolist()
            print(f"File column count: {len(columns)}")
            
            # Find field ID columns
            valid_fieldids = []
            columns_to_extract = []
            column_mapping = {}  # For storing column name mapping relationships
            
            for fieldid in fieldids:
                matched = False
                
                # 1. First try FieldID matching (including batch format)
                if fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # Try to match participant.p + fieldid format (including batch and measurement number formats)
                    pattern = f'participant.p{fieldid}_i\d+(_a\d+)?'
                    matching_cols = [col for col in columns if re.match(pattern, col)]
                    if matching_cols:
                        # Sort by batch number and measurement number
                        matching_cols.sort(key=lambda x: (
                            int(re.search(r'_i(\d+)', x).group(1)),
                            int(re.search(r'_a(\d+)', x).group(1)) if re.search(r'_a(\d+)', x) else 0
                        ))
                        valid_fieldids.append(fieldid)
                        columns_to_extract.extend(matching_cols)
                        # Create corresponding column names for each batch
                        for col in matching_cols:
                            batch_num = re.search(r'_i(\d+)', col).group(1)
                            measure_num = re.search(r'_a(\d+)', col).group(1) if re.search(r'_a(\d+)', col) else None
                            if measure_num:
                                column_mapping[col] = f"{field_name}_i{batch_num}_a{measure_num}"
                            else:
                                column_mapping[col] = f"{field_name}_i{batch_num}"
                        matched = True
                        print(f"Field ID {fieldid} matched to columns via participant.p format: {', '.join(matching_cols)}")
                        continue
                    
                    # Try matching without batch format
                    participant_p_field = f'participant.p{fieldid}'
                    if participant_p_field in columns:
                        valid_fieldids.append(fieldid)
                        columns_to_extract.append(participant_p_field)
                        column_mapping[participant_p_field] = field_name
                        matched = True
                        print(f"Field ID {fieldid} matched to column {participant_p_field} via participant.p format")
                        continue
                
                # 2. If FieldID matching fails, try Field name matching
                if not matched and fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # Find all columns containing field_name
                    matching_cols = [col for col in columns if field_name in col]
                    if matching_cols:
                        # Sort by batch number and measurement number (if present)
                        matching_cols.sort(key=lambda x: (
                            int(re.search(r'_i(\d+)', x).group(1)) if re.search(r'_i(\d+)', x) else 0,
                            int(re.search(r'_a(\d+)', x).group(1)) if re.search(r'_a(\d+)', x) else 0
                        ))
                        valid_fieldids.append(fieldid)
                        columns_to_extract.extend(matching_cols)
                        # For Field-matched columns, keep original column names
                        for col in matching_cols:
                            column_mapping[col] = col
                        matched = True
                        print(f"Field ID {fieldid} matched to columns via Field name: {', '.join(matching_cols)}")
                        continue
                
                # 3. If all else fails, try other possible matching patterns
                if not matched:
                    for col in columns:
                        if (fieldid == col or 
                            fieldid in col or 
                            (fieldid.isdigit() and (col.startswith(fieldid + '-') or 
                                                  col.startswith(fieldid + '_') or
                                                  col == 'p' + fieldid or
                                                  col.startswith('f' + fieldid + '_')))):
                            valid_fieldids.append(fieldid)
                            columns_to_extract.append(col)
                            # For other matching patterns, keep original column names
                            column_mapping[col] = col
                            matched = True
                            print(f"Field ID {fieldid} matched to column {col} via other patterns")
                            break
            
            if not valid_fieldids:
                print(f"Warning: No valid fields found in file {file_path}, skipping this file")
                continue
            
            # Find ID column
            file_id_col = None
            for id_col in id_columns:
                if id_col in columns:
                    file_id_col = id_col
                    print(f"Using '{file_id_col}' as ID column")
                    break
            
            # If ID column not found in current file, try to find in directory
            if not file_id_col:
                print(f"ID column not found in current file, trying to find in directory...")
                id_file_path, file_id_col, id_data = find_id_column_in_directory(full_path, id_columns, base_dir)
                if id_file_path and file_id_col and id_data is not None:
                    print(f"Found ID column {file_id_col} in file {id_file_path}")
                else:
                    print(f"Warning: ID column not found in file {file_path}, skipping this file")
                    continue
            
            # Ensure ID column is included
            if file_id_col not in columns_to_extract:
                columns_to_extract.append(file_id_col)
                column_mapping[file_id_col] = 'participant.eid'  # Standardize to participant.eid as ID column name
            
            # Remove duplicates
            columns_to_extract = list(dict.fromkeys(columns_to_extract))
            
            # Decide how to read based on file size
            file_df = pd.DataFrame()
            
            if file_size_mb > 100:  # Use chunked reading for files larger than 100MB
                print(f"File is large, using chunked reading...")
                
                # Try to determine file separator
                sep = ','
                for test_sep in [',', '\t', ';', '|', ' ']:
                    try:
                        test_df = pd.read_csv(full_path, nrows=5, sep=test_sep)
                        if len(test_df.columns) > 1:
                            sep = test_sep
                            break
                    except:
                        continue
                
                # Prepare columns to read
                usecols = columns_to_extract
                usecols = list(dict.fromkeys([col for col in usecols if col in columns]))
                
                # Read in chunks
                chunks = pd.read_csv(full_path, sep=sep, usecols=usecols, chunksize=50000)
                chunk_count = 0
                
                for chunk in chunks:
                    chunk_count += 1
                    if chunk_count % 5 == 0:
                        print(f"Processed {chunk_count} data chunks...")
                    
                    if file_df.empty:
                        file_df = chunk.copy()
                    else:
                        file_df = pd.concat([file_df, chunk], ignore_index=True)
                
                print(f"Chunked reading complete, total {chunk_count} data chunks")
                
            else:
                # Try to determine file separator
                sep = ','
                for test_sep in [',', '\t', ';', '|', ' ']:
                    try:
                        test_df = pd.read_csv(full_path, nrows=5, sep=test_sep)
                        if len(test_df.columns) > 1:
                            sep = test_sep
                            break
                    except:
                        continue
                
                # Read entire file
                try:
                    file_df = pd.read_csv(full_path, sep=sep)
                except Exception as e:
                    print(f"Error reading file: {str(e)}")
                    continue
            
            # Extract required columns
            columns_to_extract = [col for col in columns_to_extract if col in file_df.columns]
            
            if len(columns_to_extract) <= 1:
                print(f"Warning: No valid columns found in loaded data, skipping this file")
                continue
            
            # Extract data
            extracted_df = file_df[columns_to_extract].copy()
            
            # If ID column is from another file, add ID column
            if file_id_col not in extracted_df.columns and id_data is not None:
                extracted_df['participant.eid'] = id_data
            
            # Rename columns and standardize format
            extracted_df = extracted_df.rename(columns=column_mapping)
            extracted_df = _standardize_column_names(extracted_df)
            
            # Merge data
            if all_data.empty:
                # Handle duplicate columns in initial data
                extracted_df, rename_map = _handle_duplicate_columns(extracted_df)
                if rename_map:
                    print(f"Initial data renaming complete, processed {len(rename_map)} duplicate columns")
                all_data = extracted_df
                print(f"Initialized merged data, contains {len(extracted_df.columns)} fields and {len(extracted_df)} rows")
            else:
                # Handle duplicate columns with existing data
                extracted_df, rename_map = _handle_duplicate_columns(extracted_df, set(all_data.columns))
                if rename_map:
                    print(f"Merged data renaming complete, processed {len(rename_map)} duplicate columns")
                
                # Use outer join to merge data, preserving all rows
                try:
                    all_data = pd.merge(all_data, extracted_df, on='participant.eid', how='outer')
                    print(f"After merging: all_data has {len(all_data)} rows, {len(all_data.columns)} columns")
                except Exception as e:
                    print(f"Error merging data: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            print(f"Extracted {len(valid_fieldids)} fields from {file_path}, total {len(extracted_df)} rows of data")
            
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Save merged data
    if not all_data.empty:
        # Ensure participant.eid is in the first column
        cols = ['participant.eid'] + [col for col in all_data.columns if col != 'participant.eid']
        all_data = all_data[cols]
        
        output_path = os.path.join(output_dir, output_file)
        all_data.to_csv(output_path, index=False)
        print(f"Merged data saved to {output_path}, total {len(all_data)} rows, {len(all_data.columns)} columns")
        
        # Output column information
        print("\nData column information:")
        for i, col in enumerate(all_data.columns):
            print(f"{i+1}. {col}")
            
        # Count unmatched fields
        print("\nUnmatched field statistics:")
        unmatched_fields = []
        for file_path, fieldids in query_dict.items():
            for fieldid in fieldids:
                if fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # Check if in any column name
                    found = False
                    for col in all_data.columns:
                        if (field_name in col or 
                            f"participant.p{fieldid}" in col or 
                            fieldid in col):
                            found = True
                            break
                    if not found:
                        unmatched_fields.append(f"{fieldid} ({field_name})")
        
        if unmatched_fields:
            print("The following fields have no matching columns, need to calculate or search manually:")
            for field in unmatched_fields:
                print(f"- {field}")
        else:
            print("All fields successfully matched")
    else:
        print("No data extracted")
    
    return all_data

def main():
    """
    Main function, process command line arguments and execute program
    """
    parser = argparse.ArgumentParser(description='UK Biobank Data Integration Tool')
    
    # Add arguments
    parser.add_argument('--input', '-i', type=str, help='Input field list, comma separated')
    parser.add_argument('--file', '-f', type=str, help='File containing field list, one field per line')
    parser.add_argument('--output', '-o', type=str, default='extracted_data', help='Output directory for extracted data')
    parser.add_argument('--extract', '-e', action='store_true', help='Whether to extract data')
    parser.add_argument('--mapping', '-m', type=str, default='ukb_field_mapping.csv', help='Mapping table output filename')
    
    args = parser.parse_args()
    
    # Get input list
    input_list = [3,4,5,6,31]
    
    if args.input:
        input_list = parse_input_string(args.input)
    elif args.file:
        try:
            with open(args.file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    if line:
                        input_list.append(line)
        except Exception as e:
            print(f"Error reading input file: {str(e)}")
            return
    else:
        # Interactive input
        input_string = input("Please enter the fields or ID list to query, separated by commas: ")
        input_list = parse_input_string(input_string)
    
    if not input_list:
        print("Error: No input parameters provided")
        return
    
    print(f"Processing input: {input_list}")
    
    # Execute data integration
    result_dict, mapping_file = ukb_data_integration(input_list)
    
    # Print results
    if result_dict:
        print("\nQuery table contents:")
        for file_path, fieldids in result_dict.items():
            print(f"File: {file_path}")
            print(f"Field IDs: {', '.join(fieldids)}")
            print("---")
        
        # Extract data
        if args.extract:
            print(f"\nExtracting data to {args.output} directory...")
            merged_data = extract_ukb_data(result_dict, args.output)
            print(f"\nData extraction complete")
        else:
            extract_prompt = input("\nExtract data? (y/n): ").strip().lower()
            if extract_prompt == 'y':
                print(f"\nExtracting data to {args.output} directory...")
                merged_data = extract_ukb_data(result_dict, args.output)
                print(f"\nData extraction complete")

# Example usage
if __name__ == "__main__":
    main()
