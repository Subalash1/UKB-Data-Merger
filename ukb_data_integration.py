import os
import pandas as pd
import csv
import re
import sys
from pathlib import Path
import argparse

def ukb_data_integration(input_list):
    """
    处理UKB数据，根据输入的字段或ID列表查询相关数据
    
    参数:
    input_list (list): 包含字段名(field)或字段ID(fieldid)的列表
    
    返回:
    tuple: (查询表字典, 映射表文件路径)
    """
    # 定义目录地址
    base_dir = r"D:\ukb\raw"
    dict_file = os.path.join(base_dir, "Data_Dictionary_Showcase.csv")
    
    # 检查数据字典文件是否存在
    if not os.path.exists(dict_file):
        print(f"错误: 找不到数据字典文件 {dict_file}")
        return None, None
    
    # 创建结果数据结构
    query_dict = {}  # 查询表字典
    mapping_data = []  # 映射表数据
    
    # 处理输入参数
    field_inputs = []
    fieldid_inputs = []
    
    # 检查输入列表
    if not input_list:
        print("错误: 输入列表为空")
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
        print("错误: 输入列表中没有有效的字段名或字段ID")
        return None, None
    
    # 读取数据字典文件
    print("正在读取数据字典文件...")
    
    try:
        with open(dict_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            try:
                header = next(reader)  # 跳过表头
            except StopIteration:
                print("错误: 数据字典文件为空或格式不正确")
                return None, None
            
            # 确认列索引
            path_idx = 0  # 第一列，相对路径
            fieldid_idx = 2  # 第三列，fieldid
            field_idx = 3  # 第四列，field名称
            
            # 检查文件格式
            if len(header) <= max(path_idx, fieldid_idx, field_idx):
                print(f"错误: 数据字典文件格式不正确，需要至少 {max(path_idx, fieldid_idx, field_idx) + 1} 列")
                return None, None
            
            # 遍历数据字典
            row_count = 0
            match_count = 0
            
            for row in reader:
                row_count += 1
                
                if not row or len(row) <= max(path_idx, fieldid_idx, field_idx):
                    continue
                    
                # 检查是否匹配输入条件
                fieldid = row[fieldid_idx].strip()
                field = row[field_idx].strip() if len(row) > field_idx else ""
                
                if (fieldid in fieldid_inputs) or (field in field_inputs):
                    match_count += 1
                    
                    # 获取相对路径并转换为文件路径
                    rel_path = row[path_idx].strip()
                    
                    if not rel_path:
                        print(f"警告: 行 {row_count} 的路径为空")
                        continue
                    
                    # 将路径格式从 "A > B > C" 转换为 "A/B/C.csv"
                    file_path = rel_path.replace(" > ", "/") + ".csv"
                    full_path = os.path.join(base_dir, file_path)
                    
                    # 添加到映射数据
                    mapping_data.append({
                        "path": rel_path,
                        "file_path": file_path, 
                        "fieldid": fieldid,
                        "field": field
                    })
                    
                    # 添加到查询字典
                    if file_path not in query_dict:
                        query_dict[file_path] = []
                    
                    if fieldid not in query_dict[file_path]:
                        query_dict[file_path].append(fieldid)
            
            print(f"处理完成，共读取 {row_count} 行数据")
            
            if match_count == 0:
                print("警告: 没有找到匹配的字段或ID")
                
    except Exception as e:
        print(f"处理数据时出错: {str(e)}")
        return None, None
    
    # 保存映射表
    try:
        mapping_file = "ukb_field_mapping.csv"
        if mapping_data:
            mapping_df = pd.DataFrame(mapping_data)
            mapping_df.to_csv(mapping_file, index=False)
            print(f"映射表已保存到 {mapping_file}")
        else:
            print("没有数据可保存到映射表")
            mapping_file = None
    except Exception as e:
        print(f"保存映射表时出错: {str(e)}")
        mapping_file = None
    
    print(f"找到 {len(mapping_data)} 个匹配项，涉及 {len(query_dict)} 个文件")
    
    return query_dict, mapping_file

def parse_input_string(input_string):
    """
    解析输入字符串，将逗号分隔的字符串转换为列表
    
    参数:
    input_string (str): 逗号分隔的输入字符串
    
    返回:
    list: 解析后的列表
    """
    if not input_string:
        return []
    
    # 替换中文逗号为英文逗号
    input_string = input_string.replace('，', ',')
        
    # 将输入字符串按逗号分隔，并去除每个项的前后空格
    items = [item.strip() for item in input_string.split(',')]
    # 过滤掉空字符串
    items = [item for item in items if item]
    
    return items

def find_id_column_in_directory(file_path, id_columns, base_dir):
    """
    在指定目录下递归查找包含ID列的文件
    
    参数:
    file_path (str): 目标文件路径
    id_columns (list): 可能的ID列名列表
    base_dir (str): 基础目录路径
    
    返回:
    tuple: (找到ID列的文件路径, ID列名, ID列数据)
    """
    # 获取目标文件的目录和文件名（不含扩展名）
    target_dir = os.path.dirname(file_path)
    target_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # 构建同名文件夹路径
    folder_path = os.path.join(target_dir, target_name)
    
    if not os.path.exists(folder_path):
        return None, None, None
    
    # 获取目标文件的行数
    try:
        target_df = pd.read_csv(file_path, nrows=1)
        target_rows = sum(1 for _ in open(file_path, 'r')) - 1  # 减去标题行
    except:
        return None, None, None
    
    # 递归查找文件
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # 尝试读取文件
                    df = pd.read_csv(file_path)
                    
                    # 检查是否包含ID列
                    for id_col in id_columns:
                        if id_col in df.columns:
                            # 检查行数是否匹配
                            if len(df) == target_rows:
                                print(f"在文件 {file_path} 中找到匹配的ID列 {id_col}")
                                return file_path, id_col, df[id_col]
                except:
                    continue
    
    return None, None, None

def extract_ukb_data(query_dict, output_dir=None, output_file="ukb_extracted_data.csv"):
    """
    从查询表中提取指定字段的数据，并合并输出到一个文件
    
    参数:
    query_dict (dict): 查询表字典，键为文件路径，值为字段ID列表
    output_dir (str): 输出目录，默认为当前目录
    output_file (str): 输出文件名，默认为'ukb_extracted_data.csv'
    
    返回:
    pandas.DataFrame: 合并后的数据表
    """
    if not query_dict:
        print("错误: 查询表为空")
        return pd.DataFrame()
    
    # 设置输出目录
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = "."
    
    base_dir = r"D:\ukb\raw"
    
    # 创建一个空的DataFrame用于存储所有数据
    all_data = pd.DataFrame()
    
    # 定义可能的ID列名
    id_columns = ['participant.eid', 'eid']
    
    # 读取数据字典获取Field映射
    dict_file = os.path.join(base_dir, "Data_Dictionary_Showcase.csv")
    fieldid_to_field = {}
    try:
        dict_df = pd.read_csv(dict_file)
        for _, row in dict_df.iterrows():
            fieldid_to_field[str(row['FieldID']).strip()] = row['Field'].strip()
    except Exception as e:
        print(f"读取数据字典时出错: {str(e)}")
    
    # 处理每个文件
    print("开始处理文件...")
    total_files = len(query_dict)
    processed_files = 0
    
    def _read_csv_with_sep(file_path, nrows=None, usecols=None, chunksize=None):
        """
        通用CSV读取函数，自动尝试不同的分隔符
        
        参数:
        file_path (str): 文件路径
        nrows (int, optional): 读取的行数
        usecols (list, optional): 要读取的列
        chunksize (int, optional): 分块大小
        
        返回:
        tuple: (DataFrame或TextFileReader, 使用的分隔符)
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
        处理DataFrame中的重复列名
        
        参数:
        df (pd.DataFrame): 要处理的DataFrame
        existing_cols (set, optional): 已存在的列名集合及其对应的DataFrame，用于避免命名冲突
        
        返回:
        tuple: (处理后的DataFrame, 重命名映射字典)
        """
        rename_map = {}
        #if existing_cols is None:
        # 处理df内部的重复列
        dup_cols = df.columns[df.columns.duplicated()].unique()
        if len(dup_cols) > 0:
            for col in dup_cols:
                # 获取所有同名列的列表
                same_name_cols = [c for c in df.columns if c == col]
                # 保留第一个列，检查其他列是否与第一个列相同
                first_col_data = df[same_name_cols[0]]
                for other_col in same_name_cols[1:]:
                    if df[other_col].equals(first_col_data):
                        # 如果内容相同，直接删除重复列
                        df = df.drop(columns=[other_col])
                        print(f"列 {other_col} 与 {same_name_cols[0]} 内容相同，删除重复列")
                    else:
                        # 如果内容不同，重命名
                        suffix = 'A'
                        new_col_name = f"{col}_{suffix}"
                        while new_col_name in rename_map.values() or new_col_name in df.columns:
                            suffix = chr(ord(suffix) + 1)
                            new_col_name = f"{col}_{suffix}"
                        rename_map[other_col] = new_col_name
                        print(f"列 {other_col} 与 {same_name_cols[0]} 内容不同，重命名为 {new_col_name}")
        if existing_cols is not None:
            # 处理与现有列的冲突
            common_cols = set(df.columns) & existing_cols - {'participant.eid'}
            for col in common_cols:
                suffix = 'A'
                new_col_name = f"{col}_{suffix}"
                while new_col_name in existing_cols or new_col_name in rename_map.values():
                    suffix = chr(ord(suffix) + 1)
                    new_col_name = f"{col}_{suffix}"
                rename_map[col] = new_col_name
                print(f"列 {col} 与已有数据冲突，重命名为 {new_col_name}")
    
        if rename_map:
            df = df.rename(columns=rename_map)
    
        return df, rename_map

    def _standardize_column_names(df):
        """
        统一列名格式，将 "Name | Instance X | Array Y" 转换为 "Name_iX_aY"
        
        参数:
        df (pd.DataFrame): 要处理的DataFrame
        
        返回:
        pd.DataFrame: 处理后的DataFrame
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
            print(f"列名格式统一完成，共处理 {len(rename_map)} 个列名")
        
        return df

    for file_path, fieldids in query_dict.items():
        processed_files += 1
        print(f"处理文件 {processed_files}/{total_files}: {file_path}")
        
        full_path = os.path.join(base_dir, file_path)
        
        if not os.path.exists(full_path):
            print(f"警告: 文件 {full_path} 不存在")
            continue
        
        try:
            # 检查文件大小
            file_size = os.path.getsize(full_path)
            file_size_mb = file_size / (1024 * 1024)
            print(f"文件大小: {file_size_mb:.2f} MB")
            
            # 先尝试读取文件头部，获取列名
            header_df, sep = _read_csv_with_sep(full_path, nrows=5)
            if header_df is None:
                print(f"无法读取文件 {full_path}，跳过")
                continue
                
            columns = header_df.columns.tolist()
            print(f"文件列数: {len(columns)}")
            
            # 查找字段ID列
            valid_fieldids = []
            columns_to_extract = []
            column_mapping = {}  # 用于存储列名映射关系
            
            for fieldid in fieldids:
                matched = False
                
                # 1. 首先尝试FieldID匹配（包括带批次的格式）
                if fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # 尝试匹配participant.p + fieldid格式（包括带批次和测量次数的格式）
                    pattern = f'participant.p{fieldid}_i\d+(_a\d+)?'
                    matching_cols = [col for col in columns if re.match(pattern, col)]
                    if matching_cols:
                        # 按批次号和测量次数排序
                        matching_cols.sort(key=lambda x: (
                            int(re.search(r'_i(\d+)', x).group(1)),
                            int(re.search(r'_a(\d+)', x).group(1)) if re.search(r'_a(\d+)', x) else 0
                        ))
                        valid_fieldids.append(fieldid)
                        columns_to_extract.extend(matching_cols)
                        # 为每个批次创建对应的列名
                        for col in matching_cols:
                            batch_num = re.search(r'_i(\d+)', col).group(1)
                            measure_num = re.search(r'_a(\d+)', col).group(1) if re.search(r'_a(\d+)', col) else None
                            if measure_num:
                                column_mapping[col] = f"{field_name}_i{batch_num}_a{measure_num}"
                            else:
                                column_mapping[col] = f"{field_name}_i{batch_num}"
                        matched = True
                        print(f"字段ID {fieldid} 通过participant.p格式匹配到列: {', '.join(matching_cols)}")
                        continue
                    
                    # 尝试匹配不带批次的格式
                    participant_p_field = f'participant.p{fieldid}'
                    if participant_p_field in columns:
                        valid_fieldids.append(fieldid)
                        columns_to_extract.append(participant_p_field)
                        column_mapping[participant_p_field] = field_name
                        matched = True
                        print(f"字段ID {fieldid} 通过participant.p格式匹配到列 {participant_p_field}")
                        continue
                
                # 2. 如果FieldID匹配失败，尝试Field字段匹配
                if not matched and fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # 查找所有包含field_name的列
                    matching_cols = [col for col in columns if field_name in col]
                    if matching_cols:
                        # 按批次号和测量次数排序（如果有）
                        matching_cols.sort(key=lambda x: (
                            int(re.search(r'_i(\d+)', x).group(1)) if re.search(r'_i(\d+)', x) else 0,
                            int(re.search(r'_a(\d+)', x).group(1)) if re.search(r'_a(\d+)', x) else 0
                        ))
                        valid_fieldids.append(fieldid)
                        columns_to_extract.extend(matching_cols)
                        # 对于Field匹配的列，保持原列名
                        for col in matching_cols:
                            column_mapping[col] = col
                        matched = True
                        print(f"字段ID {fieldid} 通过Field名称匹配到列: {', '.join(matching_cols)}")
                        continue
                
                # 3. 如果都失败了，尝试其他可能的匹配模式
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
                            # 对于其他匹配模式，保持原列名
                            column_mapping[col] = col
                            matched = True
                            print(f"字段ID {fieldid} 通过其他模式匹配到列 {col}")
                            break
            
            if not valid_fieldids:
                print(f"警告: 在文件 {file_path} 中未找到任何有效字段，跳过此文件")
                continue
            
            # 查找ID列
            file_id_col = None
            for id_col in id_columns:
                if id_col in columns:
                    file_id_col = id_col
                    print(f"使用 '{file_id_col}' 作为ID列")
                    break
            
            # 如果在当前文件中未找到ID列，尝试在目录中查找
            if not file_id_col:
                print(f"在当前文件中未找到ID列，尝试在目录中查找...")
                id_file_path, file_id_col, id_data = find_id_column_in_directory(full_path, id_columns, base_dir)
                if id_file_path and file_id_col and id_data is not None:
                    print(f"在文件 {id_file_path} 中找到ID列 {file_id_col}")
                else:
                    print(f"警告: 在文件 {file_path} 中未找到ID列，跳过此文件")
                    continue
            
            # 确保包含ID列
            if file_id_col not in columns_to_extract:
                columns_to_extract.append(file_id_col)
                column_mapping[file_id_col] = 'participant.eid'  # 统一使用participant.eid作为ID列名
            
            # 去重
            columns_to_extract = list(dict.fromkeys(columns_to_extract))
            
            # 根据文件大小决定如何读取
            file_df = pd.DataFrame()
            
            if file_size_mb > 100:  # 大于100MB使用分块读取
                print(f"文件较大，使用分块读取...")
                
                # 尝试确定文件的分隔符
                sep = ','
                for test_sep in [',', '\t', ';', '|', ' ']:
                    try:
                        test_df = pd.read_csv(full_path, nrows=5, sep=test_sep)
                        if len(test_df.columns) > 1:
                            sep = test_sep
                            break
                    except:
                        continue
                
                # 准备要读取的列
                usecols = columns_to_extract
                usecols = list(dict.fromkeys([col for col in usecols if col in columns]))
                
                # 分块读取
                chunks = pd.read_csv(full_path, sep=sep, usecols=usecols, chunksize=50000)
                chunk_count = 0
                
                for chunk in chunks:
                    chunk_count += 1
                    if chunk_count % 5 == 0:
                        print(f"已处理 {chunk_count} 个数据块...")
                    
                    if file_df.empty:
                        file_df = chunk.copy()
                    else:
                        file_df = pd.concat([file_df, chunk], ignore_index=True)
                
                print(f"完成分块读取，共 {chunk_count} 个数据块")
                
            else:
                # 尝试确定文件的分隔符
                sep = ','
                for test_sep in [',', '\t', ';', '|', ' ']:
                    try:
                        test_df = pd.read_csv(full_path, nrows=5, sep=test_sep)
                        if len(test_df.columns) > 1:
                            sep = test_sep
                            break
                    except:
                        continue
                
                # 读取整个文件
                try:
                    file_df = pd.read_csv(full_path, sep=sep)
                except Exception as e:
                    print(f"读取文件时出错: {str(e)}")
                    continue
            
            # 提取所需列
            columns_to_extract = [col for col in columns_to_extract if col in file_df.columns]
            
            if len(columns_to_extract) <= 1:
                print(f"警告: 在已加载的数据中未找到有效列，跳过此文件")
                continue
            
            # 提取数据
            extracted_df = file_df[columns_to_extract].copy()
            
            # 如果ID列是从其他文件获取的，添加ID列
            if file_id_col not in extracted_df.columns and id_data is not None:
                extracted_df['participant.eid'] = id_data
            
            # 重命名列并统一格式
            extracted_df = extracted_df.rename(columns=column_mapping)
            extracted_df = _standardize_column_names(extracted_df)
            
            # 合并数据
            if all_data.empty:
                # 处理初始数据中的重复列
                extracted_df, rename_map = _handle_duplicate_columns(extracted_df)
                if rename_map:
                    print(f"初始数据重命名完成，处理了 {len(rename_map)} 个重复列")
                all_data = extracted_df
                print(f"初始化合并数据，包含 {len(extracted_df.columns)} 个字段和 {len(extracted_df)} 行")
            else:
                # 处理与现有数据的重复列
                extracted_df, rename_map = _handle_duplicate_columns(extracted_df, set(all_data.columns))
                if rename_map:
                    print(f"合并数据重命名完成，处理了 {len(rename_map)} 个重复列")
                
                # 使用outer连接合并数据，保留所有行
                try:
                    all_data = pd.merge(all_data, extracted_df, on='participant.eid', how='outer')
                    print(f"合并后：all_data 有 {len(all_data)} 行，{len(all_data.columns)} 列")
                except Exception as e:
                    print(f"合并数据时出错: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            print(f"从 {file_path} 中提取了 {len(valid_fieldids)} 个字段，共 {len(extracted_df)} 行数据")
            
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 保存合并后的数据
    if not all_data.empty:
        # 确保participant.eid在第一列
        cols = ['participant.eid'] + [col for col in all_data.columns if col != 'participant.eid']
        all_data = all_data[cols]
        
        output_path = os.path.join(output_dir, output_file)
        all_data.to_csv(output_path, index=False)
        print(f"已将合并数据保存到 {output_path}，共 {len(all_data)} 行，{len(all_data.columns)} 列")
        
        # 输出列名信息
        print("\n数据列信息:")
        for i, col in enumerate(all_data.columns):
            print(f"{i+1}. {col}")
            
        # 统计未匹配的字段
        print("\n未匹配字段统计:")
        unmatched_fields = []
        for file_path, fieldids in query_dict.items():
            for fieldid in fieldids:
                if fieldid in fieldid_to_field:
                    field_name = fieldid_to_field[fieldid]
                    # 检查是否在任何列名中
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
            print("以下字段未找到匹配的列，需要自行计算或查找:")
            for field in unmatched_fields:
                print(f"- {field}")
        else:
            print("所有字段都已成功匹配")
    else:
        print("没有提取到任何数据")
    
    return all_data

def main():
    """
    主函数，处理命令行参数并执行程序
    """
    parser = argparse.ArgumentParser(description='UK Biobank数据整合工具')
    
    # 添加参数
    parser.add_argument('--input', '-i', type=str, help='输入字段列表，用逗号分隔')
    parser.add_argument('--file', '-f', type=str, help='包含字段列表的文件，每行一个字段')
    parser.add_argument('--output', '-o', type=str, default='extracted_data', help='提取数据的输出目录')
    parser.add_argument('--extract', '-e', action='store_true', help='是否提取数据')
    parser.add_argument('--mapping', '-m', type=str, default='ukb_field_mapping.csv', help='映射表输出文件名')
    
    args = parser.parse_args()
    
    # 获取输入列表
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
            print(f"读取输入文件时出错: {str(e)}")
            return
    else:
        # 交互式输入
        input_string = input("请输入要查询的字段或ID列表，用逗号分隔: ")
        input_list = parse_input_string(input_string)
    
    if not input_list:
        print("错误: 没有提供输入参数")
        return
    
    print(f"处理输入: {input_list}")
    
    # 执行数据整合
    result_dict, mapping_file = ukb_data_integration(input_list)
    
    # 打印结果
    if result_dict:
        print("\n查询表内容:")
        for file_path, fieldids in result_dict.items():
            print(f"文件: {file_path}")
            print(f"字段ID: {', '.join(fieldids)}")
            print("---")
        
        # 提取数据
        if args.extract:
            print(f"\n正在提取数据到 {args.output} 目录...")
            merged_data = extract_ukb_data(result_dict, args.output)
            print(f"\n数据提取完成")
        else:
            extract_prompt = input("\n是否提取数据? (y/n): ").strip().lower()
            if extract_prompt == 'y':
                print(f"\n正在提取数据到 {args.output} 目录...")
                merged_data = extract_ukb_data(result_dict, args.output)
                print(f"\n数据提取完成")

# 示例用法
if __name__ == "__main__":
    main()