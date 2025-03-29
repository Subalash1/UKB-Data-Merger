# UKB数据整合工具

这是一个用于处理UK Biobank数据的Python工具，能够根据字段名或字段ID查询相关数据，并生成数据映射表和提取所需数据。

## 功能特点

- 根据字段名或字段ID查询数据位置
- 自动解析UKB数据字典
- 生成字段映射表
- 提取指定字段的数据并自动合并
- 支持大文件处理和智能列匹配
- 自动识别ID列和处理不同格式的CSV文件
- 命令行参数和交互式操作

## 使用方法

### 基本用法

```bash
# 交互式使用
python ukb_data_integration.py

# 使用命令行参数指定字段
python ukb_data_integration.py --input "21022,Age at recruitment,31,Sex"

# 从文件读取字段列表
python ukb_data_integration.py --file field_list.txt

# 直接提取数据
python ukb_data_integration.py --input "21022,31" --extract

# 指定输出目录
python ukb_data_integration.py --input "21022,31" --output my_extracted_data --extract
```

### 参数说明

- `--input`, `-i`: 输入字段列表，用逗号分隔
- `--file`, `-f`: 包含字段列表的文件，每行一个字段
- `--output`, `-o`: 提取数据的输出目录，默认为'extracted_data'
- `--extract`, `-e`: 是否提取数据，添加此参数则自动提取
- `--mapping`, `-m`: 映射表输出文件名，默认为'ukb_field_mapping.csv'

## 输入格式

- 可以使用字段名(field)或字段ID(fieldid)进行查询
- 多个查询项使用逗号分隔
- 字段名使用英文名称，如"Age at recruitment", "Sex"等
- 字段ID使用数字，如"21022", "31"等

## 输出内容

1. **查询表** - 包含文件路径和对应的字段ID
2. **映射表** - CSV文件，包含原始路径、文件路径、字段ID和字段名
3. **合并数据** - 包含所有字段的数据，按eid合并为单个CSV文件

## 数据提取和合并功能

新版本的数据提取功能有如下特点：

- 自动识别不同文件中的ID列（eid、participant_id等）
- 支持多种分隔符的CSV文件（逗号、制表符、分号等）
- 智能匹配字段ID和实际列名
- 大文件分块处理，减少内存使用
- 按ID列（eid）自动合并多个文件的数据
- 相同ID的记录会合并为同一行
- 输出文件中第一列为ID，其他列为查询的字段
- 支持处理重复列名和冲突解决

## 目录结构

- 原始数据目录设置为`D:\ukb\raw`
- 数据字典文件为`D:\ukb\raw\Data_Dictionary_Showcase.csv`
- 输出数据默认保存在当前目录下的`extracted_data`文件夹

## 示例

查询示例：
```python
sample_input = ["21022", "Age at recruitment", "31", "Sex"]
result_dict, mapping_file = ukb_data_integration(sample_input)
```

提取数据示例：
```python
output_dir = "extracted_data"
merged_data = extract_ukb_data(result_dict, output_dir)
```

## 系统要求

- Python 3.6+
- 依赖包：pandas, pathlib

## 安装依赖

```bash
pip install pandas pathlib
```

## 注意事项

- 如果多个文件包含相同ID的记录，这些记录会被合并到同一行
- 重复的字段会使用原始值，如果有冲突，将保留第一个出现的值
- 处理大文件时可能需要较多内存，建议在有足够资源的机器上运行 