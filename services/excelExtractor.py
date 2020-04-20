import json
import xlrd
import datetime
import sys
sys.path.append('..')
from helpers.utils import get_date_from_string
import helpers.log as log

## DATA DEFINITIONS

## SHEET is list(SECTION)
## SECTION is list(BLOCK)
## BLOCK is list(ROW)
## ROW is list(COLUMN)
## COLUMN is tuple

## SHEET[
#   SECTION[
#     BLOCK[
#       ROW{
#        COLUMN
#       }
#     ]
#   ]
#  ]

## FUNCTIONS

# json list(string) -> list(dict)
# Consumes a template and a list of file names and extracts the SHEET
# ASSUME: list(string) is composed of valid excel file paths
def extract_files(file_list, json):
    if(file_list is None or len(file_list) == 0):
        return []
    
    first, rest = file_list[0], file_list[1:]

    wb = xlrd.open_workbook(first)  
    
    asof_date = get_date_from_string(first)

    if(asof_date == 20000101):
        log.error(__file__, 'extract_files', "Can't imply asof date from file name")

    return _extract_sheets(json, wb) + extract_files(rest, json)

def _extract_sheets(sheet_templates, wb):
    if(len(sheet_templates) == 0):
        return []
    else:
        first, rest = sheet_templates[0], sheet_templates[1:]
        xl_sheet = wb.sheet_by_name(first["name"])  
        sheet = [xl_sheet.col_values(i, 0, xl_sheet.nrows) for i in range(xl_sheet.ncols)]

        return _extract_sections(first["columns"], sheet) + _extract_sheets(rest, wb)

# json list(list(object)) -> list(dict)
# Consumes a list of templates and extracts the SECTIONS from a SHEET
def _extract_sections(section_templates, sheet):
    if(len(sheet) == 0):
        return []

    first_section_template, rest_section_templates = section_templates[0], section_templates[1:]
    first_section = [series for ind, series in enumerate(sheet) if ind < first_section_template["end"]]
    rest_sections = [series for ind, series in enumerate(sheet) if ind > first_section_template["end"]]

    return _extract_blocks(
        first_section_template["blocks"], list(zip(*first_section))) + _extract_sections(
            rest_section_templates, rest_sections)


# json list(tuple) -> list(ROW)
# Consume a list of templates and extracts the BLOCKS from a SECTION
def _extract_blocks(block_templates, section):
    if(len(block_templates) == 0):
        return []

    first_block, rest_blocks = block_templates[0], block_templates[1:]

    block_start = _get_block_start(section, first_block["name"], first_block["y_start"]) 
    block_end = _get_block_end(section, block_start, len(block_templates))
   
    first = _extract_rows(first_block["columns"], [x for ind, x in enumerate(section) if block_start <= ind < block_end])
    rest = [x for ind, x in enumerate(section) if ind > block_end]

    d = {}
    d["name"] = first_block["name"]
    d["rows"] = first

    return [d] + _extract_blocks(rest_blocks, rest)


# json list(tuple) -> list(ROW)
# Consume a list of templates and extractts the ROWS from a BLOCK
def _extract_rows(column_templates, block):
    if(len(block) == 0):
        return []

    first = _extract_columns(column_templates, block[0])
    rest = block[1:] 
    
    return [first] + _extract_rows(column_templates, rest) 
  

# json tuple -> COLUMNS
# Consume a list of templates and extractts the COLUMNS from a ROW
def _extract_columns(row_template, row):
    d = {}
    for i in range(len(row_template)) :
        if(row_template[i]["name"] != 'ignore'):
            d[row_template[i]["name"]] = _get_value(row, row_template[i], i)

    return d    


# list(tuple) int -> int 
# Consumes a list(ROW) and returns the index of the first row that is non-empty
def _get_block_start(rows, name, skip_rows):
    for i in range(len(rows)):
        if(len(name) > 0):
            if(str(rows[i][0]).lower().strip() == name.lower().strip()):
                return i + skip_rows
    return 0 + skip_rows


# list(tuple) int -> int 
# Consumes a list(ROW) and returns the index of the first row after the start,
# that's empty or contains unwanted special characters (e.g. *)
def _get_block_end(rows, start, num_blocks=None):
    #"*" == str(sheet.cell_value(row, section["start"]))[0]
    if(num_blocks is not None):
        if(num_blocks == 1):
            return 140000

    for i in range(len(rows)):
        if(i >= start and len(str(rows[i][0])) == 0 or rows[i][0] is None):
            return i

    return len(rows)


# tuple dict -> string
# Consume cell values and find the index from column dict
# If not found, it is a custom column:
# - if column['value'] is 'date' then get datestamp
# - else use column['value']
def _get_value(values, column, i):
    try:
        if (column["name"] == 'Date'):
            datestamp = datetime.datetime(*xlrd.xldate_as_tuple(values[i],datemode=0)[0:3])
            return datestamp.strftime("%Y-%m-%d")
        try:
            return round(values[i],4)
        except:
            return values[i]
    except:
        if ("value" in column):
            if(column["value"] == 'date'):
                return datetime.date.today().strftime("%d-%m-%Y")
            else:
                return column["value"]

    # row_dict[config_column["ours"]] = re.sub("([0-9])(?:m)", r'\1', sheet.cell_value(row, x).replace('*',''))  # TODO - this only works for McQuilling





if (__name__ == "__main__"):
    with open("../templates/global.json") as file:
        data = json.load(file)
        print(extract_files(["C:/dev/temp/Energy Aspects/ea_runs.xlsx"], data))