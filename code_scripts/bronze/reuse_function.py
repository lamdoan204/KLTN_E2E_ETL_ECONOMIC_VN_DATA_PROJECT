import unicodedata
import re
def clean_text(s):
    # bỏ dấu tiếng Việt
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    
    # giữ lại chữ cái
    s = re.sub(r'[^a-zA-Z]', '', s)
    
    return s.lower()


import subprocess
import os 

def convert_xls_to_xlsx_file(input_path, output_dir):
    subprocess.run([
        'soffice',
        '--headless',
        '--convert-to', 'xlsx',
        input_path,
        '--outdir', output_dir,
    ], check= True)
    filename = os.path.basename(input_path).replace(".xls", ".xlsx")
    
    return os.path.join(output_dir, filename)