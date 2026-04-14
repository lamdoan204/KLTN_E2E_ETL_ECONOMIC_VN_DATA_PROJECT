import unicodedata
import re
def clean_text(s):
    # bỏ dấu tiếng Việt
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    
    # giữ lại chữ cái
    s = re.sub(r'[^a-zA-Z]', '', s)
    
    return s.lower()