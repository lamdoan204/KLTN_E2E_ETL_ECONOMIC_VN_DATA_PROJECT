import unicodedata
import re
def clean_text(s):
    # bỏ dấu tiếng Việt
    s = unicodedata.normalize('NFD', s)
    s = s.replace('đ','d').replace('Đ','D')

    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    
    # giữ lại chữ cái
    s = re.sub(r'[^a-zA-Z]', '', s)
    
    return s.lower()


print(clean_text('22. Vốn đầu tư thực hiện toàn xã hội theo giá hiện hành'))
