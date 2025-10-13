# extract_code.py
from urllib.parse import urlparse, parse_qs

print("請貼上完整的 URL (從 Safari 地址欄複製):")
url = input().strip()

# 解析 URL 提取 code
parsed = urlparse(url)
params = parse_qs(parsed.query)

if 'code' in params:
    code = params['code'][0]
    print("\n✅ 授權碼:")
    print(code)
    print("\n(已複製到剪貼簿,可以直接貼上)")
else:
    print("❌ 找不到 code 參數")