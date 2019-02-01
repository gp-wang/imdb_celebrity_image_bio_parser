from PIL import Image
from tinydb import TinyDB, Query
import base64
import io
from pdb import set_trace as bp

in_jpeg_encoding = b''

with open("rkt.jpg", "rb") as f:
    in_jpeg_encoding = f.read()

# gw:
# raw input is a encoding of 'jpg-encode'
# wrap it with 1st layer of encoding: base64
in_b64_encoding = base64.b64encode(in_jpeg_encoding)

in_b64_str = str(in_b64_encoding, encoding='utf8')


# in_img.show()

db = TinyDB('db.json')

db.insert({'type': 'image', 'value': in_b64_str})

ImageQuery = Query()
res = db.search(ImageQuery.type == 'image')
bp()
out_b64_str = res[0]['value']

# gw: encode the str into bytes to remove the layer of utf-8
out_b64_encoding = out_b64_str.encode(encoding='utf-8')

out_jpeg_encoding = base64.b64decode(out_b64_encoding)

out_img_file = io.BytesIO(out_jpeg_encoding)
out_img = Image.open(out_img_file)

out_img.show()
