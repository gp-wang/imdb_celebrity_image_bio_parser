from PIL import Image
from tinydb import TinyDB, Query
import base64
import io
from pdb import set_trace as bp

# note: with 'encoding' in name, it is always a bytes obj

in_jpg_encoding = None

# open some randome image
with open('rkt.jpg', 'rb') as f:
    # The file content is a jpeg encoded bytes object
    in_jpg_encoding = f.read()


# output is a bytes object    
in_b64_encoding = base64.b64encode(in_jpg_encoding)

# interpret above bytes as str, because json value need to be string
# in_str = in_b64_encoding.decode(encoding='utf-8')
in_str = str(in_b64_encoding, encoding='utf-8')  # alternative way of above statement


# simulates a transmission, e.g. sending the image data over internet using json 
out_str = in_str


# strip-off the utf-8 interpretation to restore it as a base64 encoding
out_utf8_encoding = out_str.encode(encoding='utf-8')
# out_utf8_encoding = out_str.encode() # same way of writing above statement


# strip off the base64 encoding to restore it as its original jpeg encoded conent
# note: output is still a bytes obj due to b64 decoding
out_b64_decoding = base64.b64decode(out_utf8_encoding)

out_jpg_encoding = out_b64_decoding



# ---- verification stage
out_jpg_file = io.BytesIO(out_jpg_encoding)
out_jpg_image = Image.open(out_jpg_file)
out_jpg_image.show()
