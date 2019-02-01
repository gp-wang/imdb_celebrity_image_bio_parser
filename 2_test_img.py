from PIL import Image
from tinydb import TinyDB, Query
import pickle
import base64


in_img = Image.open('rkt.jpg')

# in_img.show()

in_img_bytes = pickle.dumps(in_img)

# gw: encoding direction: byptes to str, don't use 'utf8' should use base64
# in_img_str = str(in_img_bytes, encoding='utf8')
# in_img_str = in_img_bytes.decode()

# https://www.programcreek.com/2013/09/convert-image-to-string-in-python/
# gw: note: in is byte, out is also byte
in_img_b64_bytes = base64.b64encode(in_img_bytes)
print("length of formatting pillow obj is {}".format(len(in_img_b64_bytes)))

in_img_file_bytes = None
with open('rkt2.jpg', 'rb') as f:
    in_img_file_bytes = f.read()

in_img_file_b64_bytes = base64.b64encode(in_img_file_bytes)
print("length of formatting file obj is {}".format(len(in_img_file_b64_bytes)))


out_img_file_bytes = base64.b64decode(in_img_file_b64_bytes)

# gw: bad method. Need to explicitly specify image size
# out_img_from_file = Image.frombytes('RGBA', out_img_file_bytes, 'raw')  
# out_img_from_file.show()


# gw: prefered way to open image from bytes, compared to Image.frombytes
# https://stackoverflow.com/q/14759637/8328365
import io
stream = io.BytesIO(out_img_file_bytes)
out_img_from_file = Image.open(stream)
out_img_from_file.show()





# # gw: not working, need to use base64.b64decode
# # out_img_bytes = in_img_str.decode('base64')
# out_img_bytes = base64.b64decode(in_img_b64_bytes)


# out_img = pickle.loads(out_img_bytes)

# out_img.show()
