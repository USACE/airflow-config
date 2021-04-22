
import codecs

def convert_hex_to_ascii(h):
    chars_in_reverse = []
    while h != 0x0:
        chars_in_reverse.append(chr(h & 0xFF))
        h = h >> 8

    chars_in_reverse.reverse()
    return ''.join(chars_in_reverse)

# print(bytearray.fromhex("03 18 12 12 0F 00").decode(encoding="utf-8"))

# print(codecs.encode(b"03/24/18 18:15:00", "hex"))


# import binascii

# x = b'03/24/18 18:15:00'
# x = binascii.hexlify(x)
# y = str(x,'ascii')

# print(x) # Outputs b'74657374' (hex encoding of "test")
# print(y) # Outputs 74657374

# x_unhexed = binascii.unhexlify(x)
# print(x_unhexed) # Outputs b'test'

# x_ascii = str(x_unhexed,'ascii')
# print(x_ascii) # Outputs test


import binascii

data = '03/24/18 18:15:00'

# data = str(data,'ascii')
# print(data)
# binary_string = binascii.unhexlify(data)
# print(binary_string)

str_to_hex_bytes = binascii.hexlify(b'03/24/18 18:15:00')

str_hex = str_to_hex_bytes.decode('ascii') #str

print(int(str_hex, 16))
print(hex(int(str_hex, 16)))





