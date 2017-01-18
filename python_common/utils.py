def doesFileExist(s3, bucket_name, key):
    try:
        s3.do_head_object(key = key, bucket_name = bucket_name)
        return True
    except:
        return False

def doesFileMatch(io1, io2):
    while True:
        b1 = io1.read(4096)
        b2 = io2.read(4096)
        if b1 == "":
            return b2 == ""
        elif b2 == "":
            return b1 == ""
        elif b1 != b2:
            return False
