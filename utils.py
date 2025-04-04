import re


def validate_pan(pan):
    if isinstance(pan, str):
        if re.match(r'^[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}$', pan):
            if len(pan.strip()) == 10:
                return pan.strip()
    else:
        return None


def validate_email(email):
    if  isinstance(email, str) and re.match(
            r'^(([^<>()[\]\.,;:\s@\"]+(\.[^<>()[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$',
            email):
        return email.strip().lower()
    else:
        return None

def validate_mobile(mobile):
    if  isinstance(mobile, str) and len(mobile) >6:
        return mobile.strip()[-10:]
    else:
        return None
