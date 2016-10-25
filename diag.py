import re

if __name__ == "__main__":
    test_val = 'this is a gram\t1991\t45'
    output = re.split(r'\t+', test_val)
    for el in output:
        print(el)
