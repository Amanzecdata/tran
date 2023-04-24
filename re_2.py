import re

text="aeroplane under the 0a0 table plane plan plant"
'''
x=re.findall("[a-z]|[A-Z]|[0-9]",text)
print(x)
'''
# have zero or more occurences
x=re.findall("a.*b",text)
print(x)


# have one or more occurences
text="aeroplane under z_score the 0a0 table plane abbbbplan plant abc absolute absent"
x=re.findall("a+b",text)
print(x)

x=re.findall("a.?b",text)
print(x)

x=re.findall('a.{3}b',text)
print(x)

x=re.findall('a.{3}b|a.{2}b',text)
print(x)

x=re.findall('[a-z]+_',text)
print(x)

