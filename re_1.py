import regex as re

text="aeroplane under the 00 table plane plan plant"

# find all occurances
x=re.findall('[a-c]',text)
print(x)

#special character or contains a digit
x=re.findall('\d',text)
print(x)

#does not contains a digit
x=re.findall('\D',text)
print(x)

# find word ( . represents number of characters in that particular word)
x=re.findall("aer.....e",text)
print(x)

# finds any character or word in string if string startswith the given word or the character
x=re.findall("^a",text)
y=re.findall("^aeroplane",text)
print(x,y)


# finds any character or word in string if string endswith the given word or the character
x=re.findall("t$",text)
print(x)

# find word that start with p have any number of characters having e
x=re.findall("p.*e",text)
print("s-e",x)

# find word having p then any numbver of character and a t
x=re.findall("p.+t",text)
print(x)

# ? used when there is only 0 or 1 character between them
x=re.findall("pla.?t",text)
print(x)

# find the exact word having 3 characters missing 
x=re.findall("p.{3}t",text)
print(x)

# find plant or plane
x=re.findall("plant|plane",text)
print(x)

# if string startswith
x=re.findall("\Aaero",text)
print(x)

# find string with given word but not at the beginnning of string
x=re.findall(r"\Blane",text)
print(x)

# find string with given word but not at the end of the string
x=re.findall(r"an\B",text)
print(x)

#Returns a match where the specified characters are at the end of a word
x=re.findall(r"ant\b",text)
print(x)

#Returns a match where the specified characters are at the end of a word
x=re.findall(r"\bplant",text)
print(x)

# contain whitespaces
x=re.findall('\s',text)
print(x)

# does not contain whitespaces
x=re.findall('\S',text)
print(x)

# contain whitespaces
x=re.findall('\w',text)
print(x)

# does not contain whitespaces
x=re.findall('\W',text)
print(x)

# search the word 
y=re.search('pl',text)
print(y)

# split at the match in full string
z=re.split('pl',text)
print(z)

# sub means replaces the word with the match
z=re.sub('pl','00',text)
print(z)



 