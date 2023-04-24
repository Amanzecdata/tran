# 1. length of string:
string1="flighttickets"
'''
length=len(string1)
print(length)
'''

#2. reverse words in a string:
'''
reverse=string1[::-1]
print(reverse)
'''

#3. remove spaces from string:
'''
string2="     this is a string with spaces               "
space=string2.strip()
print(string2,"\n",space)
'''
#4. Square of first 10 natural numbers:
'''
sum=0
for i in range(1,11):
    sum=sum+i
print(sum)
'''

#5. same as 4 using list compherehensions
'''
x=sum([i for i in range(1,11)])
print(x)
'''

#6. square of first 10 odd numbers:
'''
x=[i**2 for i in range(1,40) if i%2!=0]
print(x)
'''

#7. list numbers till 100, divisible by 5 and 3 using list comprehension
'''
x=[i for i in range(1,101) if i%3==0 and i%5==0]
print(x)
'''

#8. extract unique values from dictionary
'''
dict1 = {'A' : [1, 3, 5, 4],'B' : [4, 6, 8, 10],'C' : [6, 12, 4 ,8],'D' : [5, 7, 2]}
my_dict={'1':'one','2':'two','3':'three','4':'four','5':'five','2':'two','4':'four'}
x=[v for v in my_dict.values() if v not in [my_dict.values()]]

#  
res1 = list(sorted({ele for val in dict1.values() for ele in val}))

#
z=[ele for i in dict1.values() for ele in i]
res2=list(set(z))
print(res1, res2)
'''

#9. change value to key to value in a dictionary
'''
my_dict={'1':'one','2':'two','3':'three','4':'four','5':'five','2':'two','4':'four'}
new_dict={v:k for k,v in my_dict.items()}
print(new_dict)
'''

#10. generate a dictionary from the given string: “k1, v1, k2, v2, k3, v3, k4, v4…..”, 
# output dict will be: {“k1”: v1, “k2”: v2, “k3”: v3, , , , }
'''
string1="k1,v1,k2,v2,k3,v3,k4,v4,k5,v5"
new_string=string1.split(",")
k,v=[],[]
for i in range(len(new_string)):
    if i%2==0:
        k.append(new_string[i])
    else:
        v.append(new_string[i])
ans = dict(zip(k,v))
print(ans)
'''
# another way
'''
def gen_dict(st):
    l = st.split(",")
    out = {}
    for i in range(0,len(l)-1,2):
        out[l[i]] = l[i+1]
    return out
print(gen_dict("k1,v1,k2,v2,k3,v3,k4,v4,k5,v5")) 
'''

#11. solve frequency count problem, given string: “hello world!”, output dict: {“h”: 1, “e”:1, “l”: 3, “o”:2}
'''
string='hello world'
l={}
for i in string:
    l[i] = string.count(i)
print(l)
'''

#12.
"""" Module is a single python file with .py extension """
''' Package is a directory with in modules are existed and for colloborating with other 
    files there was __init__.py also present '''

#13. 
import datetime
from datetime import *
current_time=datetime.now()
print(current_time)

print(current_time.strftime("%d-%B-%Y--%l:%M:%S:%p"))

time_data = "25/05/99 02:35:5.523"
format_data = "%d/%m/%y %H:%M:%S.%f"
time=datetime.strptime(time_data,format_data)
print(time)
Today_date=date.today()
Today_time=time.today()
print(Today_date,Today_time)


first=date(2023,4,19)
last=date(2023,4,25)
diff=first-last
print(diff)

""""
EOD:
    1. Connect Mysql with my database.
    2. I have to refine the code for coverting the .Json file into CSV format. 
"""




