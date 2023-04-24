'''
l1=[12,45,6,7,8,9,0]
l2='visualstudio'
l2=[i for i in l2]
l2="".join(l2)
print(l2)

a = ("John", "Charles", "Mike")
b = ("Jenny", "Christy", "Monica", "Vicky")
x = zip(a, b)
new_dict1 = {k: v for k,v in x}
print(new_dict1)
new_dict2= set(x)
print(set(x))
for i,v in x:
    print(i,":",v)
'''

t1=(2,3,5,6,2,7,9,0,2)
x=t1.count(2)
y=t1.index(2)
print(x,y)














