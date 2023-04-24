'''
x = [2, 3, 56, 8]
y = [2, 4, 5, 6]
c = [3, 4, 56, 6]
z = map(lambda a, b, c: a*2*b*c, x, y, c)
print(list(z))
'''
'''
def balance(l):
    open=["[","{","("]
    close=["]","}",")"]
    stack=[]
    for i in l:
        if i in open:
            stack.append(i)
        elif i in close:
            p=stack.pop()
            if p in open:
                return True
            else:
                return(False)
        else:
            return(False)
        return True
    
'''

'''
def balance(s):
    stack = []
    for char in s:
        if char in ['(', '{', '[']:
            stack.append(char)
        else:
            if not stack:
                return False
            current_char = stack.pop()
            if current_char == '(' and char != ')':
                return False
            if current_char == '{' and char != '}':
                return False
            if current_char == '[' and char != ']':
                return False
    if stack:
        return False
    return True

l="[{}()]"
print(balance(l))   
'''


class dunder():
    def __init__(self,n):
        self.n=n
    def __str__(self):
        return("")
    
st="3"
obj=dunder(st)
print(obj)


















