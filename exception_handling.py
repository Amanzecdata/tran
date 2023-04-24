# 1. ZeroDivisionError
a,b,c=23,1,"string"
try:
    res=a/b
except ZeroDivisionError:
    print("Any number cannot be divisible by Zero")
else:
    print(res)
finally:
    print("To do any further operation on res")

#2. SyntaxError
try:
    eval('a===b')
except SyntaxError:
    print("Please have a look at your syntax")

#3. TypeError
try:
    print(a+c)
except TypeError:
    print("Improper")

#4. AssertionError 
x="anything"
print(id(x),x)
try:
    assert x=='change', "False"
except AssertionError as E:
    print("Assert keyword raises error")

#5. NameError
try:
    print(x+z)
except NameError as n:
    print(n)

#6 AttributeError
try:
    x.append(b)
except AttributeError as E:
    print(E)

#7. ModuleNotFoundError
try:
    import joke
except ModuleNotFoundError:
    print("There is no such module")

#8. ImportError
try:
    from array import sessions
except ImportError as A:
    print(A)

#9.  StopIteration
it=iter([1,2,3])
for i in it:
    try:
        print(next(it))
    except StopIteration:
        print("No next value")
