"""stack=[]
def balanced(string):
    for i in string:
        if i=="(" or i=="[" or i=="{":
            stack.append(i)
        elif i==")" or i=="}" or i=="]":
            last=stack[-1]
            if last=='(' and i==')' or last=="{" and i=="}" or last=="[" and i=="]":
                return True
            else:
                return False

string="[{()}]"          
bal_obj = balanced(string)
print(bal_obj)


def longest_common_prefix(strs):
    if not strs:
        return ""
    prefix = strs[0]
    for s in strs:
        print("s---: ",s,"prefix---: ",prefix)
        while s.startswith(prefix)==False:
            prefix = prefix[0:-1:]
            print("s: ",s,"prefix: ",prefix)
            if not prefix:
                return ""
    return prefix

strs = ["flower", "flow", "float"]
print(longest_common_prefix(strs))

"""


'''
git config --global user.name “Amanzecdata”

git config --global user.email “aman.s@zecdata.com”

Aman05@zecdata
token : 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
git push https://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@github.com/Amanzecdata/https://github.com/Amanzecdata/Training.git.git



-- This will set up the remote, it tells git where the repository is located
git remote add origin  https://github.com/Amanzecdata/Training.git

git remote set-url origin https://github.com/Amanzecdata/Training.git

git branch -M main

git push -u origin main

'''





