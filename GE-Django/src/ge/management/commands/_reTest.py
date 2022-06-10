from os import remove


words = ["ure7i","cluster_id","cluster","545455","RS:4343"]

for w in words:
    if w.isdigit():
        words.remove(w)


print(words)