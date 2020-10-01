def wc_mapper_parallel(collection):
    output=[]
    for i in range(len(collection)):
        document = {}
        for word in collection[i]:
            x = document.get(word,0)
            if x == 0:
                document[word]=(i, document.get(word,0)+1)
            else:
                 document[word]=(i,document.get(word)[1]+1)
        output.append(document)
    return output



 def wc_reducer_parrallel(mapped_items):
    collector={}
    
    for item in mapped_items:
        for (key, val) in item.items():
            collector[key] = []            
    for item in mapped_items:
        for (key, val) in item.items():
            for (k1, v1) in collector.items():
                if key == k1:
                    collector[k1].append(val)
    return collector



def wc_reducer_parallel_(item):
     output=[]
     (word,(doc,counts))=item
     output.append((word,(doc,sum(counts))))
     return output
