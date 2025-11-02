# {
#     hr: 150,
#     fin: 100,
#     it: {
#         soft:50
#         hardw:100
#     },
#     mark:200
# }

def merge_and_aggregate(data:dict) -> dict:
    result = {}
    par_items = {}
    stack = [(key,val,None) for key,val in data.items()]
    
    while stack:
        key, value, par = stack.pop()
        
        if isinstance(value,dict):
            
            par_items[key] = 0
            
            for k,v in value.items():
                stack.append((k,v,key))
                
        else:
            result[key] = value
            
            if par:
                par_items[par] = par_items.get(par,0) + value
                
    result.update(par_items)
    
    result["total"] = sum(v for k,v in result.items() if k != "total")
    
    return result
if __name__ == "__main__":
    data = {
    "hr":100,
    "sales":{
        "inbound":{
            "gdc":25,
            "rdc":25
        },
        "outbound":{
            "ecomm":100
        },
    },
    "fin":300,
    "ebs":150
    }

    print(merge_and_aggregate(data))