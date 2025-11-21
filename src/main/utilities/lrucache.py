class DLLNode:
    def __init__(self, key:int, val:int):
        self.key = key
        self.val = val
        self.left = None
        self.right = None

class LRUCache:

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = {}
        self.head = DLLNode(-1,-1)
        self.tail = DLLNode(-1,-1)
        self.head.right = self.tail
        self.tail.left = self.head

    def addNode(self,node):
        existing_head = self.head.right
        self.head.right = node
        existing_head.left = node
        node.right = existing_head
        node.left = self.head

    def delNode(self,node):
        node.left.right = node.right
        node.right.left = node.left
        

    def get(self, key: int) -> int:
        if key in self.cache:
            node = self.cache[key]
            self.delNode(node)
            self.addNode(node)
            self.cache[key] = node
            return node.val
        return -1
        
    def put(self, key: int, value: int) -> None:
        if key in self.cache:
            old_node = self.cache[key]
            self.delNode(old_node)
            del self.cache[key]

        new_node = DLLNode(key,value)
        self.addNode(new_node)
        self.cache[key] = new_node

        if len(self.cache) > self.capacity:
            node_to_be_removed = self.tail.left
            self.delNode(node_to_be_removed)
            del self.cache[node_to_be_removed.key]
        
if __name__ == "__main__":
    obj = LRUCache(5)
    print(obj.cache)
    obj.put(1,10)
    print(obj.cache)
    print(obj.get(1))
    obj.put(2,20)
    obj.put(3,30)
    print(obj.cache)
    print(obj.get(2))
    print(obj.get(1))