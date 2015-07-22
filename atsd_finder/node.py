from graphite.node import BranchNode, LeafNode

class AtsdBranchNode(BranchNode):

    __slots__ = ('label',)

    def __init__(self, path, label):
    
        super(AtsdBranchNode, self).__init__(path)
        self.label = label
        self.local = False


class AtsdLeafNode(LeafNode):

    __slots__ = ('label',)

    def __init__(self, path, label, reader):
    
        super(AtsdLeafNode, self).__init__(path, reader)
        self.label = label
        self.local = False