import Queue

class PriorityContainer(Queue.PriorityQueue):
    """A priority queue that supports inspection of its contents and retrieves the highest-valued
    entry first. Not thread-safe."""

    def __init__(self, *args, **kwargs):
        Queue.PriorityQueue.__init__(self, *args, **kwargs)
        self._contents = {}
        self.priority_coefficient = -1  # retrieve the highest-priority entry first

    def _put(self, item, *args):
        priority, key = item
        self._contents[key] = priority
        Queue.PriorityQueue._put(self, (self.priority_coefficient * priority, key), *args)
    
    def _get(self, *args):
        priority, key = Queue.PriorityQueue._get(self, *args)
        del self._contents[key]
        return (self.priority_coefficient * priority, key, )

    def __contains__(self, key):
        return key in self._contents
