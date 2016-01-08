import collections
import itertools

class PriorityContainer(Queue.PriorityQueue):
    """A priority queue that supports inspection of its contents.

    Not thread-safe."""

    def __init__(*args, **kwargs):
        super(PriorityContainer, self).__init__(*args, **kwargs)
        self._contents = {}

    def _put(self, priority, key, *args):
        item = (priority, key, )
        self._contents[key] = priority
        super(PriorityContainer, self)._put(item, *args)
    
    def _get(self, *args):
        priority, key = super(PriorityContainer, self)._get(*args)
        del self._contents[key]
        return (priority, key, )

    def __contains__(self, key):
        return key in self._contents

def plan(join_list, where_conditions):
    graph = build_graph(join_list)
    priority = prioritize_nodes(graph, where_conditions)
    node_order = traverse(graph, priority)

def build_graph(join_list):

    graph = {}
    for idx, join_clause in enumerate(join_list):

        neighbors = None
        if join_clause.get('join_conditions', False):
            edges = [
                (c['left_operand'].qualifiers[0], c['right_operand'].qualifiers[0])
                for c in join_clause['join_conditions']
            ]

            neighbors = frozenset([
                n for n in itertools.chain(*edges)
                if n not in (join_clause['relation']['alias'], join_clause['relation']['path'], )
            ])

        graph[join_clause['relation']['alias']] = {
            'idx': idx,
            'neighbors': set([neighbors]) if neighbors is not None else set([])
        }

    add_reverse_edges_to_graph(graph)
    return graph

def add_reverse_edges_to_graph(graph):
    for alias in graph.keys():
        for neighbor_set in graph[alias]['neighbors']:
            for neighbor_alias in neighbor_set:
                graph[neighbor_alias]['neighbors'].add(frozenset([alias]))


def prioritize_nodes(graph, where_conditions):
    """Add high priority value to nodes mentioned in where conditions.
    
    Priority describes our desire to execute a particular node earlier rather than later. For
    exmaple, where-conditions should be applied as soon as possible and joins with small tables
    should be applied before joins with large tables.
    """
    priority = {}
    for node in graph.keys():
        priority[node] = 1
    return priority

def traverse(graph, priority):
    """Return a sequence of all the nodes in the graph that greedily chooses high 'priority' nodes
    before low 'priority' nodes.
    
    A node cannot be reached unless all nodes in at least one of its neighbor-sets is visited. 
    """

    reachable = PriorityContainer()
    visited = {}

    # start by greedily choosing the highest-priority node
    current_node = max(priority.items(), key=lambda i: i[1])[0]
    visited_count = 0

    while current_node:

        # visit node
        visited[current_node] = visited_count
        visited_count += 1

        # update visit-able nodes
        for neighbor_set in current_node['neighbors']:
            for neighbor in neighbor_set:
                if neighbor not in reachable and neighbor not in visited:

                    if is_reachable(graph, neighbor):
                        reachable.put(priority[neighbor], neighbor)

        try:
            current_node, current_priority = reachable.get()
        except IndexError:
            current_node = current_priority = None

    return visited

def is_reachable(node, graph, visited):
    """A node is reachable iff all nodes in at least one of its neighbor sets are visited."""
    return any([
        all([n in visited for n in neighbor_set])
        for neighbor_set in graph[node]['neighbors']
    ])
