import itertools
from sqltxt.column import ColumnName
from sqltxt.util import PriorityContainer, Queue

def plan(relations, join_conditions, where_conditions):
    """Given a list of relations and a list of conditions across those relations, return a list
    of relation indices in an optimized join order."""

    graph = build_graph(relations, join_conditions)
    priorities = prioritize_nodes(graph, where_conditions)
    node_order = traverse(graph, priorities)

    ordered_indices = [
        graph[node]['idx'] for node, ordinal in sorted(node_order.items(), key=lambda x: x[1])
    ]
    return ordered_indices

def build_graph(relations, join_conditions):
    """Given a list of relations and join conditions across those relations, return a graph of
    relations (nodes) connected by join conditions (edges)."""

    graph = {}
    for idx, relation in enumerate(relations):
        graph[relation.alias] = { 'idx': idx, 'neighbors': set([]) }

    edges = [
        (cond.left_operand.qualifiers[0], cond.right_operand.qualifiers[0], )
        for cond in join_conditions
    ]

    for left, right in edges:
        graph[left]['neighbors'].add(right)
        graph[right]['neighbors'].add(left)

    return graph

def prioritize_nodes(graph, where_conditions):
    """Assign high priority to nodes mentioned in where conditions."""
    priorities = {}
    for node in graph.keys():
        priorities[node] = 1
    return priorities

def traverse(graph, priorities):
    """Return a sequence of all the nodes in the graph by greedily choosing high 'priority' nodes
    before low 'priority' nodes."""

    reachable = PriorityContainer()
    visited = {}

    # start by greedily choosing the highest-priority node
    current_node = max(priorities.items(), key=lambda i: i[1])[0]
    visited_count = 0

    while current_node:

        # visit node
        visited[current_node] = visited_count
        visited_count += 1

        # update visit-able nodes
        for neighbor in graph[current_node]['neighbors']:
            if neighbor not in reachable and neighbor not in visited:
                reachable.put((priorities[neighbor], neighbor))

        try:
            current_priority, current_node = reachable.get(False)
        except Queue.Empty:
            current_priority = current_node = None

    return visited
