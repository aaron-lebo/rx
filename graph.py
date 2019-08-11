import argparse
import sqlite3

import matplotlib.pyplot as plt
import networkx as nx

parser = argparse.ArgumentParser()
args = parser.parse_args()

con = sqlite3.connect('rx.db')
subms = dict((row[0], row[1]) for row in con.execute('select id, author from submissions where subreddit="worldnews"'))
comms = dict((row[0], row[1:]) for row in con.execute('select id, author, parent_id from comments where subreddit="worldnews"'))
con.close()

users = set()
edges = {}

g = nx.Graph()

for id, val in comms.items():
    g.add_edge(id, val[1])

def step(nodes, node, children, level, result):
    user = (subms.get(node) or comms[node])[0]
    users.add(user)
    result.append([node, level])

    result1 = []
    for child in children:
        step(nodes, child, nodes.get(child, []), level + 1, result1)
    for node1, level1 in result1:
        user1 = comms[node1][0]
        rel = (user, user1)
        if not edges.get(rel):
            edges[rel] = set()
        elif not edges.get((user1, user)):
            rel = (user1, user)
            edges[rel] = set()
        edges[rel].add((node1, float(level) / level1))
    result += result1

subm = 't3_1z9a47'
nodes = nx.dfs_successors(g, subm)
result = []
step(nodes, subm, nodes[subm], 1, result)
for u, u1 in [(u, u1) for u in users for u1 in users if u != u1]:
    if not edges.get((u, u1)) or edges.get((u1, u)):
        edges[(u, u1)] = (subm, 0.1)

user_g = nx.Graph()
for nodes, ties in edges.items():
    user_g.add_edge(nodes[0], nodes[1], ties=ties)
nx.draw(user_g, with_labels=True)
nx.readwrite.edgelist.write_edgelist(user_g, 'edgelist.txt')
#nx.draw(g.subgraph({subm} | nx.descendants(g, subm)), with_labels=True)
plt.show()
