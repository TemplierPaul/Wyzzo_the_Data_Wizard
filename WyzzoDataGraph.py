from sqlalchemy import create_engine
import pandas as pd
import json
import networkx as nx
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore",category=plt.cbook.mplDeprecation)

def debug(funct):
    def wrapper(*args, **kwargs):
        if True: print('\n', funct.__name__)
        r = funct(*args, **kwargs)

        return r

    return wrapper


class Graph:

    def __init__(self, graph_name=''):
        self.nodes = {}
        self.sql = {}
        if ' ' in graph_name:
            graph_name = graph_name.replace(' ', '_')
        self.graph_name = graph_name
        self.engine = None
        self.network = nx.DiGraph()
        self.root = Node('ROOT', self)

    def __str__(self):
        s = '\nDataGraph ' + self.graph_name
        s += '\nConnected: ' + str(not (self.engine is None))
        s += '\nData Nodes: ' + str([n for n in self.nodes.values()])
        s += '\nSQL Nodes: ' + str([n for n in self.sql.values()]) + '\n'
        return s

    def connect(self):
        url = 'mysql+mysqlconnector://root:@localhost'
        self.engine = create_engine(url)
        try:
            self.engine.execute('CREATE DATABASE ' + self.graph_name)
        except:
            self.engine.execute('DROP DATABASE ' + self.graph_name)
            self.engine.execute('CREATE DATABASE ' + self.graph_name)
            print('Dropped', self.graph_name)
        url = url + '/' + self.graph_name
        self.engine = create_engine(url)
        print('Connected to:', url, '\n')
        return self

    def addDataNode(self, name):
        name = name.replace(' ', '_')
        self.nodes[name] = DataNode(name=name, graph=self)
        # self.network.add_node(self.nodes[name])
        return self.nodes[name]

    def addSQLNode(self, name, input=None, output=None, output_name=None):
        name = name.replace(' ', '_')
        if output is None:
            if output_name is None:
                output = [self.addDataNode('out_' + name)]
            else:
                output = [self.addDataNode(output_name)]
        self.sql[name] = SQLNode(name=name, graph=self, input=input, output=output)
        # self.network.add_node(self.sql[name])
        return self.sql[name]

    def plotGraph(self, prog='dot'):
        self.network = nx.DiGraph()
        self.network.add_node(self.root)
        color_map = ['w']
        labels = {self.root: 'ROOT'}
        edges = []
        depths = [[] for _ in range(len(self.nodes) + len(self.sql) + 1)]
        depths[0].append(self.root)
        for n in self.nodes.values():
            self.network.add_node(n)
            color_map.append('lightcoral')
            labels[n] = n.name.replace('_', '_\n')
            depths[n.depth].append(n)
            if not (n.source is None) and 'csv' in n.source:
                edges += [(self.root, n)]
        for n in self.sql.values():
            self.network.add_node(n)
            color_map.append('paleturquoise')
            labels[n] = n.name.replace('_', '_\n')
            depths[n.depth].append(n)
            edges += n.getEdges()
        self.network.add_edges_from(edges)

        pos = nx.nx_pydot.graphviz_layout(self.network, prog=prog)
        for n, p in pos.items():
            d = len(depths[n.depth])
            if 'DataNode' in str(type(n)):
                x = 1
            elif 'SQLNode' in str(type(n)):
                x = 0
            else:
                x = 1
            x = x + depths[n.depth].index(n) * 0.25 - d * 0.125
            y = 2 * n.depth
            pos[n] = (x, y)
        print(pos)
        nx.draw(self.network, pos=pos, node_color=color_map, labels=labels, node_size=3000, font_size=12,
                font_color='k', arrowsize=20)#, node_shape='D')
        plt.show()
        return self

    def toNodes(self, l):
        if l is None:
            return None
        if 'str' in str(type(l)):
            l = [l]
        out_l = []
        for n in l:
            s = str(type(n))
            if 'str' in s:
                out_l.append(self.nodes[n])
            else:
                out_l.append(n)
        return out_l

    def saveConfig(self, path):
        d = {}
        d['Name'] = self.graph_name
        d['Data Nodes'] = [
            {'Name': n.name,
             'Source': n.source,
             'Depth' : n.depth}
            for n in self.nodes.values()]
        d['SQL Nodes'] = [
            {'Name': n.name,
             'Input': [i.name for i in n.input],
             'Output': [i.name for i in n.output],
             'SQL': n.sql,
             'Depth' : n.depth}
            for n in self.sql.values()]
        with open(path, 'w') as json_file:
            json.dump(d, json_file)
        return self

    def loadConfig(self, path):
        with open(path, 'r') as json_file:
            d = json.load(json_file)
        self.graph_name = d['Name']
        url = 'mysql+mysqlconnector://root:@localhost/' + self.graph_name
        self.engine = create_engine(url)
        for i in d['Data Nodes']:
            self.nodes[i['Name']] = DataNode(i['Name'], graph=self, create=False)
            self.nodes[i['Name']].source = i['Source']
            self.nodes[i['Name']].depth = i['Depth']
        for s in d['SQL Nodes']:
            self.sql[s['Name']] = SQLNode(name=s['Name'], graph=self, input=s['Input'], output=s['Output']).loadSQL(
                s['SQL'])
            self.sql[s['Name']].depth = s['Depth']
        return self

    def close(self):
        self.nodes = {}
        self.sql = {}
        self.engine.execute('DROP DATABASE ' + self.graph_name)
        print('\n----------------\nDATABASE DROPPED\n----------------\n')
        return self


class Node:

    def __init__(self, name, graph):
        self.name = name.replace(' ', '_')
        self.graph = graph
        self.depth = 0

    def __str__(self):
        return self.name

    def __repr__(self):
        t = str(type(self)).split("'")[1].split('.')[1]
        return t + ' ' + self.name


class DataNode(Node):

    def __init__(self, name, graph, create=True):
        if str.lower(name)!= name:
            name = str.lower(name)
            print("WARNING : No capital letters allowed - Name changed to " + name)
        super(DataNode, self).__init__(name, graph)
        self.source = None
        if create:
            try:
                graph.engine.execute('CREATE TABLE ' + self.name + ' (id INT AUTO_INCREMENT PRIMARY KEY)')
            except:
                graph.engine.execute('DROP TABLE ' + self.name)
                print("Dropped table", self.name)
                graph.engine.execute('CREATE TABLE ' + self.name + ' (id INT AUTO_INCREMENT PRIMARY KEY)')

    def __repr__(self):
        conn = self.graph.engine.connect()
        count = pd.read_sql(sql='SELECT COUNT(*) as c FROM ' + self.name, con=conn)
        conn.close()
        return super(DataNode, self).__repr__() + ' : ' + str(count.iloc[0]['c'])

    def __str__(self):
        return repr(self)

    def loadCSV(self, path):
        df = pd.read_csv(path, index_col=0)
        conn = self.graph.engine.connect()
        df.to_sql(name=self.name, con=self.graph.engine, if_exists='replace', index=False)
        conn.close()
        print('csv loaded:', path, '->', self)
        self.source = path
        self.depth = 1
        return self

    def delete(self):
        self.graph.engine.execute('DROP TABLE ' + self.name)
        self.graph.nodes.pop(self.name)


class SQLNode(Node):

    def __init__(self, name, graph, input=None, output=None):
        if str.lower(name)!= name:
            name = str.lower(name)
            print("WARNING : No capital letters allowed - Name changed to " + name)
        super(SQLNode, self).__init__(name, graph)
        self.sql = None
        self.input = self.graph.toNodes(input)
        self.output = self.graph.toNodes(output)
        for n in self.output:
            n.source = self.name
        for i in self.input:
            self.depth = max(i.depth + 1, self.depth)

    def setInput(self, *args):
        self.input = self.graph.toNodes(args)
        return self

    def setOutput(self, *args):
        self.output = self.graph.toNodes(args)
        for n in self.output:
            n.source = self.name
        return self

    def loadSQL(self, sql):
        if sql is None:
            return self
        for i in range(len(self.input)):
            sql = sql.replace('<INPUTDB_' + str(i) + '>', self.input[i].name)
        for i in range(len(self.output)):
            sql = sql.replace('<OUTPUTDB_' + str(i) + '>', self.output[i].name)
        self.sql = sql
        # print(self.sql)
        return self

    def run(self):
        if self.sql is None:
            raise Exception('No SQL')
        try:
            conn = self.graph.engine.connect()
            self.graph.engine.execute('Drop table ' + self.output[0].name)
            sql = 'create table ' + self.output[0].name + ' (' + self.sql + ')'
            self.graph.engine.execute(sql)
            conn.close()
        except:
            raise Warning('SQL run failed')
        print('SQL Executed:', self)
        for i in self.input:
            self.depth = max(i.depth + 1, self.depth)
        for i in self.output:
            i.depth = self.depth + 1
        return self

    def getEdges(self):
        l = []
        for i in self.input:
            l.append((i, self))
        for o in self.output:
            l.append((self, o))
        return l
