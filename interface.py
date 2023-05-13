from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.graph import Node
# from neo4j.work.simple import QueryResult

from queue import Queue

uri = "neo4j://localhost:7687"
username = "neo4j"
password = "project2phase2"

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()



    def bfs(self, start_node, last_node):
        
        query = """CALL gds.graph.project('YT_Graph','Location','TRIP',{relationshipProperties: 'distance'})"""
        with self._driver.session() as session:
            result = session.run(query)

        query = """MATCH (a:Location{name:%(start_node)s}), (d:Location{name:%(last_node)s})
            WITH id(a) AS source, [id(d)] AS targetNodes
            CALL gds.bfs.stream('YT_Graph', {
            sourceNode: source,
            targetNodes: targetNodes
            })
            YIELD path
            RETURN path
        """% {'start_node': start_node, 'last_node': last_node}

        records = None

        with self._driver.session() as session:
            result = session.run(query)
            records = result.data()
            with self._driver.session() as session:
                session.run("CALL gds.graph.drop('YT_Graph') YIELD graphName;")
            return records

    def pagerank(self, max_iterations, weight_property):


        query = """CALL gds.graph.project('YT_Graph','Location','TRIP',{relationshipProperties: 'distance'})"""

        with self._driver.session() as session:
            result = session.run(query)
            
        query = """
            CALL gds.pageRank.stream('YT_Graph', {
            maxIterations: %(max_iterations)s,
            dampingFactor: 0.85,
            relationshipWeightProperty: '%(weight_property)s'
            })
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).name AS name, score
            ORDER BY score DESC, name ASC
        """% {'max_iterations': max_iterations, 'weight_property': weight_property}

        records = None
        with self._driver.session() as session:
            result = session.run(query)
            records = result.data()

        if records:
            with self._driver.session() as session:
                session.run("CALL gds.graph.drop('YT_Graph') YIELD graphName;")
            return records[0], records[-1]

        with self._driver.session() as session:
            session.run("CALL gds.graph.drop('YT_Graph') YIELD graphName;")
        return None, None