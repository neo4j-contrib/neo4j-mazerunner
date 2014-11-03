MATCH (a1:Person)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)
CREATE (a1)-[:KNOWS]->(coActors);
