# findpaths
findpaths a microservice develop by rust, rabbitmq, neo4j. This small project is created in rust learning time (some days). It get input from rabbitmq, create nodes and relationships in neo4j and find a shortest path.

# Input sample

```json
{
	"nodes": [
		{"id": "id_1", "label": "MUser", "properties":[
                                    {"name": "fullname", "value": "name is id1", "mtype": "string"},
                                    {"name": "email", "value": "id1@em.com", "mtype": "string"},
                                    {"name": "age", "value": "17", "mtype": "number"}
                                    ]
        
        },
		{"id": "id_2", "label": "MUser", "properties":[
                                    {"name": "fullname", "value": "name is id2", "mtype": "string"},
                                    {"name": "email", "value": "id2@em.com", "mtype": "string"},
                                    {"name": "age", "value": "18", "mtype": "number"}
                                    ]
        
        },
		{"id": "id_3", "label": "MUser", "properties":[
                                    {"name": "fullname", "value": "name is id3", "mtype": "string"},
                                    {"name": "email", "value": "id3@em.com", "mtype": "string"},
                                    {"name": "age", "value": "20", "mtype": "number"}
                                    ]
        
        }
	],
	"relationships": [
		{"from": "id_1", "to": "id_2", "mtype": "lender", "twoway": false, "properties":[{"name": "value", "value": "20", "mtype": "number"}]},
		{"from": "id_1", "to": "id_3", "mtype": "lender", "twoway": false, "properties":[{"name": "value", "value": "40", "mtype": "number"}]},
		{"from": "id_3", "to": "id_2", "mtype": "service", "twoway": false, "properties":[{"name": "value", "value": "10", "mtype": "number"}]}
	],
    "find": {"relation_type": "lender", "begin": "id_1", "end": "id_2", "properties":[{"name": "value", "value": "> 0", "mtype": "number"}]}
}
```
I have not decided yet about output.

