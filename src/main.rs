#[macro_use] extern crate log;
extern crate lapin_futures as lapin;
extern crate futures;
extern crate tokio;
extern crate config;
extern crate rusted_cypher;

extern crate rustc_serialize;
use rustc_serialize::json::Json;

use futures::future::Future;
use futures::Stream;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use lapin::client::ConnectionOptions;
use lapin::channel::{BasicConsumeOptions,BasicPublishOptions,QueueDeclareOptions};
use lapin::types::FieldTable;

use std::collections::HashMap;

use rusted_cypher::{GraphClient, Statement};
use rusted_cypher::cypher::result::Row;

/*
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
*/

fn main() {
    
    let mut settings = config::Config::default();
    settings
        // Add in `./Settings.toml`
        .merge(config::File::with_name("Settings")).unwrap()
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        .merge(config::Environment::with_prefix("APP")).unwrap();
    
    //println!("{:?}", settings.deserialize::<HashMap<String, String>>().unwrap());
            

    let neo4jAddr = &settings.get_str("neo4j_dns").unwrap();
    let graph = GraphClient::connect(neo4jAddr).unwrap();
    
            
    let preAddr = &settings.get_str("rabbit_dns").unwrap();
    
  let addr = preAddr.parse().unwrap();

  Runtime::new().unwrap().block_on(
    TcpStream::connect(&addr).and_then(|stream| {
      // connect() returns a future of an AMQP Client
      // that resolves once the handshake is done
      lapin::client::Client::connect(stream, ConnectionOptions::default())
   }).and_then(|(client, heartbeat)| {
     // The heartbeat future should be run in a dedicated thread so that nothing can prevent it from
     // dispatching events on time.
     // If we ran it as part of the "main" chain of futures, we might end up not sending
     // some heartbeats if we don't poll often enough (because of some blocking task or such).
     tokio::spawn(heartbeat.map_err(|_| ()));

      // create_channel returns a future that is resolved
      // once the channel is successfully created
      client.create_channel()
    }).and_then(|channel| {
      let id = channel.id;
      info!("created channel with id: {}", id);

      let ch = channel.clone();
      channel.queue_declare(&settings.get_str("rabbit_queue").unwrap(), QueueDeclareOptions::default(), FieldTable::new()).and_then(move |queue| {
        info!("channel {} declared queue {}", id, &settings.get_str("rabbit_queue").unwrap());

        // basic_consume returns a future of a message
        // stream. Any time a message arrives for this consumer,
        // the for_each method would be called
        channel.basic_consume(&queue, "my_consumer", BasicConsumeOptions::default(), FieldTable::new())
      }).and_then(|stream| {
        info!("got consumer stream");

        stream.for_each(move |message| {
          debug!("got message: {:?}", message);
          info!("decoded message: {:?}", std::str::from_utf8(&message.data).unwrap());

          // println!("got message: {:?}", std::str::from_utf8(&message.data).unwrap());
        
        let jIn = Json::from_str(std::str::from_utf8(&message.data).unwrap());
        
        match jIn {
            Err(e) => {
                // Print `e` itself, not `e.description()`.
                println!("Error: {}", e);
            }
            _ => {
                
                let me = jIn.unwrap();
                
                
                //create all nodes before create relationships
                for (key, value) in me.as_object().unwrap().iter() {
                    if (key == "nodes") {
                        //println!("node Item: {}=>{}", key, value);
                        if (value.is_array()) {
                            //println!("row Item: {}=>{}", key, value);
                            for n in 0..value.as_array().unwrap().len() {
                                println!("row {}=> {}", n, value.as_array().unwrap()[n]);
                                
                                let mut mQ: String = "MERGE (n:FAuto".to_owned();
                                //mQ.push_str(value.as_array().unwrap()[n]["label"].as_string().unwrap());
                                mQ.push_str(" {n_id: '");
                                mQ.push_str(value.as_array().unwrap()[n]["id"].as_string().unwrap());
                                mQ.push_str("'}) ");
                                
                                if ((value.as_array().unwrap()[n]["properties"].is_array()) && (value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() > 0)) {
                                    //println!("properties num {}", value.as_array().unwrap()[n]["properties"].as_array().unwrap().len());
                                    mQ.push_str("SET ");
                                    
                                    for m in 0..value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() {
                                        mQ.push_str("n.");
                                        mQ.push_str(value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["name"].as_string().unwrap());
                                        mQ.push_str(" = ");
                                        
                                        if (value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["mtype"].as_string().unwrap() == "string") {
                                            mQ.push_str("'");
                                        }
                                        
                                        mQ.push_str(value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["value"].as_string().unwrap());
                                        
                                        if (value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["mtype"].as_string().unwrap() == "string") {
                                            mQ.push_str("'");
                                        }
                                        if (m < (value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() - 1)) {
                                            mQ.push_str(",\n");
                                        }
                                        
                                    }
                                    
                                }
                                println!("query: {}", mQ);
                                
                                graph.exec(mQ).unwrap();
                                
                            }
                        }
                    }
                    
                }
                
                //create relationships 
                for (key, value) in me.as_object().unwrap().iter() {
                    if (key == "relationships") {
                        if (value.is_array()) {
                            for n in 0..value.as_array().unwrap().len() {
                                let mut mQ: String = "MATCH (a:FAuto {n_id: '".to_owned();
                                mQ.push_str(value.as_array().unwrap()[n]["from"].as_string().unwrap());
                                mQ.push_str("'}),(b:FAuto {n_id: '");
                                mQ.push_str(value.as_array().unwrap()[n]["to"].as_string().unwrap());
                                mQ.push_str("'}) \nMERGE (a)");
                                
                                if (value.as_array().unwrap()[n]["twoway"].as_boolean().unwrap() == true) {
                                    mQ.push_str("<");
                                }
                                mQ.push_str("-[r:");
                                mQ.push_str(value.as_array().unwrap()[n]["mtype"].as_string().unwrap());
                                mQ.push_str(" ]->(b) \n");
                                if ((value.as_array().unwrap()[n]["properties"].is_array()) && (value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() > 0)) {
                                    mQ.push_str("SET ");
                                    for m in 0..value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() {
                                        mQ.push_str("r.");
                                        mQ.push_str(value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["name"].as_string().unwrap());
                                        mQ.push_str(" = ");
                                        
                                        if (value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["mtype"].as_string().unwrap() == "string") {
                                            mQ.push_str("'");
                                        }
                                        
                                        mQ.push_str(value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["value"].as_string().unwrap());
                                        
                                        if (value.as_array().unwrap()[n]["properties"].as_array().unwrap()[m]["mtype"].as_string().unwrap() == "string") {
                                            mQ.push_str("'");
                                        }
                                        if (m < (value.as_array().unwrap()[n]["properties"].as_array().unwrap().len() - 1)) {
                                            mQ.push_str(",\n");
                                        }
                                        
                                        
                                    }
                                    
                                }
                                
                                mQ.push_str("\n RETURN b.n_id;");
                                
                                println!("query: {}", mQ);
                                graph.exec(mQ).unwrap();
                            }
                        }
                    }
                }
                
                
                
                //find path
                for (key, value) in me.as_object().unwrap().iter() {
                    if (key == "find") {
                        let mut mQ: String = "MATCH (ms:FAuto { n_id: '".to_owned();
                        println!("query1: {}", mQ);
                        mQ.push_str(value["begin"].as_string().unwrap());
                        println!("query2: {}", mQ);
                        mQ.push_str("' }),(cs:FAuto { n_id: '");
                        println!("query3: {}", mQ);
                        mQ.push_str(value["end"].as_string().unwrap());
                        println!("query4: {}", mQ);
                        mQ.push_str("' }), p = shortestPath((ms)-[:");
                        println!("query5: {}", mQ);
                        mQ.push_str(value["relation_type"].as_string().unwrap());
                        println!("query6: {}", mQ);
                        mQ.push_str("*]-(cs)) \nWHERE ALL (r IN relationships(p)");
                        
                        if ((value["properties"].is_array()) && (value["properties"].as_array().unwrap().len() > 0)) {
                            mQ.push_str("WHERE ");
                            for m in 0..value["properties"].as_array().unwrap().len() {
                                mQ.push_str("r.");
                                mQ.push_str(value["properties"].as_array().unwrap()[m]["name"].as_string().unwrap());
                                mQ.push_str(" ");
                                mQ.push_str(value["properties"].as_array().unwrap()[m]["value"].as_string().unwrap());
                                
                                if (m < (value["properties"].as_array().unwrap().len() - 1)) {
                                    mQ.push_str(" AND ");
                                }
                            }
                        }
                        
                        mQ.push_str(") RETURN p");
                        
                        
                        println!("query: {}", mQ);
                        
                        let results = graph.exec(mQ).unwrap();
//                        let rows: Vec<Row> = results.rows().take(1).collect();
//                        let row = rows.first().unwrap();
                        println!("result count: {}", results.rows().count());
                    }
                    
                }
                
                
                
            }
        }
          //println!("got message: {:?}", jIn);

          ch.basic_ack(message.delivery_tag, false)
        })
      })
    })
  ).expect("runtime failure");
}

