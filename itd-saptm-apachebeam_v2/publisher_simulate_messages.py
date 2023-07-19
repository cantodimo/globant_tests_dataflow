from kafka import KafkaConsumer, KafkaProducer, TopicPartition  
from datetime import datetime
import json

base_message= { 
    "magic": "atMSG",
    "type": "DT",
    "headers": None,
    "messageSchemaId": None,
    "messageSchema": None,
    "message": { 
        "data": { 
            "item_no": "AAL614",
            "mfg_div_cd": "C",
            "phys_loc_cd": "M",
            "plnr_cd": "H",
            "inv_class_cd": "D",
            "use_um_cd": "EA",
            "byr_cd": "H",
            "item_desc": "RIVET 3/16 POPR FENDER",
            "itm_aut_ord_fct": "1",
            "itm_ord_mod_cd": "",
            "itm_whs_ctl_cd": "",
            "itm_aut_ord_cd": "N",
            "gnr_pt_no_ind": "",
            "div_adr_no": "",
            "ems_rel_cd": "U",
            "load_date_time": "2023-03-21 14:34:21.000000"
        },
        "beforeData": {
            "item_no": "AAL614",
            "mfg_div_cd": "C",
            "phys_loc_cd": "M",
            "plnr_cd": "H",
            "inv_class_cd": "D",
            "use_um_cd": "EA",
            "byr_cd": "H",
            "item_desc": "KAFKA UPD FOR TESTING",
            "itm_aut_ord_fct": "1",
            "itm_ord_mod_cd": "",
            "itm_whs_ctl_cd": "",
            "itm_aut_ord_cd": "N",
            "gnr_pt_no_ind": "",
            "div_adr_no": "",
            "ems_rel_cd": "U",
            "load_date_time": None
        }, 
        "headers": { 
            "operation": "UPDATE",
            "changeSequence": "20230321213416580000000000000000017",
            "timestamp": "2023-03-21T21:34:19.511",
            "streamPosition": "1015;638150312565803470;20221211140008252023B|0000000003A90202DB790001",
            "transactionId": "0000000066DA0202A903000000000000",
            "changeMask": "008080",
            "columnMask": "00FFFF",
            "transactionEventCounter": 1,
            "transactionLastEvent": True
        }
    }
}

topics= {
    "test-kafka-resume-job-3-partitions_tp2": ["item_no", "mfg_div_cd"],
    "test-kafka-resume-job-3-partitions_tp3": ["phys_loc_cd", "plnr_cd"],
}

#topic= "test-kafka-resume-job-multiple-partitions" #10 partitions
client = ["35.193.114.205:9092"]

p= KafkaProducer(bootstrap_servers=client, api_version=(0,11,5))
for tp in list(topics.keys()):
    for i in range(0,50,1):
        m= dict(base_message)
        m["message"]["headers"]["transactionEventCounter"]= i
        m["message"]["headers"]["timestamp"]= datetime.now().strftime("%Y-%m-%D %H-%M-%S")
        if i%3==0:
            ## editing data to create updated mensajes
            for columna in topics[tp]:
                m["message"]["data"][columna]= m["message"]["data"][columna] + m["message"]["data"][columna]  
    
        p.send(tp , value= json.dumps(m).encode("utf-8"))
        print(i)
        
        if i%20 ==0:
            p.flush()
            
p.flush()