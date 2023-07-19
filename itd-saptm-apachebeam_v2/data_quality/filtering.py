import json

class filter_kakfa_messages :

    @staticmethod
    def filter_v1(message, columns_to_compare_this_topic):
        kafka_message= json.loads( message )
        new_data_dict= kafka_message["message"]["data"]
        old_data_dict= kafka_message["message"]["beforeData"]
        new_data= [ new_data_dict[x] for x in columns_to_compare_this_topic ]
        old_data= [ old_data_dict[x] for x in columns_to_compare_this_topic ]
        if new_data != old_data:
            return kafka_message
        else:
            return None
        