ui_circuit = 'test'
ui_skip_kafka = False

# kafka_topic = 'smev-reg-realty-out'
kafka_topic = 'test_out_1'

srv = {
    'test': '10.10.4.28:9092',
    'prod': '10.10.5.218:9092'
}

db_connection_data = {
    'test': {
        'postgresql': {
            'rosreestr_checks': {
                'server': '10.10.4.172',
                'port': '5432',
                'username': 'postgres',
                'password': 'VYgJBx6Eun}W',
                'database': 'rosreestr_checks'
            }
        }
    },
    'prod': {
        'postgresql': {
            'rosreestr_checks': {
                'server': '',
                'port': '',
                'username': '',
                'password': '',
                'database': ''
            }
        }
    }
}
