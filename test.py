from event_correlation_service import correlator

event_a = {
    'id': 'ec2d7057-a2e6-4cf6-8089-988cf87e0ad3',
    'time': '2023-12-20T02:31:11.637228',
    'pid': 123,
    'ppid': 456,
}

event_b = {
    'id': '70c925c3-6bb3-4e98-a890-b4345c3bb54d',
    'time': '2023-12-20T02:31:15.644415',
    'pid': 123,
    'parent': {
        'pid': 456,   
    },
}

event_c = {
    'id': '0e46e1ef-8ed5-41ae-94fb-80b716498413',
    'time': '2023-12-20T02:31:30.644415',
    'pid': 123,
    'parent': 456,
}

field_aliases = {
    'time': {'timestamp'},
    'pid': {'process_id'},
    'ppid': {'parent_process_id', 'parent', 'parent.pid'},
}

matches = correlator.iter_matching_events(
    stream_a=[event_a], 
    stream_b=[event_b, event_c], 
    time_keys=['time'], 
    event_ttl=5,
    join_on=['pid', 'ppid'], 
    field_aliases=field_aliases,
)
for (a, b) in matches:
    print(a, b)
