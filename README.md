# Bitserv data analytics Platform

Technologies involved:

* Java
* RabbitMQ
* BigQuery

Data format:

1. Target - Can be dataset or table.
  
  target:
    
    a. dataset
    b. table

2. Action - Action depends on target.
  
  a. dataset
    
    action:
      1. create
      2. update
      3. delete
  
  b. table:
    
    action:
      1. create
      2. update
      3. insert
      4. delete

3. data - Data must support given action on the target.

Sample data format for above action:

CREATE DATASET:

```python
{
    'target': 'dataset',
    'action': 'create',
    'data': {
        'schema': {
            'name': 'testDataset'
        }
    }
}
```


UPDATE DATASET:

```python
{
    'target': 'dataset',
    'action': 'update',
    'data': {
        'schema': {
            'name': 'testDataset',
            'newFriendlyName': 'friendlyTestDataset'
        }
    }
}
```

DELETE DATASET:

```python
{
   'target': 'dataset',
   'action': 'delete',
   'data': {
        'schema': {
            'name': 'testDataset'
        }
    }
}
```

CREATE TABLE:

```python
{
    'target': 'table',
    'action': 'create',
    'data': {
        'schema': {
            'name': 'testTable',
            'dataset': 'testDataset',
            'fields': [
                {
                    'name': 'expired',
                    'type': 'boolean',
                    'mode': 'required',
                    'description': 'true if quote is expired'
                },
                {
                    'name': 'screenshot',
                    'type': 'bytes',
                    'mode': 'nullable',
                    'description': 'raw screenshot image'
                },
                {
                    'name': 'registrationDate',
                    'type': 'date',
                    'mode': 'nullable',
                    'description': 'Vehicle registration date'
                },
                {
                    'name': 'createdOn',
                    'type': 'datetime',
                    'mode': 'nullable',
                    'description': 'quote created datetime'
                },
                {
                    'name': 'id',
                    'type': 'integer',
                    'mode': 'required'
                },
                {
                    'name': 'response',
                    'type': 'record',
                    'fields': [
                        {
                            'name': 'finalPremium',
                            'type': 'float'
                        },
                        {
                            'name': 'idv',
                            'type': 'integer',
                            'mode': 'repeated'
                        }
                    ]
                },
                {
                    'name': 'insurerQuotes',
                    'type': 'record',
                    'mode': 'repeated',
                    'description': 'Individual insurer quotes',
                    'fields': [
                        {
                            'name': 'premium',
                            'type': 'float',
                            'mode': 'nullable'
                        },
                        {
                            'name': 'insurer',
                            'type': 'string',
                            'mode': 'required',
                            'description': 'Insurer name'
                        },
                        {
                            'name': 'cost',
                            'type': 'time',
                            'mode': 'nullable'
                        },
                        {
                            'name': 'createdOn',
                            'type': 'timestamp',
                            'mode': 'nullable'
                        }
                    ]
                }
            ]
        }
    }
}
```

UPDATE TABLE:

```python
{
    'target': 'table',
    'action': 'update',
    'data': {
        'schema': {
            'dataset': 'testDataset',
            'table': 'testTable',
            'newFriendlyName': 'friendlyTestTable'
        }
    }
}
```

DELETE TABLE:

```python
{
   'target': 'table',
   'action': 'delete',
   'data': {
        'schema': {
            'dataset': 'testDataset',
            'name': 'testTable'
        }
    }
}
```

INSERT INTO TABLE:

```python
{
    'target': 'table',
    'action': 'insert',
    'data': {
        'schema': {
            'name': 'testTable',
            'dataset': 'testDataset',
            'rows': [
                {
                    'insertId': '123456',
                    'json': {
                        'expired': 'true',
                        'screenshot': 'Cg0NDg0',
                        'registrationDate': '2017-5-16',
                        'createdOn': '2017-05-16 21:52:21.12',
                        'id': '123456',
                        'response': {
                            'finalPremium': '23769',
                            'idv': [
                                '855000',
                                '885000',
                                '915000'
                            ]
                        },
                        'insurerQuotes': [
                            {
                                'premium': '23768.55',
                                'insurer': 'oriental',
                                'cost': '0:0:3.123',
                                'createdOn': '1494929154'
                            },
                            {
                                'premium': '21768.55',
                                'insurer': 'kotak',
                                'cost': '0:0:1.123',
                                'createdOn': '1494929155'
                            }
                        ]
                    }
                },
                {
                    'insertId': '123457',
                    'json': {
                        'expired': 'false',
                        'screenshot': 'Cg0NDg0',
                        'registrationDate': '2017-5-15',
                        'createdOn': '2017-06-15 21:52:21.12',
                        'id': '123457',
                        'response': {
                            'finalPremium': '23769',
                            'idv': [
                                '855000',
                                '885000',
                                '915000'
                            ]
                        },
                        'insurerQuotes': [
                            {
                                'premium': '24768.55',
                                'insurer': 'oriental',
                                'cost': '0:0:3.123',
                                'createdOn': '1494929154'
                            }
                        ]
                    }
                }
            ]
        }
    }
}
```
