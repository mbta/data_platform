{
    "dfmVersion": "1.1",
    "taskInfo": {
        "name": "",
        "sourceEndpoint": "",
        "sourceEndpointType": "",
        "sourceEndpointUser": "",
        "replicationServer": "",
        "operation": ""
    },
    "fileInfo": {
        "name": "20211201-112233444.csv",
        "extension": "gz",
        "location": "incoming/cubic/ods_qlik/EDW.SAMPLE__ct",
        "startWriteTimestamp": "",
        "endWriteTimestamp": "",
        "firstTransactionTimestamp": null,
        "lastTransactionTimestamp": null,
        "content": "data",
        "recordCount": 2,
        "errorCount": 0
    },
    "formatInfo": {
        "format": "delimited",
        "options": {
            "fieldDelimiter": ",",
            "recordDelimiter": "\n",
            "nullValue": "",
            "quoteChar": "\"",
            "escapeChar": ""
        }
    },
    "dataInfo": {
        "sourceSchema": "EDW",
        "sourceTable": "SAMPLE",
        "targetSchema": "EDW",
        "targetTable": "SAMPLE__ct",
        "tableVersion": 1,
        "columns": [
            {
                "ordinal": 1,
                "name": "header__change_seq",
                "type": "STRING",
                "length": 35,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            },
            {
                "ordinal": 2,
                "name": "header__change_oper",
                "type": "STRING",
                "length": 1,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            },
            {
                "ordinal": 3,
                "name": "header__timestamp",
                "type": "DATETIME",
                "length": 0,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            },
            {
                "ordinal": 4,
                "name": "SAMPLE_ID",
                "type": "INT4",
                "length": 10,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 1
            },
            {
                "ordinal": 5,
                "name": "SAMPLE_NAME",
                "type": "STRING",
                "length": 100,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            },
            {
                "ordinal": 6,
                "name": "EDW_INSERTED_DTM",
                "type": "DATETIME",
                "length": 0,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            },
            {
                "ordinal": 7,
                "name": "EDW_UPDATED_DTM",
                "type": "DATETIME",
                "length": 0,
                "precision": 0,
                "scale": 0,
                "primaryKeyPos": 0
            }
        ]
    }
}
