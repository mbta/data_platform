{
    "dfmVersion":   "1.1",
    "taskInfo": {
        "name": "",
        "sourceEndpoint":   "",
        "sourceEndpointType":   "",
        "sourceEndpointUser":   "",
        "replicationServer":    "",
        "operation":    ""
    },
    "fileInfo": {
        "name": "LOAD2.csv",
        "extension":    "gz",
        "location": "incoming/cubic/ods_qlik/EDW.SAMPLE",
        "startWriteTimestamp":  "",
        "endWriteTimestamp":    "",
        "firstTransactionTimestamp":    null,
        "lastTransactionTimestamp": null,
        "content":  "data",
        "recordCount":  1,
        "errorCount":   0
    },
    "formatInfo":   {
        "format":   "delimited",
        "options":  {
            "fieldDelimiter":   ",",
            "recordDelimiter":  "\n",
            "nullValue":    "",
            "quoteChar":    "\"",
            "escapeChar":   ""
        }
    },
    "dataInfo": {
        "sourceSchema": "EDW",
        "sourceTable":  "SAMPLE",
        "targetSchema": "EDW",
        "targetTable":  "SAMPLE",
        "tableVersion": 1,
        "columns":  [{
                "ordinal":  1,
                "name": "SAMPLE_ID",
                "type": "INT4",
                "length":   10,
                "precision":    0,
                "scale":    0,
                "primaryKeyPos":    0
            }, {
                "ordinal":  2,
                "name": "SAMPLE_NAME",
                "type": "STRING",
                "length":   100,
                "precision":    0,
                "scale":    0,
                "primaryKeyPos":    0
            }, {
                "ordinal":  3,
                "name": "EDW_INSERTED_DTM",
                "type": "DATETIME",
                "length":   0,
                "precision":    0,
                "scale":    0,
                "primaryKeyPos":    0
            }, {
                "ordinal":  4,
                "name": "EDW_UPDATED_DTM",
                "type": "DATETIME",
                "length":   0,
                "precision":    0,
                "scale":    0,
                "primaryKeyPos":    0
            }]
    }
}
