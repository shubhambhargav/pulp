**Readers**
============

## **LogReader** 

**_Sample Input_**

```
Temp line text: {\"nest_key_1\": {\"key_1\": \"val_1\", \"key_2\": \"val_2\"}, \"nest_key_2\": {\"key_3\": \"val_3\"}, \"key_4\": \"val_4\", \"key_5\": \"val_5\"}
```

**_Sample Output_**

```
{
    "nest_key_1": {
                    "key_1": "val_1",
                    "key_2": "val_2"
                },
    "nest_key_2": {
                    "key_3": "val_3"
                },
    "key_4": "val_4",
    "key_5": "val_5"
}
```

**Steps:**

1. Reads the line from given .log file
2. Extracts the json from the file
3. Sends the data to Model class for further processing


## **SqlReader**

**_Sample Input_**


key_1|key_2|key_3|key_4|key_5
-----|----|------|-----|-----
val_1|val_2|val_3|val_4|val_5


**_Sample Output_**

```
{
    "key_1": "val_1",
    "key_2": "val_2",
    "key_3": "val_3",
    "key_4": "val_4",
    "key_5": "val_5"
}
```

**Steps:**

1. Connects with the table for the given config
2. Extracts each row as a corresponding dict with column name as key and row as values.


**Writers**
==========


## **SqlWriter**

**_Sample Input_**

```
{
    "key_1": "val_1",
    "key_2": "val_2",
    "key_3": "val_3",
    "key_4": "val_4",
    "key_5": "val_5"
}
```

tablename: tb_sample

**_Sample Output_**

tb_sample

|key_1|key_2|key_3|key_4|key_5|
|-----|-----|-----|-----|-----|
|val_1|val_2|val_3|val_4|val_5|

**Steps**

1. Reads the given input as processed data
2. Inserts the data into the given table


**Fields**
==========

## **Base Field**

**Field** (_No Validation_)

src : source where to read from

des : destination where to write to

required : True/False

default : default value

**Currently supported fields with validation checks:**

* IntField
* FloatField
* CharField
* BooleanField
* UrlField
* IpField (_specifically for IPV4_)
* DateTimeField
* DateField
* ListField
* ChoiceField
* EpochField
* JsonField

**Currently supported Embedded fields:**

* EmbeddedJsonField

**Models**
==========

**Arguments:**

* reader
* writer
* mailer (optional)

**Features:**

* Batch Insertion
* Error Mailing

**Workflow:**

1. Read data using reader
2. Validate reader"s input
3. Process data
4. Validate before writing
5. Write date using writer

# **Types**

## **Model**

Default batch ingestor with an intermediate processing layer.

## **AggModel**

Aggregation ingestor which aggregates all `is_agg` values based on unique `is_key` values.