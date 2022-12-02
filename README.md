# APIIngestor

## Importing
```python
from APIIngestor import Ingestor
```

## Methods and Functions
This library was developed with the intention of facilitating the ingestion of endpoints, that is, it is necessary to create an object for each endpoint to be ingested. To use the library resources, you need to create an object from the Ingestor class:
```python
my_ingestor = Ingestor(
                api_name="my_api_name", 
                url="https://the.api.url/the/endpoint", 
                method="GET"
              )
```

When initializing the object, some parameters must be passed:

> **api_name**:str=None (Required)
>
> Enter the API name here along with the name of the endpoint to be ingested. This name is passed on to the name of the files saved on the landing during the process.

> **url**:str=None (Required)
>
> Enter the URL of the endpoint to be ingested here.

> **method**:str=None (Required)
>
> The method for endpoint requests. Methods available in the latest version: GET and POST.

## Authentication

By default, the Ingestor is started without any authentication and can be used that way, setting an authentication is optional.

There are a few types of authentication available through the ingestor, these are:

### Payload

```python
my_ingestor.set_auth(
              auth_type="payload", 
              token_payload_name="api_token", 
              token="abcdef1234567890"
            )
```

> **auth_type**:str=None (Required)
>
> The authentication type. Types available in the latest version: payload, bearer, and basicauth.
>
> The payload method consists of a key-value parameter sent to each request made to the endpoint, the token_payload_name attribute is related to the key used and the token attribute is related to the value.

> **token_payload_name**:str=None (Required)
>
> The name(key) for the token parameter to be sent in requests.

> **token**:str=None (Required)
>
> The value to send in the key defined in token_payload_name.

### Bearer

```python
my_ingestor.set_auth(
              auth_type="bearer", 
              token="abcdef1234567890"
            )
```

> **auth_type**:str=None (Required)
>
> The authentication type. Types available in the latest version: payload, bearer, and basicauth.
>
> The bearer method consists of a bearer-type token to be sent with each request made to the endpoint.

> **token**:str=None (Required)
>
> The bearer token to be sent in requests.

### Basicauth

```python
my_ingestor.set_auth(
              auth_type="basicauth", 
              username="my_username",
              password="my_password"
            )
```

> **auth_type**:str=None (Required)
>
> The authentication type. Types available in the latest version: payload, bearer, and basicauth.
>
> The basicauth method consists of a username and password to be sent in all requests with the HTTPBasicAuth pattern.

> **username**:str=None (Required)
>
> The username to use.

> **password**:str=None (Required)
>
> The password to be used.

## Pagination

By default, the Ingestor performs a single request to the endpoint and saves the data obtained, but it is possible to configure a pagination for the Ingestor to perform multiple requests for the URL.

Some types of pagination are available:

### Default (incremental query)

```python
my_ingestor.set_pagination(
              pag_type="default",
              start_name="start", 
              limit=100, 
              limit_name="limit", 
              verification_path='additional_data>pagination>more_items_in_collection', 
              verification_condition=True
            )
```

> **pag_type**:str="default" (Optional)
>
> The type of pagination to be applied. Types available in the latest version: default and next_link.
>
> For the default type, the Ingestor will make multiple requests to the endpoint using an offset based on the value of the limit parameter passed. This repetition will continue as long as the verification continues to allow it, this verification occurs with the comparison of the value between an eventually available parameter and a conditional value. The verification_path field is a path for the parameter to be analyzed within the return of the last request, in the example above, the parameter is the more_items_in_collection key that is within pagination, which is within additional_data, for validation, the value present in this key will be compared with the value passed in the verification_condition attribute. Pagination will only continue while these values are absolutely equal, that is, if the parameter passed in verification_path does not exist or the comparison result is not true, paging will be terminated.

> **start_name**:str="start" (Optional)
>
> The name of the start parameter to be passed in requests.

> **limit**:int=100 (Optional)
>
> The limit to pass for the number of records per endpoint page. This parameter is also used for the offset step.

> **limit_name**:str="limit" (Optional)
>
> The name of the limit parameter to be passed in requests.

> **verification_pat**h:str=None (Required)
>
> The parameter or path to the value to compare for the paging condition. Use the “>” character to navigate a descending parameter.

> verification_condition=None (Required)
>
> The value to be compared with the parameter informed in verification_path. Pagination will continue as long as the value returned in the other parameter is EQUAL to the value passed here.

### Next link

```python
my_ingestor.set_pagination(
              pag_type="next_link",
              link_path="next_page", 
              page_validation_fn=lambda x: not x['end_of_stream'], 
              payload_update_fn=lambda p,d: {**p,"parameter":d['some_key']},
              keep_payload=True
            )
```

> **pag_type**:str="default" (Required)
>
> The type of pagination to be applied. Types available in the latest version: default and next_link.
> 
> For the next_link type, at each request the return contains a URL for the next request, in this mode, the Ingestor updates the URL at each request and thus pagination occurs.

> **link_path**:str=None (Required)
>
> The path to the returned parameter that contains the next URL. The “>” character can be used to access descendant objects.

> **page_validation_fn**=None (Optional)
>
> Allows a function to be passed so that each page is validated. This function receives the complete return of the last request and needs to return true to continue paging, that is, if this function returns anything other than true, the Ingestor will stop paging and terminate the process.

> **payload_update_fn**=None (Optional)
>
> Allows a function to be passed to update the payload of the request on each page. This function receives two parameters, the current payload and the complete return of the last request, the return of this function should be a dictionary that will be the new payload to be used in the next requests.

> **keep_payload**=True (Optional)
>
> If set to false, the payload passed in the work method is only sent in the first request, in subsequent requests no value is sent.

## Data destination

After instantiating the Ingestor, it is necessary to define the destination of the data to be saved. Due to the idea of the library to save data in the datalake entry, the script by default saves the data in some S3 bucket. The Ingestor, by default, always saves the results in JSON. Example of target configuration:

```python
my_ingestor.set_destination(
              s3_path="s3://my-bucket/path/to/landing/", 
              items_per_file=1000
            )
```

> **s3_path**:str=None (Required)
>
> The path for saving the data. Here you can also define some subfolder for partitioning, example: s3_path="s3://my-bucket/path/to/landing/partition=2022-04-08/".

> **items_per_file**:int=1000 (Optional)
>
> The number of records per JSON file within the folder. Each generated file represents a list of dictionaries, which is the received data.

## Ingestion start

After configuring the settings, the Ingestor can now start the ingest job:

```python
my_ingestor.work(
              payload={"some_parameter": "some_value"}, 
              response_path="results",
              force_json=False
            )
```

> **payload**:dict={} (Optional)
>
> Inform the payload to be sent in the request to the endpoint. These values ​​follow the method informed when creating the object, GET or POST.

> **response_path**:str=None (Optional)
>
> If the desired return is within some parameter in the return, this parameter can be used to extract the result. The “>” character can be used to navigate through descendant members.

> **force_json**:bool=False
>
> If the request method is POST, the payload can be sent in JSON format by setting this parameter to True.

Example:

```python
{
  "results": [
    {
      "id": 1,
      "name": "John"
    },
    {
      "id": 2,
      "name": "Mike"
    }
  ],
  "request_timestamp": "2022-04-08T18:10:44",
  "items": 2
}
```

On return above, to extract the desired data, use response_path=”results”.

## Requests errors

The Ingestor is prepared to deal with some errors that requests may receive, they are:

  - HTTP 404: Not found
  - HTTP 429: Too many requests
  - HTTP 500: Internal server error
  - HTTP 520: Unknown error
  - HTTP 529: Server is overloaded

For any of these errors, the Ingestor will try the request again after 1 minute of waiting. If an error occurs for 10 consecutive times, the script will not try new requests and invoke an error on the received error.
