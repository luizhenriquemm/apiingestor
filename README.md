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

By default, the Ingestor is started without any authentication and can be used that way, ie setting an authentication is optional.

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
