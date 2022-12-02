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
> api_name:str=None (Required)

Enter the API name here along with the name of the endpoint to be ingested. This name is passed on to the name of the files saved on the landing during the process.
> url:str=None (Required)

Enter the URL of the endpoint to be ingested here.
> method:str=None (Required)

The method for endpoint requests. Methods available in the latest version: GET and POST.
After the object is instantiated, some other settings can be set as follows.
