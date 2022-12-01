# version: 1.0.5

import requests, json, boto3, datetime, math, logging, time

class Ingestor():
  authentication = {'auth': None}      # Authentication handler            
  pagination = False                   # Pagination handler
  initial_payload = {}                 # The initial payload, used for payload authentication
  initial_headers = {}                 # The initial headers, used for other authentication
  destination = {}                     # Destination handler
  data = []                            # All the data getted will be saved temporaly here
  data_all_count = 0                   # An counter for all the data getted
  files_created = []                   # A list with all the files saved at the landing folder
  errors = []                          # A list of errors getted in all processes
  workinfo = {}                        # An object for hold metadata about the work
  filter_function = None               # Function for filter the data before saving it
  variations = None                    # A list of variations case set
  data_extractor_fn = None             # The function to extract data from the results
  data_extracted = []                  # The results from extractions
  allowed_errors = [404, 409, 429, 500, 520, 525, 529]
  skip_errors = []
  options = {}
  
  def __init__(self, 
               api_name:str=None,
               url:str=None, 
               method:str=None):
    if api_name is None:
      raise Exception("The api_name must be setted when creating the objetct.")
    if url is None:
      raise Exception("The url must be setted when creating the objetct.")
    if method is None:
      raise Exception("The method must be setted when creating the objetct.")
    if method.upper() not in ['GET', 'POST']: # In the future, add 'PUT', 'DELETE'
      raise Exception("The method value setted is invalid.")
    self.date_run = datetime.datetime.now() 
    self.api_name = api_name
    self.url = url
    self.method = method
    self.log(f"APIIngestin started for {api_name}, {url} using {method}.")
    return
  
  log_method = "logging"
  def set_log_method(self, method:str="logging"):
    self.log_method = method

  def log(self, text):
    if self.log_method == "logging":
      logging.info(text)
    else:
      print(text)
    return
  
  def see_error(self, n):
    e = self.errors[n-1][1]
    if e != "":
      print(e)
    else:
      print(f"There's no more info about the error #{n}.")
    return
     
  def set_auth(self,
               auth_type:str=None,
               **kwargs):
    if auth_type is None:
      raise Exception("The argument auth_type must be setted for this method.")
    if auth_type.lower() not in ['payload', 'bearer', 'basicauth', 'headers']:
      raise Exception("The auth_type defined is invalid.")
      
    if auth_type == 'payload':
      if 'token_payload_name' not in kwargs:
        raise Exception("For payload authentication type, you must provide the token_payload_name at the set_auth() method.")
      if 'token' not in kwargs:
        raise Exception("For payload authentication type, you must provide the token at the set_auth() method.")
      self.initial_payload[kwargs['token_payload_name']] = kwargs['token']
    elif auth_type == 'bearer':
      if 'token' not in kwargs:
        raise Exception("For bearer authentication type, you must provide the token at the set_auth() method.")
      self.initial_headers = {
        'Authorization': 'Bearer ' + str(kwargs['token'])
      }
    elif auth_type == 'basicauth':
      if 'username' not in kwargs:
        raise Exception("For HTTP Basic Auth type, you must provide the username at the set_auth() method.")
      if 'password' not in kwargs:
        raise Exception("For HTTP Basic Auth type, you must provide the password at the set_auth() method.")
      self.authentication['auth'] = (kwargs['username'], kwargs['password'])
    elif auth_type == 'headers':
      if 'token_header_name' not in kwargs:
        raise Exception("For payload authentication type, you must provide the token_header_name at the set_auth() method.")
      if 'token' not in kwargs:
        raise Exception("For payload authentication type, you must provide the token at the set_auth() method.")
      self.initial_headers = {kwargs['token_header_name']: kwargs['token']}
    self.authentication.update({"auth_type": auth_type, **kwargs})
    self.log(f"Authentication setted for use {auth_type} method.")
    return
      
  def set_pagination(self,
                     pag_type:str="default",
                     **kwargs):
    if pag_type not in ['default', 'next_link']:
      raise Exception("The value setted for pag_type is invalid.")
    if pag_type == "default":
      if "verification_path" not in kwargs:
        raise Exception("The verification_path value must be setted with the path for the condition check.")
      if "verification_condition" not in kwargs:
        raise Exception("The verification_condition value must be setted with the value for comparasion, the script will call the api again for the pagination while false is getted.")
      if "start_name" not in kwargs:
        start_name = "start"
      if "limit" not in kwargs:
        limit = 100
      if "limit_name" not in kwargs:
        limit_name = "limit"
      self.log(f"Pagination setted for {pag_type} using {kwargs['limit']} items per page.")  
    elif pag_type == "next_link":
      if "link_path" not in kwargs:
        raise Exception("You must pass the link_path for next_link pagination type.")
      self.log(f"Pagination setted for {pag_type} looking for {kwargs['link_path']} as next link")
    self.pagination = {
      "pag_type": pag_type,
      **kwargs
    }
    return
    
  def set_destination(self,
                      s3_path:str=None,
                      items_per_file:int=1000):
    if s3_path is None:
      raise Exception("You must set the s3_path value with the path for the file to be saved")
    self.destination = {
      "s3_path": s3_path,
      "items_per_file": items_per_file
    }
    self.log(f"Destination setted for the path {s3_path}.")
    return
  
  def set_filter(self,
                 function=None):
    if function is None:
      raise Exception("You must set the function for this method")
    if not callable(function):
      raise Exception("The object passed as function is not a function")
    self.filter_function = function
    
  def set_variations(self,
                     variations:list=None):
    if variations is None:
      raise Exception("You must set the variations list for this method")
    if self.pagination is not False and self.pagination['pag_type'] == "next_link":
      raise Exception("URL variations is not compatible with next_link pagination method")
    self.variations = {"list": variations, "index": 0}
    
  def set_data_extractor(self,
                         fn=None):
    if fn is None:
      raise Exception("You must set the fn function for this method")
    self.data_extractor_fn = fn
    
  def undo(self):
    c = 0
    if len(self.files_created) > 0:
      for item in self.files_created:
        bucket = item.replace("s3://", "").split("/")[0]
        file = "/".join(item.replace("s3://", "").split("/")[1:])
        boto3.resource('s3').Object(bucket, file).delete()
        c += 1
      self.log(f"A total of {c} file(s) was deleted")
    else:
      self.log("There's no file to delete")
    
  def save_data(self, 
                force:bool=False):
    bucket = self.destination["s3_path"].replace("s3://", "").split("/")[0]
    folder = "/".join(self.destination["s3_path"].replace("s3://", "").split("/")[1:])
    if folder[-1] != "/":
      folder += "/"
    if len(self.data) > 0:
      if force or len(self.data) >= self.destination['items_per_file']:
        to_save = self.filter_function(self.data) if callable(self.filter_function) else self.data
        if self.data_extractor_fn is not None:
          self.data_extracted += [self.data_extractor_fn(x) for x in to_save]
        self.data_all_count += len(to_save)
        file = self.api_name + "-" + self.date_run.strftime("%Y-%m-%dT%H:%M:%S+0000")  + "-" + str(len(self.files_created)) + ".json"
        boto3.resource('s3').Object(bucket, folder + file).put(Body=(bytes(json.dumps(to_save).encode('UTF-8'))))
        self.files_created.append(f"s3://{bucket}/{folder}{file}")
        self.data = []
    return
  
  def work(self,
           payload:dict={},
           response_path:str=None,
           force_json:bool=False,
           filter_function=None,
           variation:str=""):
    if self.variations is not None:
      while self.variations['index'] < len(self.variations['list'])-1:
        self.worker(payload=payload,
                    response_path=response_path,
                    force_json=force_json,
                    filter_function=filter_function,
                    variation=self.variations['list'][self.variations['index']])
        self.log(f"The variation {self.variations['list'][self.variations['index']]} is done")
        self.variations['index'] += 1
        
    else:
      self.worker(payload=payload,
                  response_path=response_path,
                  force_json=force_json,
                  filter_function=filter_function)

    self.work_done()
    return
    
  def worker(self,
           payload:dict={},
           response_path:str=None,
           force_json:bool=False,
           filter_function=None,
           variation:str=""):
    if self.destination == {}:
      raise Exception("You must set the destination before with de method set_destination().")
    if self.variations is not None:
      if variation == "":
        variation = self.variations["list"][0]
    self.workinfo["payload"] = payload
    self.workinfo["response_path"] = response_path
    self.workinfo["force_json"] = force_json
    self.workinfo["filter_function"] = filter_function
    self.workinfo["variation"] = variation
    self.workinfo["start"] = datetime.datetime.now()
    error_recurrence = 0
    pagination_payload = {}
    while True:
      if self.pagination is not False:
        if self.pagination['pag_type'] == "default":
          if pagination_payload == {}:
            pagination_payload = {
              self.pagination['start_name']: 0,
              self.pagination['limit_name']: self.pagination['limit']
            }
          else:
            pagination_payload[self.pagination['start_name']] += self.pagination['limit']
    
      if self.method.upper() == 'GET':
        try:
          r = requests.get(self.url+variation, headers={**self.initial_headers}, params={**self.initial_payload, **payload, **pagination_payload}, auth=self.authentication['auth'])
        except Exception as e:
          self.log("\nAn error ocurred in requets: " + str(e) + "\nRequest data:\n  URL: " + str(self.url+variation) + "\n  Headers: " + str({**self.initial_headers}) + "\n Auth: " + str(self.authentication['auth']) + "\n  Params: " + str({**self.initial_payload, **payload, **pagination_payload}))
          break
      elif self.method.upper() == 'POST':
        if force_json:
          try:
            r = requests.post(self.url+variation, headers={**self.initial_headers}, json={**self.initial_payload, **payload, **pagination_payload}, auth=self.authentication['auth']) 
          except Exception as e:
            self.log("\nAn error ocurred in requets: " + str(e) + "\nRequest data:\n  URL: " + str(self.url+variation) + "\n  Headers: " + str({**self.initial_headers}) + "\n Auth: " + str(self.authentication['auth']) + "\n  JSON: " + str({**self.initial_payload, **payload, **pagination_payload}))
            break
        else:
          try:
            r = requests.post(self.url+variation, headers={**self.initial_headers}, data={**self.initial_payload, **payload, **pagination_payload}, auth=self.authentication['auth'])
          except Exception as e:
            self.log("\nAn error ocurred in requets: " + str(e) + "\nRequest data:\n  URL: " + str(self.url+variation) + "\n  Headers: " + str({**self.initial_headers}) + "\n Auth: " + str(self.authentication['auth']) + "\n  Data: " + str({**self.initial_payload, **payload, **pagination_payload}))
            break

      if r.status_code != 200:
        if r.status_code not in self.skip_errors:
          error_recurrence += 1
          ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+0000")
          try:
            error_text = r.text
          except:
            error_text = ""
          self.errors.append([f"{ts}: HTTP {r.status_code} getted at {self.url+variation}", error_text])
          if error_recurrence >= 10:
            self.log("Max of 10 errors getted in sequence, the process will stop now.")
            self.work_done("Reached 10 errors in sequence while calling de api")
          if r.status_code in self.allowed_errors:
            if self.pagination is not False:
              if self.pagination['pag_type'] == "default":
                pagination_payload[self.pagination['start_name']] -= self.pagination['limit']       # For get the same page again
            self.log(f"HTTP {r.status_code} getted from the endpoint, waiting 1min before trying the same request again...")
            time.sleep(60)
            continue
          else:
            try:
              self.log("Error: ", r.text)
            except:
              pass
            raise Exception(f"HTTP {r.status_code} while calling the api.")
        else:
          self.log(f"HTTP {r.status_code} getted from the endpoint, this error code is marked for skiping, nothing will be done...")
          break
      error_recurrence = 0

      if response_path is None:
        if isinstance(r.json(), list):
          data_json = r.json()
        else:
          data_json = [r.json()]
      else:
        d = r.json()
        for x in response_path.split(">"):
          try:
            d = d[x]
          except:
            self.log(f"The key {x} doesnt exist, aborting..")
            break
        if isinstance(d, list):
          data_json = d
        else:
          data_json = [d]

      if "save_api_url" in self.options and self.options['save_api_url'] == True:
        for dj in data_json:
          dj['api_url'] = self.url+variation

      self.data += data_json

      if self.pagination is False:
        self.save_data()
        break
      else:
        if self.pagination['pag_type'] == "default":
          try:
            d = r.json()
            for x in self.pagination['verification_path'].split(">"):
              d = d[x]
            if d != self.pagination['verification_condition']:
              self.save_data()
              self.log(f"Page finished with {len(self.data) + self.data_all_count} records, finished.")
              break
            if 'page_validation_fn' in self.pagination:
              if self.pagination['page_validation_fn'](r.json()) != True:
                self.save_data()
                self.log(f"page_validation_fn is not true, finishing with {len(self.data) + self.data_all_count} records.")
                break
            self.log(f"Page finished with {len(self.data) + self.data_all_count} records, going to next page...")
          except:
            self.save_data()
            self.log(f"An error ocurred while checking the condition for pagination, finishing with {len(self.data) + self.data_all_count} records.")
            break
          if (len(self.data) + self.pagination['limit']) >= self.destination['items_per_file']:
            self.save_data()
        elif self.pagination['pag_type'] == "next_link":
          try:
            d = r.json()
            for x in self.pagination['link_path'].split(">"):
              d = d[x]
            if d != None and d != "":
              self.url = d
              self.log(f"Page finished with {len(self.data) + self.data_all_count} records, going to next page at {self.url}")
              if 'payload_update_fn' in self.pagination and callable(self.pagination['payload_update_fn']):
                payload = self.pagination['payload_update_fn'](payload, r.json())
            else:
              self.save_data()
              self.log(f"Page finished with {len(self.data) + self.data_all_count} records, finished.")
              break
            if 'page_validation_fn' in self.pagination:
              if self.pagination['page_validation_fn'](r.json()) != True:
                self.save_data()
                self.log(f"page_validation_fn is not true, finishing with {len(self.data) + self.data_all_count} records.")
                break
          except:
            self.save_data()
            self.log(f"An error ocurred while checking the next link for pagination, finishing with {len(self.data) + self.data_all_count} records.")
            break
          if (len(self.data)) >= self.destination['items_per_file']:
            self.save_data()
    return
  
  def work_done(self, error:str=""):
    self.save_data(force=True)
    self.workinfo["end"] = datetime.datetime.now()
    h, m, s = 0, 0, (self.workinfo["end"] - self.workinfo["start"]).seconds
    if s >= 3600:
      h = math.floor(s/3600)
      s = s - (h*3600)
    if s >= 60:
      m = math.floor(s/60)
      s = s - (m*60)
    ts = f"{h}:{m}:{s}"
    self.log(f"Work done, saved {len(self.data) + self.data_all_count} records, time taked: {ts}")
    if len(self.errors) >= 1:
      self.log("\nOne or more errors ocurred:")
      for i in range(len(self.errors)):
        self.log(f"  #{i}: {self.errors[i][0]}")
      self.log("You can try see more info about the error by calling the method see_error(1) for error #1 by example.")
      self.log("Some errors are predictable, the ingestor is able to not lose data whitin thoses. Check the documentation for more info.")
    if error != "":
      self.log(error)
      raise Exception(error)
