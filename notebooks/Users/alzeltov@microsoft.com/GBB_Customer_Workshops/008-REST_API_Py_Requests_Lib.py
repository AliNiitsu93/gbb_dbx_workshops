# Databricks notebook source
# MAGIC %md # Python Wrapper Module over Rest API

# COMMAND ----------

import collections     
import requests
import json

def dict_update(source, updates):
    """Update a nested dictionary or similar mapping.

    Modify ``source`` in place.
    """
    for key, value in updates.iteritems():
        if isinstance(value, collections.Mapping) and value:
            returned = dict_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = updates[key]
    return source

class DBC_REST_API (object):
    """
    This class acts as a wrapper for making calls to Databricks REST API
    Methods:
    reset_job - done
    get_job - done
    list_jobs - done
    get_jobID - done
    run_job - done
    list_runs - done
    get_run - done
    cancel_run - done
    list_clusters - done
    get_clusterID - done
    workspace_list
    workspace_mkdir
    workspace_import
    workspace_export
    workspace_get_status
    workspace_delete
    """
    
    def __init__(self, host, token):
        """
        """

        if not token or not host:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host     = host
        self.token    = token
        
    def reset_job(self, job_id, new_settings):
        """
        Desc: Overwrites the settings of a job with the provided settings.
        URL: POST /2.0/jobs/reset
        :param job:
        :param new_settings:
        :return:
        """

        #first get parameters of job
        data = self.get_job(job_id)
        #convert return to dict
        params_dict = json.loads(data.text)
        settings_only = params_dict['settings']
        #update dict with new setting
        updated_params = dict_update(settings_only, new_settings)

        api_call = '/api/2.0/jobs/reset'

        payload = {}
        payload["job_id"]=job_id
        payload["new_settings"]=updated_params

        req = self.__post_request(api_call, payload)

        return req

    def get_job(self, job_id):
        """
        Desc: Retrieves information about a single job.
        URL: GET /2.0/jobs/get
        :param job_id:
        :return:
        """
        api_call ='/api/2.0/jobs/get'

        payload = {}
        payload["job_id"]=job_id
        req = self.__get_request(api_call, payload)
        return req

    def list_jobs(self):
        """
        Desc: Lists all jobs.
        URL: GET /2.0/jobs/list
        :return:
        """
        api_call ='/api/2.0/jobs/list'
        payload={}
        req = self.__get_request(api_call, payload)
        return req

    def get_jobID(self, job_name):
        """
        Desc: Searches for Job by name and returns Job ID
        :param job_name:
        :return:
        """

        data = self.list_jobs()

        for job in data.json()['jobs']:
            if  job['settings']['name'] == job_name:
                id = job['job_id']
                return id

        return -1


    def run_job(self, job_id, notebook_params):
        """
        Desc: Runs the job now, and returns the run_id of the triggered run
        URL: POST /2.0/jobs/run-now
        :param job_id:
        :param notebook_params:
        :return:
        """
        api_call='/api/2.0/jobs/run-now'

        payload = {}
        payload["job_id"]=job_id
        payload["notebook_params"]=notebook_params

        req = self.__post_request(api_call, payload)

        return req

    def list_runs(self, job_id = -1, active_only=False, limit=20, offset=0):
        """
        Desc: Lists runs from most recently started to least
        URL: GET /2.0/jobs/runs/list
        :param job_id: Int - The job for which to list runs. If omitted, the Jobs service will list runs from all jobs.
        :param active_only: Bool - If true, lists active runs only; otherwise, lists both active and inactive runs.
        :param limit: Int - The number of runs to return. 0 means no limit. The default value is 20.
        :param offset: Int - The offset of the first run to return, relative to the most recent run.
        :return:
        """
        api_call ='/api/2.0/jobs/runs/list'

        payload = {}
        if job_id > -1:
          payload["job_id"]=job_id
        payload["active_only"]=active_only
        payload["limit"]=limit
        payload["offset"]=offset

        req = self.__get_request(api_call, payload)

        return req

    def get_run(self, run_id):
        """
        Desc: Retrieves the metadata of a run. Including Run Status.
        URL: GET /2.0/jobs/runs/get
        :param run_id:
        :return:
        """
        api_call='/api/2.0/jobs/runs/get'
        payload = {}
        payload["run_id"]=run_id

        req = self.__get_request(api_call, payload)

        return req

    def cancel_run(self, run_id):
        """
        Desc:Cancels a run. The run is canceled asynchronously, so when this request completes,
        the run may still be running. The run will be terminated shortly. If the run is already
        in a terminal life_cycle_state, this method is a no-op.
        URL:  /2.0/jobs/runs/cancel
        :param run_id:
        :return:
        """
        api_call='/api/2.0/jobs/runs/cancel'
        payload = {}
        payload["run_id"]=run_id

        req = self.__post_request(api_call, payload)

        return req

    def get_clusterID(self, cluster_name, comp_type ='eq', state='RUNNING'):
        """

        :param cluster_name:
        :param comp_type: default is 'eq'
        :param state: default is 'RUNNING'
        :return:
        """
        data = self.list_clusters()
        comp = comp_type.lower()

        for cluster in data.json()['clusters']:
          if comp == 'in':
            if  cluster_name in cluster['cluster_name'] and cluster['state'] == state:
              id = cluster['cluster_id']
              return id
          elif comp == 'eq':
            if  cluster['cluster_name'] == cluster_name and cluster['state'] == state:
              id = cluster['cluster_id']
              return id
          else:
            raise RuntimeError("Invalid parameter \'{0}\' entered in 'get_clusterID_by_name'".format(comp_type))

        return -1

    def list_clusters(self):
        """
        Desc: Returns information about all clusters which are currently active or which were terminated within the past hour.
        URL:  GET /2.0/clusters/list
        :param run_id:
        :return:
        """
        api_call ='/api/2.0/clusters/list'
        payload={}
        req = self.__get_request(api_call, payload)
        return req

    def create_cluster(self, cluster_specs):
        """
        Desc: Create a cluster -
        URL:  POST /2.0/clusters/create
        :param cluster_specs: Specifications of cluster to create
        :return: Cluster ID of newly created cluster
        """

        api_call='/api/2.0/clusters/create'
        payload = cluster_specs

        req = self.__post_request(api_call, payload)
        return req


    def delete_cluster(self, cluster_name):
        """
        Desc: Deletes a cluster by name
        URL:  POST /2.0/clusters/delete
        :param cluster_name: Name of cluster to delete
        :return:
        """

        cluster_id = self.get_clusterID(cluster_name)

        api_call='/api/2.0/clusters/delete'
        payload = {}
        payload["cluster_id"]=cluster_id

        req = self.__post_request(api_call, payload)
        return req
      
      
    def workspace_delete(self, path, recursive=False):
        """
        Desc: Deletes an object or directory from DBC
        URL:  POST 2.0/workspace/delete
        path: The absolute path of the notebook or directory.
        recursive: The flag that specifies whether to delete the object recursively. It is false by default.
        :return:
        """
        
        api_call='/api/2.0/workspace/delete'
        payload = {}
        payload["path"]=path
        payload["recursive"]=recursive

        req = self.__post_request(api_call, payload)
        return req
      
      
    def workspace_export(self, path, format):
        """
        Desc: Exports a notebook or contents of an entire directory
        URL:  GET 2.0/workspace/export
        path: The absolute path of the notebook or directory.
        recursive: The flag that specifies whether to delete the object recursively. It is false by default.
        :return:
        """

        api_call='/api/2.0/workspace/export'
        payload = {}
        payload["path"]=path
        payload["recursive"]=recursive

        req = self.__get_request(api_call, payload)
        return req
      
    def workspace_get_status(self, path):
        """
        Desc: Gets the status of an object or a directory. 
              If path does not exist, this call returns an error RESOURCE_DOES_NOT_EXIST
        URL:  GET 2.0/workspace/get-status
        path: The absolute path of the notebook or directory. This field is required.
        :return:
        """

        api_call='/api/2.0/workspace/get-status'
        payload = {}
        payload["path"]=path

        req = self.__get_request(api_call, payload)
        return req
      
    def workspace_import(self, path, format, language, content, overwrite = False):
        """
        Desc: Imports a notebook or the contents of an entire directory. If path already exists and overwrite is set to false, 
              this call returns an error RESOURCE_ALREADY_EXISTS. One can only use DBC format to import a directory.
        URL:  Post 2.0/workspace/import
        path: The absolute path of the notebook or directory. Importing directory is only support for DBC format.
        format: This specifies the format of the file to be imported. By default, this is SOURCE. 
                However it may be one of: SOURCE, HTML, JUPYTER, DBC. The value is case sensitive.
        language:	The language. If format is set to SOURCE, this field is required; otherwise, it will be ignored.
        content: Type-Bytes The base64-encoded content. This has a limit of 10 MB. If the limit (10MB) is exceeded, exception with error
                code MAX_NOTEBOOK_SIZE_EXCEEDED will be thrown. This parameter might be absent, and instead a posted file will be used. 
        overwrite: The flag that specifies whether to overwrite existing object. It is false by default. 
                   For DBC format, overwrite is not supported since it may contain a directory       
        :return:
        """
        
        api_call='/api/2.0/workspace/import'
        payload = {}
        payload["path"]=path
        payload["format"]=format
        payload["language"]=language
        payload["overwrite"]=overwrite
        payload["content"]=content
        
        req = self.__post_request(api_call, payload)
        return req
        
    def workspace_list(self, path):
        """
        Desc: Lists the contents of a directory, or the object if it is not a directory. 
              If the input path does not exist, this call returns an error RESOURCE_DOES_NOT_EXIST
        URL:  GET 2.0/workspace/list
        path: The absolute path of the notebook or directory. This field is required.
        :return:
        """
        
        api_call='/api/2.0/workspace/list'
        payload = {}
        payload["path"]=path

        req = self.__get_request(api_call, payload)
        return req
      
    def workspace_mkdir(self, path):
        """
        Desc: Creates the given directory and necessary parent directories if they do not exists. 
              If there exists an object (not a directory) at any prefix of the input path, 
              this call returns an error RESOURCE_ALREADY_EXISTS.
        URL:  POST 2.0/workspace/mkdirs
        path: The absolute path of the directory. If the parent directories do not exist, it will also create them. 
              If the directory already exists, this command will do nothing and succeed.
        :return:
        """

        api_call='/api/2.0/workspace/mkdirs'
        payload = {}
        payload["path"]=path

        req = self.__post_request(api_call, payload)
        return req
        
    def __post_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """


        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        headers = {'Authorization': 'Bearer ' + self.token}
        req = requests.post(url, headers=headers, data=payload_json)
        return req


    def __get_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """


        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        headers = {'Authorization': 'Bearer ' + self.token}
        req = requests.get(url, headers=headers, data=payload_json)
        return req