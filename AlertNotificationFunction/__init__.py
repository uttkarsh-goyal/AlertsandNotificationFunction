import logging
import os
from azure.identity import ClientSecretCredential 
from azure.purview.catalog import PurviewCatalogClient
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import  BlobClient
from deepdiff import DeepDiff
import json
import re
import azure.functions as func

client_id = os.environ["client_id"]
client_secret = os.environ["client_secret"]
tenant_id = os.environ["tenant_id"]
reference_name_purview = os.environ["reference_name_purview"]
ContainerName = os.environ["ContainerName"]
GuidList = os.environ["GuidList"]
blobConnectionString = os.environ["blobConnectionString"]

def get_credentials():
	credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
	return credentials

def get_catalog_client():
	credentials = get_credentials()
	client = PurviewCatalogClient(endpoint=f"https://{reference_name_purview}.purview.azure.com/", credential=credentials, logging_enable=True)
	return client

def main(mytimer: func.TimerRequest) -> None:
	try:
		logging.info(f"list of guid are {GuidList}")
		client = get_catalog_client()
		logging.info("Successfully login using Service Principal")
		
		ListOfGuid = GuidList.split(",")
		print(ListOfGuid)
		for Guid in ListOfGuid:
			logging.info(f"Checking data for Guid = {Guid}")
		
			Data = client.entity.get_by_guid(guid=Guid)["entity"]
			entityType = Data["typeName"]
		
			if Data["typeName"]=="azure_sql_table":
				logging.info(f"Entity type is {entityType}")
				OriginalData = Data["relationshipAttributes"]["columns"]
				blobName = Guid+".json"
			elif Data["typeName"]=="AtlasGlossary":
				logging.info(f"Entity type is {entityType}")
				OriginalData = Data["relationshipAttributes"]["terms"]
				blobName = Guid+".json"
			#print(OriginalData)
		
			#get data from blob
			blob_service_client = BlobClient.from_connection_string(conn_str=blobConnectionString, container_name=ContainerName,blob_name=blobName ) 
			if blob_service_client.exists():
				logging.info(f"There is difference in MetaData for {entityType}")

				streamdownloader = blob_service_client.download_blob()
				OldData = json.loads(streamdownloader.readall())
	
				if OldData!=OriginalData:
					Delta = DeepDiff(OldData, OriginalData, ignore_order=True)
					DeltaString = json.dumps(Delta) 
					#print(DeltaString)

					tempBlob = BlobClient.from_connection_string(conn_str=blobConnectionString, container_name=ContainerName,blob_name="temp.json" )
					if tempBlob.exists():
						tempBlob.delete_blob()
					tempBlob.upload_blob(DeltaString)
					tempDownload = tempBlob.download_blob()
					tempData = json.loads(tempDownload.readall())
	
					for key  in tempData:
						test567 = json.dumps(tempData[key])

						#to replace regix with some sepecific number while getting delta
						regixPattern = '\[[0-9]+\]'
						replaceString = '[1]'
						deltaOutput = re.sub(pattern=regixPattern, repl=replaceString, string=test567)
	
						DeltaBlob = Data["typeName"]+"/"+key+".json"
	
						#print(DeltaBlob)
						blobConnect = BlobClient.from_connection_string(conn_str=blobConnectionString, container_name=ContainerName,blob_name=DeltaBlob )
						if blobConnect.exists():
							blobConnect.delete_blob()
					
						DeltaBlobUpload = blobConnect.upload_blob(deltaOutput)
						
					tempBlob.delete_blob()
		
				else: logging.info(f"There is no difference in MetaData for {entityType}")
			else:
				print("new entity is created")
				logging.info(f"New Entity has been inserted with Guid ={Guid}")
			
				#upload metadata to blob
			OriginalDataToString = json.dumps(OriginalData)
			
			blob = BlobClient.from_connection_string(conn_str=blobConnectionString, container_name=ContainerName,blob_name=blobName )
			if blob.exists():
				blob.delete_blob()
			blobUpload = blob.upload_blob(OriginalDataToString)
			logging.info(f"MetaData has been updated for Guid = {Guid}")
	
	except HttpResponseError as e:
		logging.error(e)
		print(e)

