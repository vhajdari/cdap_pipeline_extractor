#! /usr/bin/env python

##################################################################
# Mario - The CDAP Pipeline Extraction Tool
#
# Author: Tony Hajdari, tony@cask.co
#
# Use:  Allows you to extract all the deployed 
#       pipelines from your CDAP instance.
##################################################################
import requests,json,logging, sys, os
#from logging.config import fileConfig
#from logging.handlers import RotatingFileHandler
from logging import handlers


# #fileConfig("log.conf")
log = logging.getLogger('')
log.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

# fh = handlers.RotatingFileHandler(LOGFILE, maxBytes=(1048576*5), backupCount=7)
# fh.setFormatter(format)
# log.addHandler(fh)

host = 'localhost'
port = '11015'
cdap = 'http://'+ host + ':' + port
namespaces = '/v3/namespaces'
drafts = '/v3/configuration/user'
p = { 'name':'','description':'','artifact':'','config':'' }
output = 'Pipelines'

def getJSON(url):
	r = requests.get(url)
	d = r.json()
	#d = json.loads(r.text)
	return d #returns a dict

#Example of an App collection endpoint
# http://localhost:11015/v3/namespaces/default/apps
def getApps(ns):
	return getJSON(cdap + namespaces + '/' + ns + '/apps')

#Example of an individual App endpoint
#http://localhost:11015/v3/namespaces/default/apps/MyAppName
def getApp(ns, id):
	return getJSON(cdap + namespaces + '/' + ns + '/apps' + '/' + id)

#Get the available namespaces for this CDAP instance
def getNamespaces():
	return getJSON(cdap + namespaces)

# Get pipeline drafts 
def getDrafts():
	return getJSON(cdap + drafts)

#Write the pipelline config out to a file		
def exportPipeline(ns, id, data):
	fileName = id + '.json'
	directory = output + '/' + ns
	path =  directory + '/' + fileName
	
	if not os.path.exists(directory):
		os.makedirs(directory)

	with open(path, 'w') as f:
		f.write(data)


#Get the draft pipelines -- this is NOT namespace specific
#will retrieve the drafts in ALL namespaces
drafts = getDrafts()

#loop through all namespaces
for namespace in getNamespaces():
	
	#set the global namespace name
	ns = namespace.get('name')
	log.debug('Namespace: %s', ns)
	
	#get all the drafts per namespace	
	d = drafts.get('property').get('hydratorDrafts').get(ns)
	name = d.itervalues().next().get('name')	
	p['name'] = name
	p['artifact'] = d.itervalues().next().get('description')
	p['artifact'] = d.itervalues().next().get('artifact')
	p['config']	= d.itervalues().next().get('config')
	spec = json.dumps(p)
	#log.debug('Draft Pipeline: %s', spec)
	exportPipeline(ns, name, spec)

	#Export the deployed pipelines in this namespace
	for i in getApps(ns):
		#filer out anything other than cdap-data-pipeline
		artifactType = i.get('artifact').get('name')
		if "cdap-data-pipeline" in artifactType:  
			id = i['id']
			if not id in ('_Tracker', 'dataprep'):
				log.debug('App Namespace: %s', ns)
				log.debug('pipeline name: %s', id)
				app = getApp(ns,id)
				#log.debug('Pipeline = %s', json.dumps(app, sort_keys=True, indent=4))
				p['name'] = app.get('name')
				p['description'] = app.get('description')
				p['artifact'] = app.get('artifact')
				p['config']	= json.loads(app.get('configuration'))
				spec = json.dumps(p, sort_keys=True, indent=4)
				exportPipeline(ns, id, spec)






