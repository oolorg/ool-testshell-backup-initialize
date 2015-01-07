#!/usr/bin/python
# coding:utf-8

from flask import Flask, abort, redirect, url_for, request, json, Response
from functools import wraps
import svrst_manager
import ConfigParser
import tsbk_manager
import logging
import logging.handlers
import svbk_conflict

app = Flask(__name__)
app.debug = True
#app.debug = False

KEY_TOKEN='Token'
KEY_TOPOLOGY='topology_name'
KEY_TENANT='tenant_name'
KEY_BR_MODE='br_mode'
KEY_BACKUP='backup_name'
KEY_NODE_LIST='node_list'
KEY_OPT='option'
KEY_STATUS='status'
KEY_MSG='msg'
KEY_TARGET_OS='target_os'

FILEDIR="/etc/backuprestore"
CONFIG_FILE = 'ool_br_rest.ini'

logger = logging.getLogger('bktestShelllog')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
logger.addHandler(handler)

#--------------------------------------------------------------
def consumes(content_type):
	def _consumes(function):
		@wraps(function)
		def __consumes(*argv, **keywords):
			if request.headers['Content-Type'] != content_type:
				abort(400)
			return function(*argv, **keywords)
		return __consumes
	return _consumes

def isNull(str_data):
	if '' != str_data:
		return False
	return True

def set_system_param():

	conf = ConfigParser.SafeConfigParser()

	set_file_path='%s/%s' % (FILEDIR, CONFIG_FILE)

	ret=conf.read(set_file_path)

	if len(ret)==0 :
		print '####set_system_param ng file is nothing '
		return [-1,0,0]

	print '####set_system_param file_name :%s' %(ret[0])

	host = conf.get('options', 'host')
	port= conf.get('options', 'port')

	return [0, host, port]

@app.route("/")
def index():
	res = Response()
	res.status_code=501
	return res

@app.route("/br",methods=['POST'])
@consumes('application/json')
def reset():
	# get parameter
	print request.data
	req_data=json.loads(request.data)
	
	# Check key item
	try:
		if (KEY_TOKEN not in req_data):
			err_msg=KEY_TOKEN
			raise Exception
		if (KEY_TOPOLOGY not in req_data):
			err_msg=KEY_TOPOLOGY
			raise Exception
		if (KEY_TENANT not in req_data):
			err_msg=KEY_TENANT
			raise Exception
		if (KEY_BR_MODE not in req_data):
			err_msg=KEY_BR_MODE
			raise Exception
		if (KEY_BACKUP not in req_data):
			err_msg=KEY_BACKUP
			raise Exception
		if (KEY_NODE_LIST not in req_data):
			err_msg=KEY_NODE_LIST
			raise Exception
		if (KEY_OPT not in req_data):
			err_msg=KEY_OPT
			raise Exception
	except Exception:
		res = Response()
		res.status_code=400
		res.data=json.dumps({"status":"NG","msg":"bad parameter:key(%s is not found)" % err_msg})
		return res

	# check null
	try:
		Token = req_data[KEY_TOKEN]
		if isNull(Token):
			err_msg=KEY_TOKEN
			raise Exception

		topology_name = req_data[KEY_TOPOLOGY]
		if isNull(topology_name):
			err_msg=KEY_TOPOLOGY
			raise Exception

		tenant_name = req_data[KEY_TENANT]
		if isNull(tenant_name):
			err_msg=KEY_TENANT
			raise Exception

		br_mode = req_data[KEY_BR_MODE]
		if (('r' != br_mode) and ('b' != br_mode) and ('i' != br_mode) and ('r_info' != br_mode) and ('del' != br_mode)):
			err_msg=KEY_BR_MODE
			raise Exception
	
		backup_name = req_data[KEY_BACKUP]
		if isNull(backup_name):
			err_msg=KEY_BACKUP
			raise Exception

		node_list = req_data[KEY_NODE_LIST]
		if False == isinstance(node_list, list):
			err_msg=KEY_NODE_LIST
			raise Exception

		option = req_data[KEY_OPT]
		
	except Exception:
		res = Response()
		res.status_code=400
		res.data=json.dumps({"status":"NG","msg":"bad parameter:key(%s is NULL)" % err_msg})
		return res

	# Exectute
	res_data = {}

	if 'del' == br_mode:

		index = option["index"];

		tsbk=tsbk_manager.tsbk_manager(logger)

		retArray = tsbk.delete_dblist(0, Token, br_mode, topology_name, tenant_name, index)

		res_data.update({KEY_STATUS:retArray[0]})
		res_data.update({KEY_MSG:retArray[1]})

	if 'r_info' == br_mode:
		tsbk=tsbk_manager.tsbk_manager(logger)

		retArray = tsbk.get_dblist(0, Token, br_mode, topology_name, tenant_name)

		res_data.update({KEY_STATUS:retArray[0]})
		res_data.update({KEY_MSG:retArray[1]})

	if 'r' == br_mode:
		#pass
		#ret=["OK","restore success"]
		#res_data.update({KEY_STATUS:ret[0]})
		#res_data.update({KEY_MSG:ret[1]})

		#tsbk=tsbk_manager.tsbk_manager()
		#retArray = tsbk.restore_cluster(0, Token, topology_name, tenant_name, node_list,restore_name=backup_name)
		#res_data.update({KEY_STATUS:retArray[0]})
		#res_data.update({KEY_MSG:retArray[1]})

		print option["index"]

		index = option["index"];

		tsbk=tsbk_manager.tsbk_manager(logger)
		retArray = tsbk.restore_cluster_index(0, Token, topology_name, tenant_name, node_list,index)

		res_data.update({KEY_STATUS:retArray[0]})
		res_data.update({KEY_MSG:retArray[1]})


	if 'b' == br_mode:
		#pass
		#retArray=["OK","restore success"]
		tsbk=tsbk_manager.tsbk_manager(logger)
		retArray = tsbk.backup_cluster(0, Token, topology_name, tenant_name, node_list,backup_name=backup_name)
		res_data.update({KEY_STATUS:retArray[0]})
		res_data.update({KEY_MSG:retArray[1]})


	if 'i' == br_mode:

		if KEY_TARGET_OS in option :
			target_os = option["target_os"]
		else:
			target_os = ''

		svrst=svrst_manager.svrst_manager()
		svrst.set_Token(Token)
		svrst.set_node_list(node_list)
		ret = svrst.reset_cluster(group_name=topology_name, tenant_name=tenant_name, target_os=target_os)

		res_data.update({KEY_STATUS:ret[0]})
		res_data.update({KEY_MSG:ret[1]})

	res=json.dumps(res_data)
	return res

@app.route('/redir')
def redir():
	return redirect(url_for('index'))

if __name__ == "__main__":

	conf = svbk_conflict.svbk_conflict()
	conf.del_file()

	ret = set_system_param()
	if 0 == ret[0]:
		app.run(host=ret[1], port=int(ret[2]))

