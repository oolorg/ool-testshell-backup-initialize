import urllib2
import json
import os

import logging.handlers

# stub mode
#    ON : read test file
#    OFF: read data from device manager
STUB_FLAG = 'OFF'
HOME_DIR = './'
PY_PATH = os.path.abspath(os.path.dirname(__file__)) 
CNF_FILE = 'db_info.cnf'

DB_URL_KEY = 'DB_URL='
DB_PORT_KEY = 'DB_PORT='
ROOT_DIR = '/ool_rm/'

PLANE_LIST = ['C-Plane', 'M-Plane', 'D-Plane', 'S-Plane', 'B-Plane', 'M2-Plane']

DBG_FLAG = 'OFF'
#DBG_FLAG = 'ON'

ERR_MSG_DEVICE = 'NOT found device name'
ERR_MSG_CLUSTER = 'NOT found cluster name'
ERR_MSG_TENNANT = 'NOT found tenant name'

GET_NODE_KEY   = ['device_name','status','owner','site','traffic_type']
GET_SWITCH_KEY = ['device_name','status','owner','site']
GET_PORT_KEY   = ['device_name','port_name','band','openflow_flg']
GET_NIC_KEY    = ['device_name','nic_name','band','mac_address','ip_address','traffic_type']
GET_USED_KEY   = ['device_name','user_name','tenant_name']
GET_TENANT_KEY = ['device_name','tenant_name']
GET_BACKUP_KEY = ['cluster_name','backup_name']

####add##########
if 'ON' == DBG_FLAG:
	LOG_FILENAME = 'logging_ool_rm_if.out'
	
	# Set up a specific logger with our desired output level
	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)
	
	# Add the log message handler to the logger
	handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=20, backupCount=5)
	
	my_logger.addHandler(handler)
####add##########

class ool_rm_if:

	def __init__(self):
		self.auth = ''
		# load configfile
		c_file= '%s/%s' % (PY_PATH, CNF_FILE)

		if 'ON' == DBG_FLAG:
			my_logger.debug('c_file:%s' %(c_file))
		try:
			f = open(c_file, 'r')
		except:
			print 'file open(OOL_RM_IF_CNF) error'
			return

		for line in f:
			if line.find(DB_URL_KEY) != -1:
				self.url_head=line[line.find(DB_URL_KEY)+len(DB_URL_KEY):-1]
			if line.find(DB_PORT_KEY) != -1:
				self.port=line[line.find(DB_PORT_KEY)+len(DB_PORT_KEY):-1]

		if ('' == self.url_head) or ('' == self.port):
			print 'config file error'
			return
		
		self.rm_create = ool_rm_if_createImpl()
		self.rm_create.set_url_head(self.url_head)
		self.rm_create.set_port(self.port)
		self.rm_get = ool_rm_if_getImpl()
		self.rm_get.set_url_head(self.url_head)
		self.rm_get.set_port(self.port)

		self.rm_update = ool_rm_if_updateImpl()
		self.rm_update.set_url_head(self.url_head)
		self.rm_update.set_port(self.port)

		self.rm_delete = ool_rm_if_deleteImpl()
		self.rm_delete .set_url_head(self.url_head)
		self.rm_delete.set_port(self.port)

#--auth
	def set_auth(self, auth):
		self.auth = auth
		self.rm_create.set_authImpl(auth)
		self.rm_get.set_authImpl(auth)
		self.rm_update.set_authImpl(auth)
		self.rm_delete.set_authImpl(auth)

#--create interface
	def set_node_data(self, node_data):
		return self.rm_create.set_node_dataImpl(node_data)
	def set_switch_data(self, switch_data):
		return self.rm_create.set_switch_dataImpl(switch_data)
	def set_port_data(self, device_name, port_data):
		return self.rm_create.set_port_dataImpl(device_name, port_data)
	def set_nic_data(self, device_name, nic_data):
		return self.rm_create.set_nic_dataImpl(device_name, nic_data)
	def set_used_data(self, device_name, used_data):
		return self.rm_create.set_used_dataImpl(device_name, used_data)
	def set_tenant_data(self, tenant_name, tenant_data):
		return self.rm_create.set_tenant_dataImpl(tenant_data, tenant_name=tenant_name)
	def set_tenant_device_data(self, device_name, tenant_data):
		return self.rm_create.set_tenant_dataImpl(tenant_data, device_name=device_name)
	def set_backup_data(self, cluster_name, backup_name, device_list):
		return self.rm_create.set_backup_dataImpl(cluster_name, backup_name, device_list)

#--get interface
	def get_device(self, device_name):
		return self.get_node(device_name)
	def get_node(self, device_name):
		return self.get_node_query(device_name=device_name)
	def get_device_all(self):
		return self.get_node_all()
	def get_node_all(self):
		return self.get_node_query()
	def get_node_query(self, **query):
		return self.rm_get.get_nodeImpl(query)
	def get_switch(self, device_name):
		return self.get_switch_query(device_name=device_name)
	def get_switch_all(self):
		return self.get_switch_query()
	def get_switch_query(self, **query):
		return self.rm_get.get_switchImpl(query)
	def get_port(self, device_name):
		return self.get_port_query(device_name=device_name)
	def get_port_query(self, **query):
		return self.rm_get.get_portImpl(query)
	def get_nic(self, device_name):
		return self.get_nic_query(device_name=device_name)
	def get_nic_traffic_info(self, device_name, traffic_type):
		t_type=''
		for i in range(0, len(PLANE_LIST)):
			if PLANE_LIST[i]==traffic_type:
				t_type='{0:03d}'.format(i+1)
				break
		return self.get_nic_query(device_name=device_name, traffic_type=t_type)
	def get_nic_query(self, **query):
		return self.rm_get.get_nicImpl(query)
	def get_used(self, device_name, user_name):
		return self.get_used_query(device_name=device_name, user_name=user_name)
	def get_used_query(self, **query):
		return self.rm_get.get_usedImpl(query)
	def get_tenant(self, device_name, tenant_name):
		if device_name:
			if tenant_name:
				return self.get_tenant_query(device_name=device_name, tenant_name=tenant_name)
			else:
				return self.get_tenant_query(device_name=device_name)
		else:
			if tenant_name:
				return self.get_tenant_query(tenant_name=tenant_name)
			else:
				return self.get_tenant_query()
	def get_tenant_query(self, **query):
		return self.rm_get.get_tenantImpl(query)
	def get_backup_cluster(self, cluster_name):
		return self.get_backup_query(cluster_name=cluster_name)
	def get_backup_query(self,  **query):
		return self.rm_get.get_backupImpl(query)

#--update interface
	def mod_node_data(self, device_name, node_data):
		return self.rm_update.mod_node_dataImpl(device_name, node_data)

#-- delete interface
	def del_node(self, device_name):
		return self.rm_delete.del_nodeImpl(device_name)
	def del_switch(self, device_name):
		return self.rm_delete.del_switchImpl(device_name)
	def del_port(self, device_name, port_name):
		return self.del_port_query(device_name=device_name, port_name=port_name)
	def del_port_all(self, device_name):
		return self.del_port_query(device_name=device_name)
	def del_port_query(self, **query):
		return self.rm_delete.del_portImpl(query)
	def del_nic(self, device_name, nic_name):
		return self.del_nic_query(device_name=device_name, nic_name=nic_name)
	def del_nic_all(self, device_name):
		return self.del_nic_query(device_name=device_name)
	def del_nic_query(self, **query):
		return self.rm_delete.del_nicImpl(query)
	def del_used(self, device_name):
		return self.rm_delete.del_usedImpl(device_name)
	def del_tenant(self, device_name, tenant_name):
		return self.rm_get.del_tenantImpl(device_name, tenant_name)
	def del_backup(self, cluster_name, backup_name):
		return self.del_backup_query(cluster_name=cluster_name, backup_name=backup_name)
	def del_backup_cluster(self, cluster_name):
		return self.del_backup_query(cluster_name=cluster_name)
	def del_backup_query(self, **query):
		return self.rm_delete.del_backupImpl(query)

#--common
class ool_rm_if_common:
	def __init__(self):
		self.url_head = ''
		self.port     = ''
		self.errmsg   = ''

	def set_url_head(self, url_head):
		self.url_head = url_head

	def set_port(self, port):
		self.port = port

	def http_get(self, url_para):
		return self.__http_read__(url_para)

	def http_post(self, url_para, body):
		return self.__http_write__(url_para, '', body)

	def http_put(self, url_para, body):
		return self.__http_write__(url_para, 'UPDATE', body)

	def url_delete(self, url_para, body):
		return self.__http_write__(url_para, 'DELETE', body)

	def __http_read__(self, url_para):
		try:
			url='%s:%s%s' % (self.url_head, self.port, url_para)
			if 'ON' == STUB_FLAG:
				read_data=self.url_head_read_tst(url)
			else:
				read_data=json.load(urllib2.urlopen(url), encoding='utf8')
				if 'ON' == DBG_FLAG:
					my_logger.debug('read url:%s' %(url))
			return read_data
		except Exception, e:
			self.errmsg=e
			print str(e)
			return -1

	def __http_write__(self, url_para, sel, body):
		try:
			url='%s:%s%s' % (self.url_head, self.port, url_para)
			if 'ON' == STUB_FLAG:
				output = self.url_head_write_tst(url, body)
			else:
				body_data=str(body)
				req = urllib2.Request(url, body_data)
				req.add_header('Content-Type', 'application/json')
				req.add_header('Accept-Charset', 'utf-8')
				if 'UPDATE' == sel:
					req.get_method = lambda: 'PUT'
				if 'DELETE' == sel:
					req.get_method = lambda: 'DELETE'
				response =urllib2.urlopen(req)
				output   =json.load(response, encoding='utf8')
				if 'ON' == DBG_FLAG:
					my_logger.debug('read url:%s' %(url))
			return output
		except Exception, e:
			self.errmsg = e
			print str(e)
			return -1

	def __list2dict__(self, para1):
		if True == isinstance(para1, list):
			dict_tmp=para1[0]
		else:
			dict_tmp=para1
		return dict_tmp

	def __err_chk__(self, err):
		if -1 == err:
			return [-1, self.errmsg]
		
		if ((200 != err['status']) and (201 != err['status'])):
			return [-1, str(err['status'])]

		return [0, 0]

#--create Impl
class ool_rm_if_createImpl(ool_rm_if_common):
	def __init__(self):
		self.auth = ''

	def set_authImpl(self, auth):
		self.auth = auth

	def set_data(self, url_para, body):
		http_write = self.http_post(url_para, body)
		err_status = self.__err_chk__(http_write)
		if -1 == err_status[0]:
			return err_status

		ret=http_write['status']
		return [0, ret]

	def set_dataImpl(self, url_para, body):
		http_write = self.http_post(url_para, body)
		err_status = self.__err_chk__(http_write)
		if -1 == err_status[0]:
			return err_status

		ret=http_write['status']
		return [0, ret]

	def set_node_dataImpl(self, node_data):
		body = {}
		body.update({'auth':self.auth})
		body.update({'params':node_data})

		url_para = '%sNode' % (ROOT_DIR)
		return self.set_data(url_para, body)

	def set_switch_dataImpl(self, switch_data):
		body = {}
		body.update({'auth':self.auth})
		body.update({'params':switch_data})

		url_para = '%sSwitch' % (ROOT_DIR)
		return self.set_data(url_para, body)

	def set_port_dataImpl(self, device_name, port_data):
		body = {}
		body.update({'auth':self.auth})
		body.update({'device_name':str(device_name)})
		body.update({'params':port_data})

		url_para = '%sPort' % (ROOT_DIR)
		return self.set_data(url_para, body)

	def set_nic_dataImpl(self, device_name, nic_data):
		body = {}
		body.update({'auth': self.auth})
		body.update({'device_name': str(device_name)})
		body.update({'params': nic_data})

		url_para = '%sNic' % (ROOT_DIR)
		return self.set_data(url_para, body)

	def set_used_dataImpl(self, device_name, used_data):
		body = {}
		body.update({'auth': self.auth})
		body.update({'device_name': str(device_name)})
		body.update({'params': used_data})

		url_para = '%sUsed' % (ROOT_DIR)
		return self.set_data(url_para, body)

	def set_tenant_dataImpl(self, tenant_data, **key):
		body = {}
		body.update({'auth':self.auth})
		if 'device_name' in key:
			url_para='%sTenant/%s' % (ROOT_DIR, 'Device')
			body.update({'device_name':key['device_name']})
		elif 'tenant_name' in key:
			url_para='%sTenant/%s' % (ROOT_DIR, 'Tenant')
			body.update({'tenant_name':key['tenant_name']})
		body.update({'params':tenant_data})

		return self.set_data(url_para, body)

	def set_backup_dataImpl(self, cluster_name, backup_name, device_list):
		body = {"auth":"","params":{"cluster_name":"","backup_name":"","devices":""}}	
		body['auth']                   = self.auth
		body['params']['cluster_name'] = str(cluster_name)
		body['params']['backup_name']  = str(backup_name)
		
		#for i in range(0, len(device_list)):
		#	device_list[i] = str(device_list[i])
		body['params']['devices'] = device_list

		url_para = '%sBackup' % (ROOT_DIR)
		return self.set_data(url_para, body)

#--get Impl
class ool_rm_if_getImpl(ool_rm_if_common):
	def __init__(self):
		self.auth = ''

	def set_authImpl(self, auth):
		self.auth = auth

	def get_data(self, url_para):
		http_read=self.http_get(url_para)
		err_status = self.__err_chk__(http_read)
		if -1 == err_status[0]:
			return err_status

		ret =  http_read['result']
		return [0, ret]

	def get_node_filter(self, data):
		url_para='%sNode?' %(ROOT_DIR)
		for key_str in GET_NODE_KEY:
			if key_str in data:
				url_para = url_para + '%s=%s&' % (key_str, data[key_str])
		url_para = url_para + 'auth=%s' % (self.auth)

		dev_info = self.get_data(url_para)
		return dev_info

	def get_switch_filter(self, data):
		url_para = '%sSwitch?' %(ROOT_DIR)
		for key_str in GET_SWITCH_KEY:
			if key_str in data:
				url_para=url_para + '%s=%s&' % (key_str, data[key_str])
		url_para=url_para + 'auth=%s' % (self.auth)

		sw_info = self.get_data(url_para)
		return sw_info

	def get_port_filter(self, data):
		url_para='%sPort?' %(ROOT_DIR)
		REQUIRED_KEY = 'device_name'
		get_flag=0
		for key_str in GET_PORT_KEY:
			if key_str in data:
				url_para = url_para + '%s=%s&' % (key_str, data[key_str])
			if key_str == REQUIRED_KEY:
				get_flag = 1

		if 0 == get_flag:
			return [-1, ERR_MSG_DEVICE]
		url_para = url_para + 'auth=%s' % (self.auth)

		port_info = self.get_data(url_para)
		return port_info

	def get_nic_filter(self, data):
		url_para = '%sNic?' %(ROOT_DIR)
		REQUIRED_KEY = 'device_name'
		get_flag = 0
		for key_str in GET_NIC_KEY:
			if key_str in data:
				url_para=url_para + '%s=%s&' % (key_str, data[key_str])
			if key_str == REQUIRED_KEY:
				get_flag = 1

		if 0 == get_flag:
			return [-1, ERR_MSG_DEVICE]
		url_para = url_para + 'auth=%s' % (self.auth)

		nic_info = self.get_data(url_para)
		return nic_info

	def get_used_filter(self, data):
		url_para = '%sUsed?' %(ROOT_DIR)
		for key_str in GET_USED_KEY:
			if key_str in data:
				url_para=url_para + '%s=%s&' % (key_str, data[key_str])
		url_para = url_para + 'auth=%s' % (self.auth)

		used_info = self.get_data(url_para)
		return used_info

	def get_tenant_filter(self, data):
		url_para='%sTenant?' %(ROOT_DIR)
		for key_str in GET_TENANT_KEY:
			if key_str in data:
				url_para = url_para + '%s=%s&' % (key_str, data[key_str])
		url_para = url_para + 'auth=%s' % (self.auth)

		tenant_info = self.get_data(url_para)
		return tenant_info

	def get_backup_filter(self, data):
		url_para = '%sBackup?' %(ROOT_DIR)
		for key_str in GET_BACKUP_KEY:
			if key_str in data:
				url_para = url_para + '%s=%s&' % (key_str, data[key_str])
		url_para = url_para + 'auth=%s' % (self.auth)

		bk_info = self.get_data(url_para)
		return bk_info

#--get Impl
	def get_nodeImpl(self, key):
		para = {}
		for key_str in GET_NODE_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		dev_info = self.get_node_filter(para)
		if -1 == dev_info[0]:
			return [dev_info[0],dev_info[1]]
		else:
			if 0 == len(para):
				dev_data = []
				dev_info_wk = dev_info[1]
				for i in range(0,len(dev_info_wk)):
					dev_wk = self.__list2dict__(dev_info_wk[i])
					dev_data.append(dev_wk['device_name'])
			else:
				dev_data = self.__list2dict__(dev_info[1])
			return [0, dev_data]

	def get_switchImpl(self, key):
		para = {}
		for key_str in GET_SWITCH_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		sw_info = self.get_switch_filter(para)
		if -1 == sw_info[0]:
			return [sw_info[0], sw_info[1]]
		else:
			if 0 == len(para):
				sw_data = []
				sw_info_wk = sw_info[1]
				for i in range(0, len(sw_info_wk)):
					sw_wk=self.__list2dict__(sw_info_wk[i])
					sw_data.append(sw_wk['device_name'])
					if 'vender_name' in sw_wk:
						sw_data.append(sw_wk['vender_name'])
					else:
						sw_data.append('')
			else:
				sw_data = self.__list2dict__(sw_info[1])
		return [0, sw_data]

	def get_portImpl(self,  key):
		para = {}
		for key_str in GET_PORT_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		port_info = self.get_port_filter(para)
		return port_info

	def get_nicImpl(self, key):
		para = {}
		for key_str in GET_NIC_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		nic_info = self.get_nic_filter(para)
		return nic_info

	def get_usedImpl(self,  key):
		para = {}
		for key_str in GET_USED_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		used_info = self.get_used_filter(para)
		return used_info

	def get_tenantImpl(self, key):
		para = {}
		for key_str in GET_TENANT_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		tenant_info = self.get_tenant_filter(para)
		return tenant_info

	def get_backupImpl(self, key):
		para = {}
		for key_str in GET_BACKUP_KEY:
			if key_str in key:
				para.update({key_str:key[key_str]})

		backup_info = self.get_backup_filter(para)
		return backup_info

#--update Impl
class ool_rm_if_updateImpl(ool_rm_if_common):
	def __init_(self):
		self.auth = ''
		
	def set_authImpl(self, auth):
		self.auth = auth

	def update_data(self, url_para, data):
		url_update = self.http_put(url_para, data)
		err_status = self.__err_chk__(url_update)
		if -1 == err_status[0]:
			return err_status

		ret=url_update['status']
		return [0, ret]

	def mod_node_dataImpl(self, device_name, node_data):
		body = {}
		body.update({'auth': str(self.auth)})
		body.update({'device_name': str(device_name)})
		body.update({'params': node_data})

		url_para = '%sNode' % ROOT_DIR
		return self.update_data(url_para, body)

#--delete Impl
class ool_rm_if_deleteImpl(ool_rm_if_common):
	def __iniit__(self):
		self.auth = ''

	def set_authImpl(self, auth):
		self.auth = auth

	def del_data(self, url_para, data):
		url_del=self.url_delete(url_para, data)
		err_status = self.__err_chk__(url_del)
		if -1 == err_status[0]:
			return err_status

		ret=url_del['status']
		return [0, ret]

	def del_cmn_filter(self, url_para, data):
		if "device_name" in data:
			url_para = url_para + 'device_name=%s&' % (data['device_name'])
		else:
			return [-1, ERR_MSG_DEVICE]
		url_para = url_para + 'auth=%s' % (self.auth)
		ret=self.del_data(url_para, '')
		return ret
	
	def del_node_filter(self, data):
		url_para = '%sNode?' %(ROOT_DIR)
		return self.del_cmn_filter(url_para, data)

	def del_switch_filter(self, data):
		url_para = '%sSwitch?' % (ROOT_DIR)
		return self.del_cmn_filter(url_para, data)

	def del_port_filter(self, data):
		url_para = '%sPort?' % (ROOT_DIR)
		if "device_name" in data:
			url_para = url_para + 'device_name=%s&' % (data['device_name'])
		else:
			return [-1, ERR_MSG_DEVICE]
		if "port_name" in data:
			url_para = url_para + 'port_name=%s&' % (data['port_name'])

		url_para = url_para + 'auth=%s' % (self.auth)
		ret = self.del_data(url_para, '')
		return ret

	def del_nic_filter(self, data):
		url_para = '%sNic?' % (ROOT_DIR)
		if "device_name" in data:
			url_para = url_para + 'device_name=%s&' % (data['device_name'])
		else:
			return [-1, ERR_MSG_DEVICE]
		if "nic_name" in data:
			url_para = url_para + 'nic_name=%s&' % (data['nic_name'])

		url_para = url_para + 'auth=%s' % (self.auth)
		ret = self.del_data(url_para, '')
		return ret

	def del_used_filter(self, data):
		url_para = '%sUsed?' % (ROOT_DIR)
		return self.del_cmn_filter(url_para, data)

	def del_tenant_filter(self, data):
		url_para = '%sTenant?' % (ROOT_DIR)
		if "device_name" in data:
			url_para = url_para + 'device_name=%s&' % (data['device_name'])
		else:
			if "tenant_name" in data:
				url_para = url_para + 'tenant_name=%s&' % (data['tenant_name'])
			else:
				return [-1, ERR_MSG_TENNANT]

		url_para = url_para + 'auth=%s' % (self.auth)
		ret=self.del_data(url_para, '')
		return ret

	def del_backup_filter(self, data):
		url_para = '%sBackup?' % (ROOT_DIR)
		if "cluster_name" in data:
			url_para = url_para + 'cluster_name=%s&' % (data['cluster_name'])
		else:
			return [-1, ERR_MSG_CLUSTER]
		if "backup_name" in data:
			url_para = url_para + 'backup_name=%s&' % (data['backup_name'])

		url_para = url_para + 'auth=%s' % (self.auth)
		ret=self.del_data(url_para, '')
		return ret

#------
	def del_nodeImpl(self, device_name):
		para = {}
		if '' != device_name:
			para.update({'device_name':device_name})
		else:
			return[-1, ERR_MSG_DEVICE]

		return self.del_node_filter(para)

	def del_switchImpl(self, device_name):
		para = {}
		para.update({'device_name':device_name})
		return self.del_switch_filter(para)

	def del_portImpl(self, key):
		para = {}
		if 'device_name' in key:
			para.update({'device_name':key['device_name']})
		else:
			return[-1, ERR_MSG_DEVICE]

		if 'port_name' in key:
			para.update({'port_name':key['port_name']})
		return self.del_port_filter(para)

	def del_nicImpl(self, key):
		para = {}
		if 'device_name' in key:
			para.update({'device_name':key['device_name']})
		else:
			return[-1, ERR_MSG_DEVICE]

		if 'nic_name' in key:
			para.update({'nic_name':key['nic_name']})
		return self.del_nic_filter(para)

	def del_usedImpl(self, device_name):
		para = {}
		if '' != device_name:
			para.update({'device_name':device_name})
		else:
			return[-1, ERR_MSG_DEVICE]
		return self.del_used_filter(para)

	def del_tenantImpl(self, device_name, tenant_name):
		para = {}
		if '' != device_name:
			para.update({'device_name':device_name})
		if '' != tenant_name:
			para.update({'tenant_name':tenant_name})
		return self.del_tenant_filter(para)

	def del_backupImpl(self, key):
		para = {}
		if 'cluster_name' in key:
			para.update({'cluster_name':key['cluster_name']})
		else:
			return [-1, ERR_MSG_CLUSTER]

		if 'backup_name' in key:
			para.update({'backup_name':key['backup_name']})
		return self.del_backup_filter(para)

#--test code
class ool_rm_if_tst:
	def __init__(self):
		return

	def http_read_tst(self, url):
		wk = str(url)

		data={}
		if -1 != wk.find(ROOT_DIR + 'Node?auth='):
			f = open(HOME_DIR + "test_data/dev_cnf_all.txt", 'r')
			data = json.load(f)
			f.close()
			return data
		else:
			if -1 !=  wk.find(ROOT_DIR + 'Node?device_name='):
				r_fname = '%stest_data/dev_cnf_%s.txt' % (HOME_DIR, wk[wk.rfind('_name=')+6:wk.find('&')])
				f = open(r_fname, 'r')
				data = json.load(f)
				f.close()
				return data

		if -1 != wk.find(ROOT_DIR + 'Switch?auth='):
			f = open(HOME_DIR + "test_data/sw_cnf_all.txt", 'r')
			data = json.load(f)
			f.close()
			return data
		else:
			if -1 !=  wk.find(ROOT_DIR + 'Switch?device_name='):
				r_fname = '%stest_data/sw_cnf_%s.txt' % (HOME_DIR, wk[wk.rfind('_name=')+6:wk.find('&')])
				f = open(r_fname, 'r')
				data = json.load(f)
				f.close()
				return data

		if -1 != wk.find(ROOT_DIR + 'Port?'):
			f = open(HOME_DIR + "test_data/port_cnf.txt", 'r')
			data = json.load(f)
			f.close()
			return data

		if -1 != wk.find(ROOT_DIR + 'Nic?traffic_type='):
			r_fname = '%stest_data/nic_cnf_%s.txt' % (HOME_DIR, wk[wk.rfind('_type=')+6:wk.find('&')])
			f = open(r_fname, 'r')
			data = json.load(f)
			f.close()
			return data
		else:
			if -1 != wk.find(ROOT_DIR + 'Nic?'):
				f = open(HOME_DIR + "test_data/nic_cnf.txt", 'r')
				data = json.load(f)
				f.close()
				return data

		if -1 != wk.find(ROOT_DIR + 'Backup?'):
			f = open(HOME_DIR + "test_data/backup_cnf.txt", 'r')
			data = json.load(f)
			f.close()
			return data

	def http_write_tst(self, url, body):
		wk = str(url)

		if -1 != wk.find(ROOT_DIR + 'Backup?auth='):
			f = open(HOME_DIR + "test_data/backup_wdata.txt", 'w')
			f.write(body)
			f.close()
			return {"result":[''],"status":200}
