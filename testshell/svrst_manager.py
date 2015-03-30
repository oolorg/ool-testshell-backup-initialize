#!/usr/bin/env python
# coding: utf-8
import svbk_manager
import psbk_manager
import svbk_conflict
import svbk_utls
import traceback
import time
import logging
import os
import ConfigParser
import ool_rm_if
import sqlite3
import sys
import pexpect
import re
import pxssh
from retry import retry
import fuel_utls
import ipmi_client

#disk size
LIMIT_DISK_SIZE_G=1
INTERVAL_TIME=10
ROOP_TIME_OUT=60

SET_CONFIG_FILE = 'settings.ini'
OP_CONFIG_PATH='/etc/opencenter'
OP_CONFIG_FILE='opencenter.conf'

KEY_SW='availablesw'
KEY_PARENT='parent_id'

NAME_INDEX	=0
IP_INDEX	=1
USER_INDEX	=2
PW_INDEX	=3

STORAGE_SV	=0
START_INDEX	=1

R_END_STATUS="restore_ok"
R_NG_STATUS="restore_ng"

#FILEDIR="/etc/backuprestore"
FILEDIR="/etc/ool_br_rest"
BASE_DIR_NAME="/backup"
MASTER_ENV='BASE'
RESET_ENV='INITIALIZE'
INIT_NODE=0
#TODO
STOP_NODE = 999

OK=0
NG=-1

SV_NAME=0
SW_NAME=1

#DEBUG="ON"
DEBUG="OFF"

R_STOP_FILENAME="/r_stop"
MODE_BACKUP = "backup"
MODE_RESTORE= "restore"
MODE_NONE= "none"

OCCHEF='occhef'

KNIFE='/opt/chef-server/bin/knife'

TIMEOUT=30
WAIT_CNT=10
RETRY_CNT=5
CONSOLE_OUT_FLAG='ON'
EXPECT_STDPT='.*$'
EXPECT_LOGIN='login:'
EXPECT_PASSWD='password:'
EXPECT_YESNO='.*\(yes/no\)\?'
SEND_EXIT='exit'
SEND_YES='yes'

DEFAULT_OS='Ubuntu_12.04LTS'
# Fuel restore image name
FUEL_IMG = 'fuel511-img'
FUEL6_IMG = 'fuel600-img'
UBUNTU12_IMG = 'ubuntu12-img'

# Server Type
OPENORION_AGENT = '001'
FUEL_SERVER = '002'
FUEL_AGENT = '003'
FUEL6_SERVER = '004'
NON_ASSIGNMENT = '999'

# put
OPENORION_HOST = '172.16.1.51'
HTTP_PORT = '58080'

PUT_CMD = 'PUT'
PUT_HEADER = "'Content-type: application/json'"

BASE_SRV_URL = "http://%s:%s/switches"
QUERY_PARA   = ""
SRV_URL = BASE_SRV_URL + QUERY_PARA

#PUT_DATA = '-d "{"switches":%s}"'
PUT_DATA = '"{"switches":%s}"'

# curl [opt] [hdr] [url] [data]
CURL_CMD = "'curl -X %s -H %s '%s' %s'"

class runError(Exception):
    def __init__(self, value):
        self.msg = value
    def __str__(self):
        return repr(self.msg)

#---------------------------------------------------------
class svrst_manager():
	def __init__(self):
		logger = logging.getLogger('rstlog')
		logger.setLevel(logging.DEBUG)
#		handler = logging.handlers.SysLogHandler(address = '/dev/log')
		handler = logging.handlers.WatchedFileHandler('/var/log/testbed/ool-brst.log')
		handler.setFormatter(logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(process)d [%(levelname)s] [-] %(message)s'))
		logger.addHandler(handler)
		self.logObj=logger

		self.svbkm = svbk_manager.svbk_manager(self.logObj)
		self.svbkm.set_log_Title()
		self.svbkc = svbk_conflict.svbk_conflict()
		self.ori   = ool_rm_if.ool_rm_if()
		self.Token =''
		self.node_list=[]
		self.server_list=[]
		self.switch_list=[]
		self.limit_disk_size=LIMIT_DISK_SIZE_G
		self.interval_time=INTERVAL_TIME
		self.roop_timeout_m=ROOP_TIME_OUT
		self.opencenter_server_name=""
		self.storage_server_name=""
		self.topology_name=''
		self.tenant_name=''
		self.connector=''

	def set_node_list(self, node_list):
		svbkutl = svbk_utls.svbk_utls()
		svbkutl.set_auth(self.Token)
		tmp_node_list = svbkutl.separate_node(node_list)

		if ((-1 == tmp_node_list[SV_NAME]) or (-1 == tmp_node_list[SW_NAME])):
			return -1
		
		self.node_list = node_list
		self.server_list = tmp_node_list[SV_NAME]
		self.switch_list = tmp_node_list[SW_NAME]
		return 0
	
	def set_Token(self, Token):
		self.Token=Token

	def set_topology_name(self, topology_name):
		self.topology_name = topology_name

	def set_tenant_name(self, tenant_name):
		self.tenant_name = tenant_name

	def set_logging(self, logging):
		self.logObj=logging

	def _reset_precheck(self, br_mode, cluster_name):
		# set conflict info
		node_list=','.join(self.server_list)
		node_list='%s,%s' % (node_list, ','.join(self.switch_list))

		ret = self.svbkc.chk_mode_state(br_mode, cluster_name, node_list)
		return ret

	def _open_sql(self):
		conf = ConfigParser.SafeConfigParser()
		set_file_path='%s/%s' % (OP_CONFIG_PATH, OP_CONFIG_FILE)

		ret=conf.read(set_file_path)
		if len(ret)==0 :
			return -1

		db_path= conf.get('main', 'database_uri')
		db_path= db_path.replace('sqlite:///', '')

		self.connector = sqlite3.connect(db_path)
		return 0

	def _close_sql(self):
		self.connector.close()

	def _show_sql(self, query):
		cursor = self.connector.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		cursor.close()
		return result

	def _update_sql(self, query):
		try:
			cursor = self.connector.cursor()
			cursor.execute(query)
		except:
			self.connector.rollback()
			cursor.close()
			return -1
		finally:
			self.connector.commit()
			cursor.close()

		return 0

	def _restart_opencenter_agent(self, ip, uname, upw):
		
		for i in range(1,WAIT_CNT):
			c = pexpect.spawn('ssh -l %s %s' % (uname, ip),  timeout=TIMEOUT)
			if 'ON' == CONSOLE_OUT_FLAG:
				c.logfile=sys.stdout

		i = c.expect([EXPECT_YESNO, EXPECT_PASSWD])
		if 0==i:
			c.sendline(SEND_YES)
			c.expect(EXPECT_PASSWD)
			c.sendline(upw)
		if 1==i:
			c.sendline(upw)

		c.expect(EXPECT_STDPT,  timeout=TIMEOUT)
		c.sendline('sudo service opencenter-agent restart')

		c.expect('.*password for .*',  timeout=TIMEOUT)
		c.sendline(upw)

		c.expect(EXPECT_STDPT,  timeout=TIMEOUT)
		c.sendline(SEND_EXIT)

		c.expect(pexpect.EOF)
		c.close()

	def set_system_param(self, clster_name, node_id, br_mode):

		conf = ConfigParser.SafeConfigParser()

		set_file_path = '%s/%s' % (FILEDIR, SET_CONFIG_FILE)

		ret = conf.read(set_file_path)

		if len(ret) == 0:
			self.svbkm.br_log(node_id, clster_name, br_mode, '####set_system_param ng file is nothing ')
			return NG

		self.svbkm.br_log(node_id, clster_name, br_mode, '####set_system_param file_name :%s' % (ret[0]))

		self.limit_disk_size = int(conf.get('options', 'limit_disk_size'))
		self.interval_time = int(conf.get('options', 'interval_time'))
		self.opencenter_server_name = conf.get('options', 'opencenter_server_name')
		self.storage_server_name = conf.get('options', 'storage_server_name')
		self.clonezilla_server_name = conf.get('options', 'clonezilla_server_name')
		self.loop_timeout_m = int(conf.get('options', 'loop_timeout_m'))

		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file limit_disk_size :%s' % (self.limit_disk_size))
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file interval_time :%s' % (self.interval_time))
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file opencenter_server_name :%s' % (self.opencenter_server_name))
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file storage_server_name :%s' % (self.storage_server_name))
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file clonezilla_server_name :%s' % (self.clonezilla_server_name))
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file loop_timeout_m :%s' % (self.loop_timeout_m))

		return OK

	def reset_cluster(self, **kwargs):
		try:
			br_mode = "r"
			node_id = INIT_NODE
			CLSTER_NAME = self.topology_name

			#####################
			#set prefix log title
			#####################
			self.svbkm.set_log_Title('TS_%s_%s_' % (self.tenant_name, CLSTER_NAME))

			##################
			#make file LogName
			##################
			self.svbkm.make_log_file_name(CLSTER_NAME, node_id, br_mode, restore_name="reset")

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start(reset_cluster)')

			ret = self._reset_precheck(br_mode, CLSTER_NAME)

			if 1 == ret:
				return ['NG', '#### reset already running']
			elif -1 == ret:
				return ['NG', '#### While backup is running, can not reset']
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK(reset_cluster) ')

			####################
			# Check Servertype #
			####################
			self.ori.set_auth(self.Token)
			data = []
			openorion_num = 0
			fuel_num = 0
			
			for server_name in self.server_list:
				data_work = self.ori.get_node(server_name)
				if -1 != data_work[0]:
					data.append(data_work[1])
					if data_work[1]["server_type"] == OPENORION_AGENT:
						openorion_num += 1
					elif data_work[1]["server_type"] == FUEL_SERVER or \
						data_work[1]["server_type"] == FUEL6_SERVER or \
						data_work[1]["server_type"] == FUEL_AGENT:
						fuel_num += 1
				else:
					print "get node info Error"

			print data

			# Fuel / OpenOrion miexd check
			if openorion_num > 0 and fuel_num > 0:
				print "enviroment server_type setting error"
				retArray = ['NG', "Error"]
			else:
				###############
				####Restore####
				###############
				if openorion_num > 0:
					retArray = self.restore_osimage(data)
				elif fuel_num > 0:
					retArray = self.setup_fuelserver(data)
				else:
					retArray = self.restore_osimage(data)
					#retArray = ['OK', "Success"]

			#set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			return retArray

		except Exception, e:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :' + str(e))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' % (traceback.format_exc()))

			#set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			raise

	#####################
	#Reset Module
	#####################
	def reset_cluster_sub(self, **kwargs):

		#####################
		#set predefine 
		#####################
		br_mode = "r"
		CLSTER_NAME = self.topology_name
		node_id = kwargs['node_id']
		target_os = kwargs['target_os']

		if '' == target_os:
			target_os = DEFAULT_OS

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Reset Start ')

		#get restore folder name
		BACKUP_FOLDER_RNAME="%s/%s/%s" % (BASE_DIR_NAME, RESET_ENV, MASTER_ENV)

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'reset_name: %s' % BACKUP_FOLDER_RNAME)

		ret=self.svbkm.set_system_param(CLSTER_NAME,node_id,br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
			return ['NG', '#### set_system_param err']

		ret=self.set_system_param(CLSTER_NAME,node_id,br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
			return ['NG', '#### set_system_param err']

		#####################
		#set define
		#####################
		SAVE_DIR_NAME="%s/server" % (BACKUP_FOLDER_RNAME)
		SAVE_DIR_NAME_SWITCH="%s/switch" % (BACKUP_FOLDER_RNAME)

#		NODE_INFO_FILE_NAME="node_info"
#		NODE_LIST_FILE="%s/%s" % (BACKUP_FOLDER_RNAME, NODE_INFO_FILE_NAME)

		##################
		#set token
		##################
		ret = self.svbkm.set_token_value(CLSTER_NAME, node_id, br_mode, self.Token)

		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '###get token err')
			return ['NG', '###get token err']

		#start time get
		time1 = time.time()

		######################
		#Make Server info list
		######################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

		#make storage server save val
		server_cnt=1
		server_info_num=4
		server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

		########################
		#Set Storage Server info
		########################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

		# Network expansion  
		#retdata = self.svbkm.set_server_info_SPlane(node_id, server_info[0], self.storage_server_name, CLSTER_NAME, br_mode)
		retdata = self.svbkm.set_server_info(node_id, server_info[0], self.storage_server_name, CLSTER_NAME, br_mode)

		if 0 != retdata[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
			return ['NG', 'server info set err']

		################
		#Set Exec_User
		################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
		ret = self.svbkm.get_user_name(self.opencenter_server_name)
		if 0 == ret[0]:
			EXEC_USER=ret[1]
		else:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
			msg='Set Exec_User error'
			return ['NG', msg]

		#####################################
		#Set Storage Server info
		#####################################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

		i=STORAGE_SV
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'Reset exec start  index=%d' %( i))
		ret=self.svbkm.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
		if ret!=0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Reset exec start err index=%d' %( i) )
			return ['NG', 'make_exec err']

		########################
		#Get server name for opencenterDB or parameter
		#######################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for storage server')

		server_node_name=self.server_list
		server_num=len(server_node_name)
		server_cnt=server_num+1

		tmp_server_info = server_info[STORAGE_SV]
		###################
		#resize server info
		###################
		server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]
		server_info[STORAGE_SV] = tmp_server_info

		switch_node_name = self.switch_list
		switch_num=len(switch_node_name)
		switch_node_name_char=','.join(switch_node_name)

		if (0 == server_num) and ( 0 == switch_num):
			#self.logger.debug('sever num = 0 then not backup')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
			return ['OK', 'success']

		########################
		#Set Topology info
		########################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Topology info')

		for i in range(1, server_cnt):
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )

			#retdata = self.svbkm.set_server_info_CPlane(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
			retdata = self.svbkm.set_server_info(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
			if 0 != retdata[0]:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )

				msg='Set Server info set server_node_name=%s' %(server_node_name[i-1])
				return ['NG', msg]

		#####################################
		#Set Topology Make Exec Env
		#####################################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Topology Make Exec Env')

		for i in range(1, server_cnt):
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start all=%d index=%d' %(server_cnt, i))
			ret=self.svbkm.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
			if ret != 0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err all=%d index=%d' %(server_cnt, i) )

				return ['NG', 'make_exec err']

		#######################
		#Check Directory
		#######################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

		cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
		ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
		if ret!=0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory Err %s is no directory' %(SAVE_DIR_NAME))
			#return self._fail(msg='Check Directory ')
			return ['NG', 'Check Directory ']

		#DDD debug
		if DEBUG == "ON":
			switch_num=0
		#################
		#switch restore
		#################
		if switch_num != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Switch reset Start')

			psbk=psbk_manager.psbk_manager(EXEC_USER, self.storage_server_name, SAVE_DIR_NAME_SWITCH,self.logObj)

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH Call psbk.set_PS_list')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_name_char))

			ret=psbk.set_PS_list(switch_node_name_char)
			if 0 != ret:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH psbk.set_PS_list Err')

				return ['NG', '#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)']

			psbk.set_auth(self.Token)
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
			ret=psbk.exec_restore()
			if 0 != ret:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
				return ['NG', '####SWITCH  psbk.exec_restore() ']

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch restore End')

		#DDD
		if DEBUG == "ON":
			server_num = 0

		if server_num == 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
			return ['OK', 'success']

		#######################
		#Dell Restore Status File
		#######################
		cmd='ssh root@%s rm -rf %s/status_r_*  2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME)
		ret = self.svbkm.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Dell Restore Status File Err ')
			
			return ['NG', 'Dell Restore Status File Err ']

		################
		#Run backup
		################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Run backup')

		svbkutl=svbk_utls.svbk_utls()
		svbkutl.set_auth(self.Token)

		for i in range(START_INDEX, server_cnt):
			# update_br_agent
			ret =svbkutl.update_br_agent(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
			if NG == ret[0]:
				msg=' update_br_agent reset [%s] err' % server_info[i][IP_INDEX]
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### %s' % msg)
				return ['NG', msg]

			#copy br.org_update for fast
			ret = svbkutl.update_br_org(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
			if NG == ret[0]:
				msg='copy br.org_update [%s] err ' % (server_info[i][IP_INDEX])
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, msg )
				return ['NG', msg]

			# 誤ってlocalhostのNICの情報を取得しているようなのでコメントアウトする
			# vvv ----------
			nic_rule = ''
			"""
			# set nic rule
			# get mac_address from resourcedb
			ret = svbkutl.get_macaddr(self.server_list[i-1])

			if NG == ret[0]:
				msg='get mac_address on DB [%s] err ' % (self.server_list[i-1])
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, msg )
				return ['NG', msg]

			nic_name=ret[1]
			nic_mac_addr=ret[2]

			# cnt nic
			ret = svbkutl.shellcmd_exec_localhost('ifconfig -a|grep eth|wc -l')
			if NG == ret[0]:
				msg='get cnt nic err '
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, msg )
				return ['NG', msg]

#			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### nic_cnt=%s' % ret[1])

			nic_cnt=ret[1]
			own_mac_addr=[]
			own_bus_info=[]
			for j in range(0, int(nic_cnt)):
				ret = svbkutl.shellcmd_exec_localhost('ethtool -P eth%s' % str(j))
				if NG == ret[0]:
					msg='get mac_address [eth%s] err ' % str(j)
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, msg )
					return ['NG', msg]

				patern=re.compile('([a-f0-9][a-f0-9]:)+[a-f0-9][a-f0-9]')
				match=patern.search(ret[1])
				own_mac_addr.append(match.group(0))

#				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### own_mac_addr=%s' % own_mac_addr[j])

				ret = svbkutl.shellcmd_exec_localhost('ethtool -i eth%s' % str(j))
				if NG == ret[0]:
					msg='get bus_info [eth%s] err ' % str(j)
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, msg )
					return ['NG', msg]

				patern=re.compile('[0-9][0-9]:[0-9][0-9]\.[0-9]')
				match=patern.search(ret[1])
				own_bus_info.append(match.group(0))

#				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### own_bus_info=%s' % own_bus_info[j])

			# 
			nic_rule=''
			for k in range(0, int(nic_cnt)):
				for m in range(0, len(nic_name)):
					if own_mac_addr[k] == nic_mac_addr[m]:
						if '' == nic_rule:
							nic_rule= '%s,%s' % (nic_name[m],own_bus_info[k])
						else:
							nic_rule= '%s,%s,%s' % (nic_rule, nic_name[m],own_bus_info[k])
						break;
					else:
						if '' == nic_rule:
							nic_rule= 'eth%s,%s' % (k,own_bus_info[k])
						else:
							nic_rule= '%s,eth%s,%s' % (nic_rule, k,own_bus_info[k])
						break;
			"""
			# ^^^----------

			nic_rule=nic_rule + ",end,end"
			cmd='ssh root@%s /boot/%s %s %s r %s %s %s %s %s %s' % (server_info[i][IP_INDEX], svbkutl.get_br_agent_name(), server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME, server_info[STORAGE_SV][IP_INDEX], server_info[STORAGE_SV][USER_INDEX],  server_info[STORAGE_SV][PW_INDEX], target_os, nic_rule)

			ret = self.svbkm.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
			if ret != 0:
				msg=' make run reset [%s] err' % server_info[i][IP_INDEX]
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### %s' % msg)
				return ['NG', msg]

		################
		#Start status check
		################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

		while 1:
			all_ret=0

			################
			#time cal
			################

			#end time get
			time2 = time.time()
			timedf = time2-time1

			timedf_int = int(timedf)
			HH= timedf_int / 3600
			SS= timedf_int % 3600
			MM= timedf_int / 60
			SS= timedf_int % 60

			#logout
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, "#### Loop  status check Total Time: %s:%s:%s (h:m:s)" %(HH,MM,SS) )

			for i in range(START_INDEX, server_cnt):

				################
				#status loop check
				################
				cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_END_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

				ret = self.svbkm.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'server_info[%s] IP=%s SV_NAME=%s ' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )

				all_ret = ret + all_ret

				################
				#status view
				################
				cmd='ssh root@%s tail -n 1 %s/status_r_%s  2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][NAME_INDEX])

				ret = self.svbkm.shellcmd_exec_br_state(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

				################
				#status ng check
				################
				cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_NG_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

				ret = self.svbkm.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
				if ret==0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Status Loop check [backup_ng] Err IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
					#return self._fail(msg='Status loop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
					msg='Status loop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX])
					return ['NG', msg]

				time.sleep(self.interval_time )

			if all_ret==0:
				break

			#force-stop
			if os.path.exists(FILEDIR+R_STOP_FILENAME) :
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### force stop')
				os.remove(FILEDIR+R_STOP_FILENAME)
				break

			#time-out
			if MM > self.loop_timeout_m :
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### MM is %s min Over stop' %(self.loop_timeout_m))
				break

		self.ori.set_auth(self.Token)
		#####################################
		#get Info of Open Orion 
		#####################################
		data= self.ori.get_device(self.opencenter_server_name)
		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set openorion get_device err')
			return ['NG', 'Set openorion get_device err']

		dev_data={}
		dev_data=data[1]
		OPEN_ORION_user=dev_data['user_name']
		OPEN_ORION_upw=dev_data['password']
		
		#get IP address
		data=self.ori.get_nic_traffic_info(self.opencenter_server_name, 'C-Plane')

		if -1 != data[0]:
			data1={}
			data1=data[1][0]
			OPEN_ORION=data1['ip_address']
		else:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, "get ip_address error:%s" %(data[1]))
			return ['NG', 'get ip_address error']

		#####################################
		#Set chef Make Exec Env
		#####################################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef Make Exec Env')

		data= self.ori.get_device(OCCHEF)
		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef get_device err')
			return ['NG', 'Set chef get_device err']

		dev_data={}
		dev_data=data[1]
		chef_user=dev_data['user_name']
		chef_pw  =dev_data['password']

		data = self.ori.get_nic_traffic_info(OCCHEF, 'M-Plane')
		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef get_nic_traffic err')
			return ['NG', 'Set chef get_nic_traffic err']
		
		nic_data={}
		nic_data=data[1][0]
		chef_ip= nic_data['ip_address']
		
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'Set chef Make Exec')
		ret=self.svbkm.make_exec(chef_user, chef_ip, chef_user, chef_pw, FILEDIR, br_mode, node_id, CLSTER_NAME)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef Make Exec err')
			return ['NG', 'Set chef make_exec err']

		if (0 != server_num) :
			############################
			#Delete node from OpenOrion
			############################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node')
	
			end_point =  'https://admin:password@%s:8443/' % (OPEN_ORION)
	
			for i in range(START_INDEX, server_cnt):
				cmd = 'opencentercli node delete %s --endpoint %s 2> /dev/null' % (server_info[i][NAME_INDEX], end_point)
	
				ret = self.svbkm.shellcmd_exec(OPEN_ORION_user, br_mode, node_id, CLSTER_NAME, cmd)
				if ret!=0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node err %s' %(server_info[i][NAME_INDEX]))
#					return ['NG', 'Delete node err %s' %(self.server_list[i])]
	
			############################
			#Delete info from chef
			############################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete info from chef')
	
			for i in range(START_INDEX, server_cnt):
				cmd = 'ssh root@%s %s node delete -y %s 2> /dev/null' % (chef_ip, KNIFE, server_info[i][NAME_INDEX])
	
				ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
				if ret!=0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node from chef err (%s)' %(server_info[i][NAME_INDEX]))
#					return ['NG', 'Delete node from chef err (%s)' %(server_info[i][NAME_INDEX])]
			
				cmd = 'ssh root@%s %s client delete -y %s 2> /dev/null' % (chef_ip, KNIFE, server_info[i][NAME_INDEX])
	
				ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
				if ret!=0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete client from chef err (%s)' %(server_info[i][NAME_INDEX]))
#					return ['NG', 'Delete client from chef err (%s)' %(server_info[i][NAME_INDEX])]
	
			###############################
			#setup opencenter-agent to node
			###############################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent')
	
			for i in range(START_INDEX, server_cnt):
				while 1:
					ret=self.svbkm.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
					if ret != 0:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### rsa key copy all=%d index=%d' %(server_cnt, i) )

					cmd = 'ls'
					cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)
					ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
					if ret!=0:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### chk boot-up  NG(%s)' %(server_info[i][NAME_INDEX]))
					else:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### chk boot-up  OK(%s)' %(server_info[i][NAME_INDEX]))
						break

				cmd = '/bin/rm -r /etc/opencenter'
				cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)
	
				ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
				if ret!=0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent del err (%s)' %(server_info[i][NAME_INDEX]))
#					return ['NG', 'setup opencenter-agent del err (%s)' %(server_info[i][NAME_INDEX])]
	
				cmd = 'curl -s -L http://sh.opencenter.rackspace.com/install.sh | bash -s - --role=agent --ip=%s' % (OPEN_ORION)
				cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

				for loop_cnt in range(0, RETRY_CNT):
					ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
					if ret!=0:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent err (%s)' %(server_info[i][NAME_INDEX]))
#						return ['NG', 'setup opencenter-agent err (%s)' %(server_info[i][NAME_INDEX])]

					else:
						cmd = 'patch /usr/share/pyshared/opencenteragent/__init__.py < /home/openstack/agent.patch'
						cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

						ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
						if ret!=0:
							self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent patch err (%s)' %(OPEN_ORION))
						break

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### OpenOrion agent restart')
			self._restart_opencenter_agent(OPEN_ORION, OPEN_ORION_user, OPEN_ORION_upw)

		if ( 0 != switch_num):
			########################
			#Set_Parent_id by switch
			########################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set_Parent_id by switch')

			if -1 == self._open_sql():
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Parent_id by switch open sql err')
				return ['NG', 'Set Parent_id by switch open sql err' ]

			sql = 'select * from nodes where name="%s"' % KEY_SW
			result = self._show_sql(sql)
			availablesw_id=result[0][0]

			for i in range(0, switch_num):
				sql = 'select * from nodes where name="%s"' % self.switch_list[i]
				result = self._show_sql(sql)
				node_id=result[0][0]

				sql = 'update facts set value=%s where node_id=%s and key="%s"' % (availablesw_id, node_id, KEY_PARENT)
				result = self._update_sql(sql)
				if -1 == result:
					self._close_sql()
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Parent_id by switch update sql err')
					return ['NG', 'Set Parent_id by switch update sql err' ]

			self._close_sql()

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')
		return ['OK', 'success']
# Fuel --------
	"""
	def reset_fuelnode_reboot(self, hostname, username, password):

		# Get Network info
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)
		server_ip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		server_bip = nicinfo.get_ip_address(nicinfo.B_PLANE)
		if (server_ip == -1):
			self.svbkm.b_log("", self.topology_name, '#### reset_fuelnode_reboot get_nic Error')
			return -1
		else:
			print server_ip
			server_ip = '172.16.10.180'
			print server_bip
			#server_bip = '172.16.1.180'

		try:
			s2 = pxssh.pxssh()
			s2.login(server_ip, username, password)
			s2.sendline('sudo reboot')
			s2.expect('.*password for .*')
			s2.sendline(password)
			s2.prompt()
			s2.logout()

			return 0

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		except pexpect.EOF:
			# use bmc port reboot
			print "pxssh failed on login. use bmc reboot"
			ipmi = ipmi_client.IpmiClient()
			#ipmi.run_reboot(server_bip)
			ret = ipmi.get_all_status(server_bip)
			print ret
	"""

	def setup_fuelserver(self, data):

		#####################
		#set predefine
		#####################
		br_mode = "r"
		CLSTER_NAME = self.topology_name
		node_id = ""

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup_fuelserver Start ')

		ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
			return ['NG', '#### set_system_param err']

		ret, tmpdata = self.get_server_info(self.clonezilla_server_name)
		if ret == -1:
			self.svbkm.b_log(node_id, CLSTER_NAME, '####setup_fuelserver ng file is nothing ')
			return ['NG', 'Error']

		clonezilla_info = {}
		clonezilla_info['ip_address'] = tmpdata['ip_address']
		clonezilla_info['username'] = tmpdata['user_name']
		clonezilla_info['password'] = tmpdata['password']

		node_num = 0

		server_info = {}
		# Set FuelServer info
		for node_info in data:
			if (node_info['server_type'] == FUEL_SERVER) or \
				(node_info['server_type'] == FUEL6_SERVER):
				server_info['hostname'] = node_info['device_name']
				server_info['username'] = node_info['user_name']
				server_info['password'] = node_info['password']

				if node_info['server_type'] == FUEL_SERVER:
					server_info['img_name'] = FUEL_IMG
				else:
					server_info['img_name'] = FUEL6_IMG

				# Get Network info
				nicinfo = fuel_utls.node_nic_info(self.Token, server_info['hostname'])
				server_info['ip_address'] = nicinfo.get_ip_address(nicinfo.M_PLANE)
				server_info['ip_address_c'] = nicinfo.get_ip_address(nicinfo.M2_PLANE)

				if (server_info['ip_address'] == -1) or (server_info['ip_address_c'] == -1):
					self.svbkm.b_log(node_id, CLSTER_NAME, '#### setup_fuelserver Error')
					return ['NG', 'Error']
				else:
					print server_info

				node_num += 1

		# Fuel Server num Check
		if node_num > 1:
			print "Error"
			return ['NG', 'Error']

		ret = fuel_utls.clonezilla_exec(
			self.Token, fuel_utls.MODE_RESTORE, clonezilla_info, [server_info], 1)
		if ret == -1:
			self.svbkm.b_log("", self.topology_name, '####setup_fuelserver ng file is nothing ')
			return ['NG', 'Error']

		ret = self.wait_fuelserver_startup(
			server_info['hostname'], server_info['username'], server_info['password'])
		if ret == -1:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Init FuelServer env Error')
			return ['NG', 'Error']

		return ['OK', 'success']

		"""
		try:
			s = pxssh.pxssh()
			s.login(clonezilla_serverip, clonezilla_username, clonezilla_password)

			cmd = 'sudo drbl-ocs -b -g auto -e1 auto -e2 -r -x -j2 -p reboot -h "' + \
									server_cip + \
									'" -l ja_JP.UTF-8 startdisk restore ' + FUEL_IMG + ' sda'
			#print cmd
			#--- Fuel Server image restore command to Clonsezilla
			s.sendline(cmd)
			s.expect('.*password for .*')
			s.sendline(clonezilla_password)
			s.prompt()

			#--- reboot Fuel node
			ret = fuel_utls.node_reboot(self.Token, hostname, username, password)
			if ret != 0:
				#TODO
				return ['NG', 'Error']

			#--- wait for finish restore
			s.expect('You are in clonezilla box mode!')
			s.sendline('tail -f /var/log/clonezilla/clonezilla-jobs.log')
			chk_string_src = 'client ' + server_cip
			chk_string = chk_string_src.replace('.', '\.')
			# print chk_string
			s.expect(chk_string, timeout=20*60)
			s.sendcontrol('c')
			s.prompt()
			logs = s.before
			print logs

			#--- check error
			# todo

			s.logout()

			return ['OK', 'success']

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)
		"""

	def restore_osimage(self, data):

		#####################
		# set predefine
		#####################
		br_mode = "r"
		CLSTER_NAME = self.topology_name
		node_id = ""

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####  Start restore_osimage')

		ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
			return ['NG', '#### set_system_param err']

		ret, tmpdata = self.get_server_info(self.clonezilla_server_name)
		if ret == -1:
			self.svbkm.b_log(node_id, CLSTER_NAME, '####restore_osimage ng file is nothing ')
			return ['NG', 'Error']

		clonezilla_info = {}
		clonezilla_info['ip_address'] = tmpdata['ip_address']
		clonezilla_info['username'] = tmpdata['user_name']
		clonezilla_info['password'] = tmpdata['password']

		node_num = 0

		server_info = []
		# Set FuelServer info
		for node_info in data:
			if (node_info['server_type'] == NON_ASSIGNMENT) or \
				(node_info['server_type'] == OPENORION_AGENT):
				tmp_server_info = {}
				tmp_server_info['hostname'] = node_info['device_name']
				tmp_server_info['username'] = node_info['user_name']
				tmp_server_info['password'] = node_info['password']
				tmp_server_info['img_name'] = UBUNTU12_IMG

				# Get Network info
				nicinfo = fuel_utls.node_nic_info(self.Token, tmp_server_info['hostname'])
				tmp_server_info['ip_address'] = nicinfo.get_ip_address(nicinfo.M_PLANE)
				tmp_server_info['ip_address_c'] = nicinfo.get_ip_address(nicinfo.M2_PLANE)

				if (tmp_server_info['ip_address'] == -1) or (tmp_server_info['ip_address_c'] == -1):
					self.svbkm.b_log(node_id, CLSTER_NAME, '#### restore_osimage Error')
					return ['NG', 'Error']
				else:
					print tmp_server_info
				server_info.append(tmp_server_info)

				node_num += 1

		ret = fuel_utls.clonezilla_exec(
			self.Token, fuel_utls.MODE_RESTORE, clonezilla_info, server_info, node_num)
		if ret == -1:
			self.svbkm.b_log("", self.topology_name, '####restore_osimage ng file is nothing ')
			return ['NG', 'Error']

		setup_sts = 0
		devices = []
		for server in server_info:
			ret = self.wait_os_startup(
				server['hostname'], server['username'], server['password'])
			device_name = {"deviceName": server['hostname']}
			devices.append(device_name)
			if ret == -1:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### restore_osimage env Error')
				setup_sts = -1

		# set mac mapping
		# Grubメニューでストップする場合の回避処理
		# 起動途中で設定が動作し、起動完了前にリブートしてしまわないようにwaitする。
		#time.sleep(90)
		url = 'http://172.16.1.98:8080/ncs/v2/mac_mapping'
		param = {"auth": self.Token, "params": devices}
		code, mgg = fuel_utls.http_request(url, param, 'PUT')
		if code != 200:
			self.svbkm.b_log("", self.topology_name, '####restore_osimage mac_mapping NG')


		# Init SW
		ret = self.reset_switch(self.Token, self.node_list)
		if ret[0] == 'NG':
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### restore_osimage rest switch Error')
			setup_sts = -1
			#return ['NG', 'Error']

		if setup_sts == -1:
			return ['NG', 'Error']

		return ['OK', 'success']

	def get_server_info(self, server_name):

		server_info = {}
		ori = ool_rm_if.ool_rm_if()
		ori.set_auth(self.Token)

		#get IP address
		data = ori.get_nic_traffic_info(server_name, 'M-Plane')
		#data_c = ori.get_nic_traffic_info(server_name, 'C-Plane')
		if (-1 != data[0]):
			server_info['ip_address'] = data[1][0]['ip_address']
			#server_info[C_IP_INDEX] = data_c[1][0]['ip_address']
		else:
			# todo
			#self.br_log(node_id, name, br_mode, "nic traffic_type error:%s" % (data[1]))
			return -1

		#get username password
		data = ori.get_device(server_name)
		ret = 0

		if -1 != data[0]:
			#input server info
			server_info['user_name'] = data[1]['user_name']
			server_info['password'] = data[1]['password']
		else:
			#self.br_log(node_id, name, br_mode, "set_server_info <device error:+ data[0]=%s " % (data[0]))
			ret = -1

		return ret, server_info

	def reset_fuelenviroment(self, **kwargs):
		try:
			br_mode = "r"
			node_id = INIT_NODE
			CLSTER_NAME = self.topology_name

			#####################
			#set prefix log title
			#####################
			self.svbkm.set_log_Title('TS_%s_%s_' % (self.tenant_name, CLSTER_NAME))

			##################
			#make file LogName
			##################
			self.svbkm.make_log_file_name(CLSTER_NAME, node_id, br_mode, restore_name="reset")

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start(reset_fuelenviroment)')

			ret = self._reset_precheck(br_mode, CLSTER_NAME)

			if 1 == ret:
				return ['NG', '#### reset already running']
			elif -1 == ret:
				return ['NG', '#### While backup is running, can not reset']
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK(reset_fuelenviroment) ')

			####################
			# Check Servertype #
			####################
			self.ori.set_auth(self.Token)
			data = []
			na_node_list = []
			fuel_flag = 0
			openorion_flg = 0
			for server_name in self.server_list:
				data_work = self.ori.get_node(server_name)
				if -1 != data_work[0]:
					na_node_list.append(data_work[1])
					# Fuel Server or Agent
					if (data_work[1]["server_type"] == FUEL_SERVER) or \
						(data_work[1]["server_type"] == FUEL6_SERVER) or \
						(data_work[1]["server_type"] == FUEL_AGENT):
						fuel_flag += 1
						data.append(data_work[1])
					elif data_work[1]["server_type"] == OPENORION_AGENT:
						openorion_flg += 1
				else:
					self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)
					print "get node info Error"
					return ['NG', "get node info error from resouce manager "]

			print data

			# Fuel / OpenOrion miexd check
			if openorion_flg > 0 and fuel_flag > 0:
				print "enviroment server_type setting error"
				retArray = ['NG', "enviroment server_type setting error"]
			else:
				if openorion_flg > 0:
					self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)
					retArray = self.init_oo_env(na_node_list)
				elif fuel_flag > 0:
					retArray = self.init_fuelenv(data)
				else:
					retArray = self.init_ubuntu_env(na_node_list)
					# retArray = ['OK', "success"]

			# set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			return retArray

		except Exception, e:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :' + str(e))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' % (traceback.format_exc()))

			# set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			raise

	def init_oo_env(self, data):

		#####################
		# set predefine
		#####################
		node_id = ""
		br_mode = "r"
		CLSTER_NAME = self.topology_name

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_oo_env Start')

		# Reboot Fuel Agent
		for node_info in data:
			hostname = node_info['device_name']
			username = node_info['user_name']
			password = node_info['password']

			ret = self.wait_os_startup_oo(hostname, username, password)
			if ret == -1:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_oo_env Error')

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'node_list : %s' % self.node_list)
		retArray = self.reset_oo_agent(self.Token, self.node_list)

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_oo_env End')
		return retArray

	def init_ubuntu_env(self, data):

		#####################
		#set predefine
		#####################
		node_id = ""
		br_mode = "r"
		CLSTER_NAME = self.topology_name

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_ubuntu_env Start')

		# Reboot Fuel Agent
		sts = 0
		for node_info in data:
			if node_info["server_type"] == NON_ASSIGNMENT:
				time.sleep(120)
			hostname = node_info['device_name']
			username = node_info['user_name']
			password = node_info['password']

			# server reboot
			ret = fuel_utls.node_reboot(self.Token, hostname, username, password)
			if ret != 0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_ubuntu_env reboot error')
				sts += 1
				#return ['NG', 'Error']

		if sts > 0:
			return ['NG', 'Error']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_ubuntu_env End')
		return ['OK', 'Success']

	def init_fuelenv(self, data):

		#####################
		#set predefine
		#####################
		node_id = ""
		br_mode = "r"
		CLSTER_NAME = self.topology_name

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_fuelenv Start')

		# Get FuelServer info
		for node_info in data:
			if (node_info['server_type'] == FUEL_SERVER) or \
				(node_info['server_type'] == FUEL6_SERVER):
				hostname = node_info['device_name']
				username = node_info['user_name']
				password = node_info['password']

		if len(node_info) < 1:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Get FuelServer info Error')
			return ['NG', "Error"]

		# Custamize FuelServer network setting
		ret = self.init_fuelserver_env(hostname, username, password)
		if ret == -1:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Init FuelServer env Error')
			return ['NG', 'Error']

		# Reboot Fuel Agent
		for node_info in data:
			if node_info['server_type'] == FUEL_AGENT:
				hostname = node_info['device_name']
				username = node_info['user_name']
				password = node_info['password']

				# server reboot
				ret = fuel_utls.node_reboot(self.Token, hostname, username, password)
				if ret != 0:
					# TODO
					return ['NG', 'Error']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### init_fuelenv End')
		return ['OK', 'Success']

	@retry(pexpect.EOF, tries=100, delay=10)
	def wait_fuelserver_startup(self, hostname, username, password):

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_fuelserver_startup Start')

		# Get C-Plane/M-Plane address
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)

		server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		if (server_mip == -1):
			self.svbkm.b_log(hostname, self.topology_name, '#### wait_fuelserver_startup get M_PLANE Error')
			return -1

		# --- Set Public network env
		try:
			s = pxssh.pxssh()
			s.login(server_mip, username, password, login_timeout=10*60)
			s.logout()

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_fuelserver_startup End')
		return 0

	@retry(pexpect.EOF, tries=100, delay=10)
	def wait_os_startup(self, hostname, username, password):

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup Start')

		# Get C-Plane/M-Plane address
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)

		server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		if (server_mip == -1):
			self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup get M_PLANE Error')
			return -1

		# --- Set Public network env
		try:
			s = pxssh.pxssh()
			s.login(server_mip, username, password, login_timeout=10*60)
			self._pxssh_set_sudopasswd(s, password)
			self._pxssh_set_zabbixagent(s, hostname, server_mip)
			# self._pxssh_set_network(s)
			# self._pxssh_put_oopatch(s)
			self._pxssh_set_hostname(s, hostname, password)
			s.logout()

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup End')
		return 0

	@retry(pexpect.EOF, tries=100, delay=10)
	def wait_os_startup_oo(self, hostname, username, password):

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup_oo Start')

		# Get C-Plane/M-Plane address
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)

		server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		if (server_mip == -1):
			self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup_oo get M_PLANE Error')
			return -1

		# --- Set Public network env
		try:
			s = pxssh.pxssh()
			s.login(server_mip, username, password, login_timeout=10*60)
			s.logout()

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		self.svbkm.b_log(hostname, self.topology_name, '#### wait_os_startup_oo End')
		return 0

	def _pxssh_set_sudopasswd(self, instance, password):
		s = instance

		cmd = 'sudo ls'
		s.sendline(cmd)
		s.expect('.*password for .*', timeout=2*600)
		s.sendline(password)
		s.prompt()

	def _pxssh_set_hostname(self, instance, hostname, password):
		s = instance

		hostname_script = 'cat > /tmp/hostname.sh << EOF_MAIN\r' \
			'sed -i -e s/novacomxx/%(hostname)s/ /etc/hostname\r' \
			'sed -i -e s/novacomxx/%(hostname)s/ /etc/hosts\r' \
			'hostname %(hostname)s\r' \
			'EOF_MAIN\r' % {"hostname": hostname}

		s.sendline(hostname_script)
		s.sendline('chmod +x /tmp/hostname.sh')
		s.sendline('sudo /tmp/hostname.sh')
		s.prompt()

	def _pxssh_set_zabbixagent(self, instance, hostname, server_mip):
		s = instance

		# Set Zabbix Agent config
		cmd = 'sudo sed -i -e s/172\.16\.1\.180/' + server_mip + \
								'/ //etc/zabbix/zabbix_agentd.conf'
		s.sendline(cmd)
		s.prompt()

		cmd = 'sudo sed -i -e s/novacomXX/' + hostname + \
								'/ //etc/zabbix/zabbix_agentd.conf'
		s.sendline(cmd)
		s.prompt()

		s.sendline('sudo service zabbix-agent restart')
		s.prompt()

	def _pxssh_set_network(self, instance):
		s = instance

		cmd = 'sudo sed -i -e s/"auto eth3"/"#auto eth3"/ //etc/network/interfaces'
		s.sendline(cmd)
		s.prompt()

		cmd = 'sudo sed -i -e s/"iface eth3"/"#iface eth3"/ //etc/network/interfaces'
		s.sendline(cmd)
		s.prompt()

	def _pxssh_put_oopatch(self, instance):
		s = instance

		oo_patch = 'cat > /tmp/agent.patch << EOF_MAIN\r' \
			'--- __init__.py_org	2014-07-23 14:39:04.239097199 +0900\r' \
			'+++ __init__.py	2014-07-23 14:39:25.194833750 +0900\r' \
			'@@ -118,7 +118,8 @@\r' \
			'         except Exception:\r' \
			'             self._exit(True)\r' \
			'\r' \
			'-        socket.setdefaulttimeout(30)\r' \
			'+        #socket.setdefaulttimeout(30)\r' \
			'+        socket.setdefaulttimeout(2*60)\r' \
			'\r' \
			'     def _exit(self, exception):\r' \
			'         """Terminate the agent.\r' \
			'EOF_MAIN\r'

		s.sendline(oo_patch)
		s.sendline('sudo cp /tmp/agent.patch /home/openstack/')

	@retry(pexpect.EOF, tries=100, delay=10)
	def init_fuelserver_env(self, hostname, username, password):

		self.svbkm.b_log(hostname, self.topology_name, '#### init_fuelserver_env Start')

		# Get C-Plane/M-Plane address
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)

		server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		if (server_mip == -1):
			self.svbkm.b_log(hostname, self.topology_name, '#### init_fuelserver_env get M_PLANE Error')
			return -1

		# --- Set Public network env
		try:
			s = pxssh.pxssh()
			s.login(server_mip, username, password, login_timeout=10*60)
			self._pxssh_set_sudopasswd(s, password)
			self._pxssh_set_zabbixagent(s, hostname, server_mip)

			# DHCP Start up waitting
			time.sleep(120)
			#s.sendline('sudo tail -n 100 -f /var/log/docker-logs/dnsmasq.log')
			#s.expect('.*password for .*')
			#s.sendline(password)
			#s.expect("started", timeout=20*60)
			#s.sendcontrol('c')
			s.logout()

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		self.svbkm.b_log(hostname, self.topology_name, '#### init_fuelserver_env End')
		return 0

	def teardown_server(self, **kwargs):
		try:
			br_mode = "stop"
			node_id = STOP_NODE
			CLSTER_NAME = self.topology_name

			#####################
			#set prefix log title
			#####################
			self.svbkm.set_log_Title('TS_%s_%s_' % (self.tenant_name, CLSTER_NAME))

			##################
			#make file LogName
			##################
			self.svbkm.make_log_file_name(CLSTER_NAME, node_id, br_mode, restore_name="reset")

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start(teardown_server)')

			"""
			ret = self._reset_precheck(br_mode, CLSTER_NAME)

			if 1 == ret:
				return ['NG', '#### reset already running']
			elif -1 == ret:
				return ['NG', '#### While backup is running, can not reset']
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK(teardown_server)')
			"""

			####################
			# Check Servertype #
			####################
			self.ori.set_auth(self.Token)
			data = []
			for server_name in self.server_list:
				data_work = self.ori.get_node(server_name)
				if -1 != data_work[0]:
					# Fuel Server or Agent
					if (data_work[1]["server_type"] == FUEL_SERVER) or \
						(data_work[1]["server_type"] == FUEL6_SERVER):
						data.append(data_work[1])
				else:
					print "get node info Error"
					return ['NG', "get node info error from resouce manager "]

			print data

			# Fuel agent check
			if len(data) == 0:
				return ['OK', "success"]

			###############
			####TearDown####
			###############
			retArray = self.teardown_fuelserver(data)

			#set mode none
			#self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			return retArray

		except Exception, e:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :' + str(type(e)))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### message:' + str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :' + str(e))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' % (traceback.format_exc()))

			#set mode none
			#self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			raise

	def teardown_fuelserver(self, data):

		#####################
		#set predefine
		#####################
		node_id = ""
		br_mode = "stop"
		CLSTER_NAME = self.topology_name

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### teardown_fuelserver Start')

		# Get FuelServer info
		for node_info in data:
			hostname = node_info['device_name']
			username = node_info['user_name']
			password = node_info['password']

			# Disable FuelServer NIC1(DHCP working)
			ret = self.disable_fuelserver_dhcp(hostname, username, password)
			if ret == -1:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Disable FuelServer NIC1 Error')
				return ['NG', 'Error']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### teardown_fuelserver End')
		return ['OK', 'Success']

	def disable_fuelserver_dhcp(self, hostname, username, password):

		self.svbkm.b_log(hostname, self.topology_name, '#### disable_fuelserver_hdcp Start')

		# Get M-Plane address
		nicinfo = fuel_utls.node_nic_info(self.Token, hostname)

		server_mip = nicinfo.get_ip_address(nicinfo.M_PLANE)
		if (server_mip == -1):
			self.svbkm.b_log(hostname, self.topology_name, '#### disable_fuelserver_hdcp get M_PLANE Error')
			return -1

		#--- disable eth0
		try:
			s = pxssh.pxssh()
			s.login(server_mip, username, password)

			#--- Set Public network ipaddress
			cmd = 'sudo sed -i -e s/ONBOOT=yes/ONBOOT=no' \
									'/ /etc/sysconfig/network-scripts/ifcfg-eth0'
			#print cmd

			s.sendline(cmd)
			s.expect('.*password for .*')
			s.sendline(password)
			s.prompt()
			#print s.before

			s.sendline('sudo service network restart')
			s.prompt()
			logs = s.before
			s.logout()

		except pxssh.ExceptionPxssh, e:
			print "pxssh failed on login."
			print str(e)

		#--- check error
		#print logs
		if "NG" in logs:
			self.svbkm.b_log(hostname, self.topology_name, '#### network restart error')
			return -1
		# ^^^---

		self.svbkm.b_log(hostname, self.topology_name, '#### disable_fuelserver_hdcp End')
		return 0

	################################
	# reset OpenOrion Agent
	################################
	def reset_oo_agent(self, token, node_list, **kwargs):

		br_mode = 'r'
		node_id = ''
		CLSTER_NAME = 'reset_oo-agent'
		#set token
		ret = self.svbkm.set_token_value(CLSTER_NAME, node_id, br_mode, token)

		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '###set token err')
			return ['NG', '###set token err']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Reset OpenOrion Agent Start ')

		# set parameter
		ret = self.svbkm.set_system_param(CLSTER_NAME, node_id, br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
			return ['NG', '#### set_system_param err']

		ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err1')
			return ['NG', '#### set_system_param err1']

		# set prefix log title
		self.svbkm.set_log_Title('TS_%s_%s_' % (self.tenant_name, CLSTER_NAME))

		# make file LogName
		self.svbkm.make_log_file_name(CLSTER_NAME, node_id, br_mode, restore_name="reset")

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start(reset_oo_agent)')

		ret = self._reset_precheck(br_mode, CLSTER_NAME)

		if 1 == ret:
			return ['NG', '#### reset already running']
		elif -1 == ret:
			return ['NG', '#### While backup is running, can not reset']
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK(reset_oo_agent) ')

		ret = self.reset_oo_agent_sub(
			token, node_list, br_mode=br_mode, node_id=node_id, clster_name=CLSTER_NAME)

		# set mode none
		self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

		return ret

	def reset_oo_agent_sub(self, token, node_list, **kwargs):

		server_info_num = 4
		br_mode = kwargs['br_mode']
		node_id = kwargs['node_id']
		CLSTER_NAME = kwargs['clster_name']

		self.ori.set_auth(token)
		#####################################
		# get Info of Open Orion
		#####################################
		data = self.ori.get_device(self.opencenter_server_name)

		self.svbkm.br_log(
			node_id, CLSTER_NAME, br_mode, '#### s_name=%s' % (self.opencenter_server_name))

		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set openorion get_device err')
			return ['NG', 'Set openorion get_device err']

		dev_data = {}
		dev_data = data[1]
		oo_user = dev_data['user_name']
		oo_upw = dev_data['password']

		# get IP address(M-Plane)
		data = self.ori.get_nic_traffic_info(self.opencenter_server_name, 'M-Plane')

		if -1 != data[0]:
			data1 = {}
			data1 = data[1][0]
			oom_ip = data1['ip_address']
		else:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, "get ip_address error:%s" % (data[1]))
			return ['NG', 'get ip_address error']

		# get IP address(C-Plane)
		data = self.ori.get_nic_traffic_info(self.opencenter_server_name, 'C-Plane')

		if -1 != data[0]:
			data1 = {}
			data1 = data[1][0]
			oo_ip = data1['ip_address']
		else:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, "get ip_address error:%s" % (data[1]))
			return ['NG', 'get ip_address error']

		#####################################
		# Set chef Make Exec Env
		#####################################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef Make Exec Env')

		data = self.ori.get_device(OCCHEF)
		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef get_device err')
			return ['NG', 'Set chef get_device err']

		dev_data = {}
		dev_data = data[1]
		chef_user = dev_data['user_name']
		chef_pw = dev_data['password']

		data = self.ori.get_nic_traffic_info(OCCHEF, 'M-Plane')
		if -1 == data[0]:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef get_nic_traffic err')
			return ['NG', 'Set chef get_nic_traffic err']

		nic_data = {}
		nic_data = data[1][0]
		chef_ip = nic_data['ip_address']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'Set chef Make Exec')
		ret = self.svbkm.make_exec(
			chef_user, chef_ip, chef_user, chef_pw, FILEDIR, br_mode, node_id, CLSTER_NAME)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef Make Exec err')
			return ['NG', 'Set chef make_exec err']

		ret = self.set_node_list(node_list)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set node_list err')
			return ['NG', 'Set node_list err']

		server_node_name = self.server_list
		server_num = len(server_node_name)
		server_info = [["null" for j in range(server_info_num)] for i in range(server_num)]

		# Set Server info
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Server info')

		for i in range(0, server_num):
			self.svbkm.br_log(
				node_id, CLSTER_NAME, br_mode, 'Set Server info server_node_name[%s]=%s' % (i, server_node_name[i]))

			retdata = self.svbkm.set_server_info(
				node_id, server_info[i], server_node_name[i], CLSTER_NAME, br_mode)
			if 0 != retdata[0]:
				self.svbkm.br_log(
					node_id, CLSTER_NAME, br_mode, '#### Set Server info  server_node_name=%s' % (server_node_name[i]))

				msg = 'Set Server info set server_node_name=%s' % (server_node_name[i])
				return ['NG', msg]

		if (0 != server_num):
			############################
			# Delete node from OpenOrion
			############################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node')

			end_point = 'https://admin:password@%s:8443/' % (oo_ip)

			for i in range(0, server_num):
				# cmd = 'opencentercli node delete %s --endpoint %s 2> /dev/null' % (server_info[i][NAME_INDEX], end_point)
				cmd = 'ssh root@%s opencentercli node delete %s --endpoint %s 2> /dev/null' % \
					(chef_ip, server_info[i][NAME_INDEX], end_point)
				ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
				if ret != 0:
					self.svbkm.br_log(
						node_id, CLSTER_NAME, br_mode,
						'#### Delete node from opencenter err (%s)' % (server_info[i][NAME_INDEX]))

				cmd = 'ssh root@%s %s client delete -y %s 2> /dev/null' % (chef_ip, KNIFE, server_info[i][NAME_INDEX])

				ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
				if ret != 0:
					self.svbkm.br_log(
						node_id, CLSTER_NAME, br_mode, '#### Delete client from chef err (%s)' % (server_info[i][NAME_INDEX]))

			# Set Exec_User
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
			ret = self.svbkm.get_user_name(self.opencenter_server_name)
			if 0 == ret[0]:
				EXEC_USER = ret[1]
			else:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
				msg = 'Set Exec_User error'
				return ['NG', msg]

			###############################
			# setup opencenter-agent to node
			###############################
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent')

			for i in range(0, server_num):
				while 1:
					ret = self.svbkm.make_exec(
						EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX],
						server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
					if ret != 0:
						self.svbkm.br_log(
							node_id, CLSTER_NAME, br_mode, '#### rsa key copy all=%d index=%d' % (server_num, i))

					cmd = 'ls'
					cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)
					ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
					if ret != 0:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### chk boot WAIT(%s)' %(server_info[i][NAME_INDEX]))
					else:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### chk boot OK(%s)' %(server_info[i][NAME_INDEX]))
						break

				cmd = '/bin/rm -r /etc/opencenter'
				cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

				ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
				if ret != 0:
					self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent del err (%s)' %(server_info[i][NAME_INDEX]))
	
				cmd = 'curl -s -L http://sh.opencenter.rackspace.com/install.sh | bash -s - --role=agent --ip=%s' % (oo_ip)
				cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

				for loop_cnt in range(0, RETRY_CNT):
					ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
					if ret != 0:
						self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent err (%s)' %(server_info[i][NAME_INDEX]))

					else:
						cmd = 'patch /usr/share/pyshared/opencenteragent/__init__.py < /home/openstack/agent.patch'
						cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

						ret = self.svbkm.shellcmd_exec(EXEC_USER, br_mode, node_id, CLSTER_NAME, cmd)
						if ret != 0:
							self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent patch err (%s)' %(oo_ip))
						break

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### OpenOrion agent restart')
			self._restart_opencenter_agent(oom_ip, oo_user, oo_upw)

		switch_node_name = self.switch_list
		switch_num = len(switch_node_name)
		if ( 0 != switch_num):
			put_url = BASE_SRV_URL % (OPENORION_HOST, HTTP_PORT)
			switches = []
			for switch_name in self.switch_list:
				tmp_data = switch_name.encode('utf-8')
				switches.append(tmp_data)
			para_data = { "switches" : switches }

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### put_url = %s, para_data = %s' % (put_url, para_data))
			code, msg = fuel_utls.http_request(put_url, para_data, 'POST')
			if code != 200:
				self.svbkm.bir_log(node_id, CLSTER_NAME, '#### POST Switch to OpenOrion ERR')

			#cmd =  CURL_CMD % (PUT_CMD, PUT_HEADER, put_url, para_data)

			#ret, stdout_data, stderr_data = self.cmd_run(cmd)
			pass


		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Reset OpenOrion Agent End ')
		return ['OK', 'success']

	def reset_switch(self, token, node_list, **kwargs):

		br_mode = 'r'
		node_id = ''
		CLSTER_NAME = 'reset_switch'

		#set token
		ret = self.svbkm.set_token_value(CLSTER_NAME, node_id, br_mode, token)

		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '###set token err')
			return ['NG', '###set token err']
		#get restore folder name
		BACKUP_FOLDER_RNAME="%s/%s/%s" % (BASE_DIR_NAME, RESET_ENV, MASTER_ENV)
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, 'reset_name: %s' % BACKUP_FOLDER_RNAME)

		#set define
		SAVE_DIR_NAME_SWITCH="%s/switch" % (BACKUP_FOLDER_RNAME)

		ret = self.set_node_list(node_list)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set node_list err')

		if (0 == len(self.switch_list)):
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### switch num is 0 then "no action"')
			return ['OK', 'success']

		#switch restore
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Switch reset Start')

		ret = self.set_system_param(CLSTER_NAME, node_id, br_mode)
		if ret != 0:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err')
			return ['NG', '#### set_system_param err']

		#Set Exec_User
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
		ret = self.svbkm.get_user_name(self.opencenter_server_name)
		if 0 == ret[0]:
			exec_user=ret[1]
		else:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User error')
			msg='Set Exec_User error'
			return ['NG', msg]

		psbk = psbk_manager.psbk_manager(exec_user, self.storage_server_name, SAVE_DIR_NAME_SWITCH, self.logObj)

		switch_node_names=','.join(self.switch_list)
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH Call psbk.set_PS_list')
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_names))

		ret = psbk.set_PS_list(switch_node_names)
		if 0 != ret:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH psbk.set_PS_list Err')
			return ['NG', '#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)']

		psbk.set_auth(token)
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
		ret = psbk.exec_restore()
		if 0 != ret:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
			return ['NG', '####SWITCH  psbk.exec_restore() ']

		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch restore End')
		return ['OK', 'success']
