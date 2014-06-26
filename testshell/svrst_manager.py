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

FILEDIR="/etc/backuprestore"
BASE_DIR_NAME="/backup"
MASTER_ENV='BASE'
RESET_ENV='INITIALIZE'
INIT_NODE=0

OK=0
NG=1

SV_NAME=0
SW_NAME=1

#DEBUG="ON"
DEBUG="OFF"

R_STOP_FILENAME="/r_stop"
MODE_BACKUP = "backup"
MODE_RESTORE= "restore"
MODE_NONE= "none"

OCCHEF='occhef'
#OPEN_ORION='172.16.1.51'
OPEN_ORION='172.16.1.184'

KNIFE='/opt/chef-server/bin/knife'

#---------------------------------------------------------
class svrst_manager():
	def __init__(self):
		logger = logging.getLogger('rstlog')
		logger.setLevel(logging.DEBUG)
		handler = logging.handlers.SysLogHandler(address = '/dev/log')
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
		svbkutl=svbk_utls.svbk_utls()
		svbkutl.set_auth(self.Token)
		node_list=svbkutl.separate_node(node_list)

		if ((-1 == node_list[SV_NAME]) or (-1 == node_list[SW_NAME])):
			return -1
		
		self.node_list=node_list
		self.server_list=node_list[SV_NAME]
		self.switch_list=node_list[SW_NAME]
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

	def set_system_param(self,clster_name,node_id,br_mode):

		conf = ConfigParser.SafeConfigParser()

		set_file_path='%s/%s' % (FILEDIR, SET_CONFIG_FILE)

		ret=conf.read(set_file_path)

		if len(ret)==0 :
			self.svbkm.br_log(node_id, clster_name, br_mode, '####set_system_param ng file is nothing ')
			return NG

		self.svbkm.br_log(node_id, clster_name, br_mode, '####set_system_param file_name :%s' %(ret[0]) )

		self.limit_disk_size = int(conf.get('options', 'limit_disk_size'))
		self.interval_time = int(conf.get('options', 'interval_time'))
		self.opencenter_server_name=conf.get('options','opencenter_server_name')
		self.storage_server_name = conf.get('options', 'storage_server_name')
		self.loop_timeout_m = int(conf.get('options', 'loop_timeout_m'))

		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file limit_disk_size :%s' %(self.limit_disk_size) )
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file interval_time :%s' %(self.interval_time) )
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file opencenter_server_name :%s' %(self.opencenter_server_name) )
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file storage_server_name :%s' %(self.storage_server_name) )
		self.svbkm.br_log(node_id, clster_name, br_mode, '####read_file loop_timeout_m :%s' %(self.loop_timeout_m) )

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
			self.svbkm.make_log_file_name(CLSTER_NAME,node_id,br_mode, restore_name="reset")

			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

			ret = self._reset_precheck(br_mode, CLSTER_NAME)

			if 1 == ret:
				return ['NG', '#### reset already running']
			elif -1==ret:
				return ['NG', '#### While backup is running, can not reset']
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')

			###############
			####Restore####
			###############
			retArray = self.reset_cluster_sub(node_id)

			#set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			return retArray

		except Exception,e:
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()))

			#set mode none
			self.svbkc.set_mode_state(CLSTER_NAME, MODE_NONE)

			raise

	#####################
	#Reset Module
	#####################
	def reset_cluster_sub(self, node_id='', **kwargs):

		#####################
		#set predefine 
		#####################
		br_mode="r"
		CLSTER_NAME = self.topology_name

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
			return [NG, msg]

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

				return [NG, '#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)']

			psbk.set_auth(self.Token)
			self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
			ret=psbk.exec_restore()
			if 0 != ret:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
				return [NG, '####SWITCH  psbk.exec_restore() ']

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

		for i in range(START_INDEX, server_cnt):
			# update_br_agent
			svbkutl=svbk_utls.svbk_utls()
			ret =svbkutl.update_br_agent(server_info[i][USER_INDEX], server_info[i][IP_INDEX])
			if NG == ret[0]:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### update_br_agent reset err ')
				return ['NG', 'update_br_agent reset err']

			cmd='ssh root@%s  /boot/%s  %s  %s  r %s' %(server_info[i][IP_INDEX],  svbkutl.get_br_agent_name(), server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME)
			ret = self.svbkm.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
			if ret != 0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### make run reset err ')
				return ['NG', 'make run reset err']

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

		#####################################
		#Set chef Make Exec Env
		#####################################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Set chef Make Exec Env')

		self.ori.set_auth(self.Token)
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

		############################
		#Delete node from OpenOrion
		############################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node')

		end_point =  'https://admin:password@%s:8443/' % (OPEN_ORION)

		for i in range(START_INDEX, server_cnt):
			cmd = 'opencentercli node delete %s --endpoint %s 2> /dev/null' % (server_info[i][NAME_INDEX], end_point)

			ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
			if ret!=0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node err %s' %(server_info[i][NAME_INDEX]))
#				return ['NG', 'Delete node err %s' %(self.server_list[i])]

		############################
		#Delete info from chef
		############################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete info from chef')

		for i in range(START_INDEX, server_cnt):
			cmd = 'ssh root@%s %s node delete -y %s 2> /dev/null' % (chef_ip, KNIFE, server_info[i][NAME_INDEX])

			ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
			if ret!=0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete node from chef err (%s)' %(server_info[i][NAME_INDEX]))
#				return ['NG', 'Delete node from chef err (%s)' %(server_info[i][NAME_INDEX])]
		
			cmd = 'ssh root@%s %s client delete -y %s 2> /dev/null' % (chef_ip, KNIFE, server_info[i][NAME_INDEX])

			ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
			if ret!=0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### Delete client from chef err (%s)' %(server_info[i][NAME_INDEX]))
#				return ['NG', 'Delete client from chef err (%s)' %(server_info[i][NAME_INDEX])]

		###############################
		#setup opencenter-agent to node
		###############################
		self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent')

		for i in range(START_INDEX, server_cnt):
			cmd = '/bin/rm -r /etc/opencenter'
			cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)

			ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
			if ret!=0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent del err (%s)' %(server_info[i][NAME_INDEX]))
				return ['NG', 'setup opencenter-agent del err (%s)' %(server_info[i][NAME_INDEX])]

			cmd = 'curl -s -L http://sh.opencenter.rackspace.com/install.sh | sudo bash -s - --role=agent --ip=%s' % (OPEN_ORION)
			cmd = 'ssh root@%s "%s"' % (server_info[i][IP_INDEX], cmd)
			
			ret = self.svbkm.shellcmd_exec(chef_user, br_mode, node_id, CLSTER_NAME, cmd)
			if ret!=0:
				self.svbkm.br_log(node_id, CLSTER_NAME, br_mode, '#### setup opencenter-agent err (%s)' %(server_info[i][NAME_INDEX]))
				return ['NG', 'setup opencenter-agent err (%s)' %(server_info[i][NAME_INDEX])]

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
