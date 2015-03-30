import ool_rm_if
import pica8_if
import pfs_if
import riava_if
import centec_if
import datetime
import pslist_backup
import commands
import os
import ConfigParser

# Physical Switch Backup/Restore Manager
TST_FLAG='OFF'
DBG_FLAG='ON'
TRACE_TITLE='PSBK_MANAGER:%s'
EXCEPT_TITLE='PSBK_MANAGER:%s'

AUTH_KEY='xxx'

BACKDIR="/backup/"
PY_PATH=os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = 'switch.ini'
TST_FILE='psbk_manager_tst.cnf'

MODEL_KEY='A_MODEL='
MAC_KEY='A_MAC='
IP_KEY='A_IP='
UID_KEY='A_UID='
UPW_KEY='A_UPW='

PICA8_KEY1="P-3290"
PICA8_KEY2="P-3295"
RIAVA_KEY1="BCM56846"
PFS_KEY1="PF5240"
CENTEC_KEY1="V350"
#-------------------------------------------------------
class psbk_manager:

	def __init__(self, Local_User, BK_Host, BK_Dir, ObjLog):
		self.auth =""
		self.a_device_name={}
		self.a_model={}
		self.a_mac={}
		self.a_ip={}
		self.a_uid={}
		self.a_upw={}
		self.a_host=[]

		self.ps_list=''

		self.ori=ool_rm_if.ool_rm_if()

		self.local_user= Local_User
		self.bk_host= BK_Host
		self.bk_dir = BK_Dir
		self.logger = ObjLog
		self.sync_mode = 'SYNC'
		self.time_out = 120

		self.ori.set_auth(AUTH_KEY)

	def __get_BK_info__(self):
		node_data=self.ori.get_node(self.bk_host)
		nic_data=self.ori.get_nic_traffic_info(self.bk_host, "M-Plane")

		if -1 != node_data[0]:
			node_data1={}
			node_data1=node_data[1]
			self.bk_uid = node_data1['user_name']
			self.bk_upw = node_data1['password']
			nic_data1={}
			nic_data1=nic_data[1][0]
			self.bk_uip = nic_data1['ip_address']

		else:
			self.__except_log__(node_data[1])
			return -1
		return 0

	def __trace_log__(self, log):
		if 'ON' == DBG_FLAG:
			self.logger.debug(TRACE_TITLE %(log))
		return 0

	def __except_log__(self, log):
		self.logger.debug(EXCEPT_TITLE %(log))
		return 0

	def __get_SW_info__(self):
		self.__trace_log__('get_PS_info IN')
		self.a_host = self.ps_list.split(",")

		# Get Physical switch infomation from resource manager
		for i in range(0, len(self.a_host)):
			self.ori.set_auth(self.auth)
			data_sw=self.ori.get_switch(self.a_host[i])

			if -1 != data_sw[0]:
				pfs_info={}
				pfs_info=data_sw[1]
				self.a_model[i]=pfs_info["product_name"]
				self.a_mac[i]=pfs_info["mac_address"]
				self.a_ip[i]=pfs_info["ip_address"]
				self.a_uid[i]=pfs_info["user_name"]
				self.a_upw[i]=pfs_info["password"]
			else:
				self.__except_log__('<url access error>')
				return -1

		if TST_FLAG == 'ON':
			tst_file= '%s/%s' % (PY_PATH, TST_FILE)

			try:
				f=open(tst_file, 'r')
			except Exception, e:
				self.__except_log__(str(e))
				return -1

			for line in f:
				if line.find(MODEL_KEY) != -1:
					self.a_mode=line[line.find(MODEL_KEY)+len(MODEL_KEY):-1]
				if line.find(MAC_KEY) != -1:
					self.a_mac=line[line.find(MAC_KEY)+len(MAC_KEY):-1]
				if line.find(IP_KEY) != -1:
					self.a_ip=line[line.find(IP_KEY)+len(IP_KEY):-1]
				if line.find(UID_KEY) != -1:
					self.a_uid=line[line.find(UID_KEY)+len(UID_KEY):-1]
				if line.find(UPW_KEY) != -1:
					self.a_upw=line[line.find(UPW_KEY)+len(UPW_KEY):-1]

		self.__trace_log__('get_PS_info OUT')
		return 0

	def set_PS_list(self, PS_list):
		self.__trace_log__('set_PS_list IN')
		self.ps_list = PS_list
		self.__trace_log__('set_PS_list OUT')
		return 0

	def set_auth(self, auth):
		self.__trace_log__('set_auth IN')
		self.auth=auth
		self.ori.set_auth(auth)
		self.__trace_log__('set_auth OUT')

	def set_config(self, Conf_Dir):
		if "" == Conf_Dir:
			conf_path = './%s' % CONFIG_FILE
		else:
			conf_path = '%s/%s' % (Conf_Dir, CONFIG_FILE)

		conf = ConfigParser.SafeConfigParser()
		ret = conf.read(conf_path)

		if len(ret) == 0:
			self.__trace_log__('#### setting default to sync mode for switch')
			self.sync_mode = 'SYNC'
			self.time_out = 120
		else:
			self.sync_mode = conf.get('mode', 'sync_mode')
			self.time_out    = conf.get('mode', 'time_out')
			self.__trace_log__('#### setting value(%s, %s) to sync mode for switch' % self.sync_mode, str(self.time_out))

	def exec_backup(self):
		self.__trace_log__('exec_backup IN')
		ret=self.__get_SW_info__()
		if -1==ret:
			self.__except_log__('get_PS_info error')
			return -1

		if -1 ==self.__get_BK_info__():
			return -1

		print "Start  Backup of Physical Switch"

		for i in range(0, len(self.a_host)):
			bk_dir_tmp=self.bk_dir.replace(".", ":")

			# backup procedure
			if ((True == self.a_model[i].startswith(PICA8_KEY1)) or (True == self.a_model[i].startswith(PICA8_KEY2))):
				bk=pica8_if.pica8_if()
			elif  (True == self.a_model[i].startswith(RIAVA_KEY1)):
				bk=riava_if.riava_if()
			elif  (True == self.a_model[i].startswith(PFS_KEY1)):
				bk=pfs_if.pfs_if()
			elif  (True == self.a_model[i].startswith(CENTEC_KEY1)):
				bk=centec_if.centec_if()
			else:
				self.__except_log__('Physical Switch info err %s' % self.a_model[i])
				return -1

			bk.set_auth(self.auth)
			bk.set_host_name(self.a_host[i])
			bk.set_logger(self.logger)
			ret=bk.set_bksrv(self.bk_host, BACKDIR + bk_dir_tmp)
			if ret == -1:
				self.__except_log__('backup process error')
				return -1

			ret=bk.exec_backup()
			if ret == -1:
				self.__except_log__('backup process error')
				return -1

		print "Finish Backup of Physical Switch"

		# Save Physical Switch list
		d = datetime.datetime.today()
		tm= d.strftime("%H%M%S")

		TMP_FILE='/tmp/ps_' + tm
		try:
			f=open(TMP_FILE, 'w')
		except Exception, e:
			self.__except_log__('pfs file error(backup)' + str(e))
			return -1
		else:
			f.write(self.ps_list)
		f.close()

		psl=pslist_backup.pslist_backup(self.local_user, self.bk_uip, self.bk_uid, self.bk_upw)
		psl.set_logger(self.logger)
		ret = psl.set_pslist(TMP_FILE, self.bk_dir)
		if 0 != ret:
			self.__except_log__('pslist copy err')
			print 'pslist copy err'
			return -1

		commands.getoutput('rm ' + TMP_FILE)

		self.__trace_log__('exec_backup OUT')
		return 0

	def exec_restore(self):
		self.__trace_log__('exec_restore IN')
		ret=self.__get_SW_info__()
		if -1==ret:
			self.__except_log__('get_PS_info error')
			return -1

		if -1 ==self.__get_BK_info__():
			return -1

		# Load Physical Switch list
		d = datetime.datetime.today()
		tm= d.strftime("%H%M%S")

		TMP_FILE='/tmp/ps_' + tm

		psl=pslist_backup.pslist_backup(self.local_user, self.bk_uip, self.bk_uid, self.bk_upw)
		psl.set_logger(self.logger)
		ret = psl.get_pslist(TMP_FILE, self.bk_dir)
		if 0 != ret:
			self.__except_log__('pslist copy err')
			print 'pslist copy err'
			return -1

		pslist=''
		try:
			f=open(TMP_FILE, 'r')
		except Exception, e:
			self.__except_log__('psl file error(restore)' + str(e))
			return -1
		else:
			for line in f:
				pslist=line
		f.close()

		commands.getoutput('rm ' + TMP_FILE)

		# Check Physical Switch list
		for key in range(0, len(self.a_host)):
			if -1 == pslist.find(self.a_host[key]):
				self.__except_log__('different physical switch list' + self.a_host[key])
				print ' different backup and restore to physical switch list'
				return -1

		print "Start  Restore of Physical Switch"

		for i in range(0, len(self.a_host)):
			bk_dir_tmp=self.bk_dir.replace(".", ":")

			# restore procedure
			if ((True == self.a_model[i].startswith(PICA8_KEY1)) or (True == self.a_model[i].startswith(PICA8_KEY2))):
				rst=pica8_if.pica8_if()
			elif  (True == self.a_model[i].startswith(RIAVA_KEY1)):
				rst=riava_if.riava_if()
			elif  (True == self.a_model[i].startswith(PFS_KEY1)):
				rst=pfs_if.pfs_if()
			elif  (True == self.a_model[i].startswith(CENTEC_KEY1)):
				rst=centec_if.centec_if()
			else:
				self.__except_log__('Physical Switch info err %s' % self.a_model[i])
				return -1

			rst.set_auth(self.auth)
			rst.set_host_name(self.a_host[i])
			rst.set_logger(self.logger)
			rst.set_sync_mode(self.sync_mode, self.time_out)
			ret=rst.set_bksrv(self.bk_host, BACKDIR + bk_dir_tmp)
			if ret == -1:
				self.__except_log__('backup process error')
				return -1

			ret = rst.exec_restore()
			if ret == -1:
				self.__except_log__('restore process error')
				return -1

		print "Finish Restore of Physical Switch"

		#put configuration to resource manager

		self.__trace_log__('exec_restore OUT')
		return 0
