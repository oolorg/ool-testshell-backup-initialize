import json
import fcntl
import logging
import os.path

BASE_DIR_NAME="/etc/ool_br_rest"
CONFLICT_FILE='br_conflict.inf'
MODE_BACKUP = "backup"
MODE_RESTORE= "restore"
MODE_NONE   = "none"

INFO_FRAME=[{'status':MODE_NONE,'GroupName':'', 'NodeList':''}]

class svbk_conflict():
	def __init__(self):
		self.logger=logging

	def set_loger(self,logging):
		self.logger=logging

	def _logout(self, group_name_s, group_name_d, br_mode, msg):
		if '' == group_name_d:
			self.logger.debug('SVBK_Conflict:mode=%s,cluster=%s,%s ' % (br_mode, group_name_s, msg))
			print 'SVBK_Conflict:mode=%s,cluster=%s,%s ' % (br_mode, group_name_s, msg)
		else:
			self.logger.debug('SVBK_Conflict:mode=%s,cluster=%s:%s,%s ' % (br_mode, group_name_s, group_name_d, msg))
			print 'SVBK_Conflict:mode=%s,cluster=%s:%s,%s ' % (br_mode, group_name_s, group_name_d, msg)

	def _isfile(self,file_path):
		if os.path.isfile(file_path):
			pass
		else:
			fd= open(file_path, "w")
			fd.close()
			os.chmod(file_path, 0666)

	def del_file(self):
		del_file_name='%s/%s' % (BASE_DIR_NAME, CONFLICT_FILE)
		if os.path.exists(del_file_name):
			os.remove(del_file_name)
			self.logger.debug('SVBK_Conflict del file=%s' % del_file_name)

	def chk_mode_state(self, br_mode, group_name, node_list):

		if br_mode == "b":
			br_state=MODE_BACKUP
		if br_mode == "r":
			br_state=MODE_RESTORE
		
		file_name='%s/%s' % (BASE_DIR_NAME, CONFLICT_FILE)
		self._isfile(file_name)
		fd= open(file_name, "r+")
		fcntl.flock(fd,fcntl.LOCK_EX)

		ret_status=0
		chk_status=''
		mode_none_cnt=0
		list_exist=0
		loop_cnt=0

		r_data = fd.read()
		if 0 == len(r_data):
			info=''
		else:
			info=json.loads(r_data)

		if 0!=len(info):
			for loop_cnt in range(len(info)):
				# check status
				if MODE_NONE == info[loop_cnt]['status']:
					mode_none_cnt=mode_none_cnt+1
					continue
		
				#check node
				node_data=node_list.split(',')
				for node_name in node_data:
					if node_name in info[loop_cnt]['NodeList']:
						list_exist=1
						break
				if 1==list_exist:
					break

			# status of all data is none
			if mode_none_cnt == len(info):
				chk_status = MODE_NONE
			elif 1== list_exist:
				chk_status = info[loop_cnt]['status']
			else:
				for loop_cnt in range(len(info)):
					if group_name == info[loop_cnt]['GroupName']:
						chk_status=info[loop_cnt]['status']
						break

			if MODE_BACKUP in chk_status:
				if br_mode == "b":
					self._logout(info[loop_cnt]['GroupName'], '', br_mode, '####1 backup already running')
					ret_status=1
				if br_mode == "r":
					self._logout(info[loop_cnt]['GroupName'], group_name, br_mode, '####2 While backup is running, can not restore')
					ret_status=-1
	
			elif MODE_RESTORE in chk_status:
				if br_mode == "b":
					self._logout(info[loop_cnt]['GroupName'], group_name, br_mode, '####3 While restore is running, can not backup')
					ret_status=-1
				if br_mode == "r":
					self._logout(info[loop_cnt]['GroupName'], '', br_mode, '####4 restore already running')
					ret_status=1
	
			else:
				self._logout(info[loop_cnt]['GroupName'], '', br_mode, '####can backup :nothing set mode')
				# check group
				group_exist = 0
				list_exist  = 0
				for loop_cnt in range(len(info)):
					if group_name == info[loop_cnt]['GroupName']:
						group_exist=1
						info[loop_cnt]['status'] = br_state
						info[loop_cnt]['NodeList'] = node_list
						break

				# not found node_name in Nodelist
				if 0 == group_exist:
					add_data={}
					add_data.update({'status':br_state})
					add_data.update({'GroupName':group_name})
					add_data.update({'NodeList':node_list})
					info.append(add_data)
	
		else:
			# create status info
			init_data=INFO_FRAME
			init_data[0]['status']=br_state
			init_data[0]['GroupName']=group_name
			init_data[0]['NodeList']=node_list
	
			w_data=json.dumps(init_data)
			fd.seek(0)
			fd.write(w_data)
			fd.truncate() 
			fcntl.flock(fd,fcntl.LOCK_UN)
			fd.close()
			return ret_status

		if 0==ret_status:
			w_data=json.dumps(info)
			fd.seek(0)
			fd.write(w_data)
			fd.truncate() 
		
		fcntl.flock(fd,fcntl.LOCK_UN)
		fd.close()
		return ret_status

	def set_mode_state(self, group_name, state):
		file_name='%s/%s' % (BASE_DIR_NAME, CONFLICT_FILE)
		self._isfile(file_name)
		fd= open(file_name, "r+")
		fcntl.flock(fd,fcntl.LOCK_EX)
		
		group_exit=0
		ret_status=0
		loop_cnt  =0

		r_data = fd.read()
		if 0 == len(r_data):
			chk=''
		else:
			chk=json.loads(r_data)

		if 0!=len(chk):
			for loop_cnt in range(len(chk)):
				# check group
				if group_name == chk[loop_cnt]['GroupName']:
					chk[loop_cnt]['status']=state
					group_exit=1
					break

		if 0 == group_exit:
			self._logout(group_name, '', state, '#### no specified group name')
			# create status info
			if isinstance(chk, list):
				init_data=chk
			else:
				init_data=[]
			add_data={}
			add_data.update({'status':state})
			add_data.update({'GroupName':group_name})
			add_data.update({'NodeList':''})
			init_data.append(add_data)

			w_data=json.dumps(init_data)
			fd.seek(0)
			fd.write(w_data)
			fd.truncate() 
		else:
			w_data=json.dumps(chk)
			fd.seek(0)
			fd.write(w_data)
			fd.truncate() 

		fcntl.flock(fd,fcntl.LOCK_UN)
		fd.close()
		return ret_status
