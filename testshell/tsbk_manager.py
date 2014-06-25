import time
import os
import subprocess
import pexpect
import ool_rm_if
import datetime
import commands
import ConfigParser
import psbk_manager
import traceback
import json
import logging
import logging.handlers
import svbk_utls
import svbk_conflict

# Physical Switch Backup/Restore Manager
# disk size
LIMIT_DISK_SIZE_G=1
INTERVAL_TIME=10
CANCEL="off"

CONFIG_FILE = 'settings.ini'


NAME_INDEX  =0
IP_INDEX    =1
USER_INDEX  =2
PW_INDEX    =3

STORAGE_SV  =0
START_INDEX =1


R_END_STATUS="restore_ok"
R_NG_STATUS="restore_ng"

B_END_STATUS="backup_ok"
B_NG_STATUS="backup_ng"


FILEDIR="/etc/backuprestore"
BASE_DIR_NAME="/backup"

OK=0
NG=1

#DEBUG="ON"
DEBUG="OFF"

B_STOP_FILENAME="/b_stop"
R_STOP_FILENAME="/r_stop"
ROOP_TIME_OUT=60

MODE_BACKUP = "backup"
MODE_RESTORE= "restore"
MODE_NONE= "none"


SW="sw "
SV="sv "

SV_NAME=0
SW_NAME=1
SV_NODE=2
SW_NODE=3
SV_P_NODE=4
SW_P_NODE=5
SV_BACKENDS=6

BACKUP_DATA_SV = "server"
BACKUP_DATA_SW = "switch"

BACKUP_DATA_KEY_NAME     ="name"
BACKUP_DATA_KEY_P_ID     ="parent_id"
BACKUP_DATA_KEY_BACKENDS ="backends"
BACKUP_DATA_KEY_N_ID ="id"

BR_AGENT_SRC_DIR='/etc/backuprestore'
BR_AGENT='br_agent_update'
BR_AGENT_DST_DIR='/boot'

###############################
#Server Backup Up Manager
###############################
class tsbk_manager:
    def __init__(self,logger):

        self.logObj=logger
        self.limit_disk_size=LIMIT_DISK_SIZE_G
        self.interval_time=INTERVAL_TIME
        self.loop_timeout_m=ROOP_TIME_OUT
        self.storage_server_name=""
        self.logpath="/var/log/br/"
        self.logfile="ddd.log"
        self.token=""
        self.date_time = datetime.datetime.today()
        self.folder_date_str = self.date_time.strftime("%Y_%m%d_%H%M%S---")

    def __del__( self ):
        del self.logObj
        pass

    def make_log_file_name(self, clster_name, node_id, br_mode, **kwargs):
        ret=0
        d = self.date_time
        tm1= d.strftime("%m%d_%H%M_%S_")

        if "b" == br_mode :
            self.logfile=tm1 + br_mode +"_" + clster_name + "_ID%s_" %(node_id) \
                                   + "N"+ self.folder_date_str + kwargs['backup_name'] + ".log"
        else:
            self.logfile=tm1 + br_mode +"_" + clster_name + "_ID%s_" %(node_id) \
                                   + "N"+ self.folder_date_str + kwargs['restore_name'] + ".log"

        return 0

    def br_log(self, node_id, name, br_mode, log):

        self.logObj.debug('BRLOG ID=%s NAME=%s %s:%s' %(node_id, name, br_mode, log))

        d = datetime.datetime.today()
        tm= d.strftime("%m%d %H:%M:%S")

        f = open(self.logpath+self.logfile, 'a+')
        f.write('%s BRLOG ID=%s NAME=%s %s:%s \n' %(tm, node_id, name, br_mode, log))
        f.close()

        return 0


    def set_system_param(self,clster_name,node_id,br_mode):

        conf = ConfigParser.SafeConfigParser()

        set_file_path=FILEDIR+"/"+CONFIG_FILE

        ret=conf.read(set_file_path)

        if len(ret)==0 :
            self.br_log(node_id, clster_name, br_mode, '####set_system_param ng file is nothing ')
            return NG

        self.br_log(node_id, clster_name, br_mode, '####set_system_param file_name :%s' %(ret[0]) )


        self.limit_disk_size = int(conf.get('options', 'limit_disk_size'))
        self.interval_time = int(conf.get('options', 'interval_time'))
        self.storage_server_name = conf.get('options', 'storage_server_name')
        self.loop_timeout_m = int(conf.get('options', 'loop_timeout_m'))

        self.br_log(node_id, clster_name, br_mode, '####read_file limit_disk_size :%s' %(self.limit_disk_size) )
        self.br_log(node_id, clster_name, br_mode, '####read_file interval_time :%s' %(self.interval_time) )
        self.br_log(node_id, clster_name, br_mode, '####read_file storage_server_name :%s' %(self.storage_server_name) )
        self.br_log(node_id, clster_name, br_mode, '####read_file loop_timeout_m :%s' %(self.loop_timeout_m) )

        return OK

    def set_token(self,clster_name,node_id,br_mode, token):

        if len(token) == 0:
            self.br_log(node_id, clster_name, br_mode, '###token is none ' )
            return 1

        self.token = token

        #self.br_log(node_id, clster_name, br_mode, '###token is : %s' % self.token)

        return 0

    def get_node_info(self, node_id, name, br_mode,node_name):

        #ZANTEI set node

        #get all node id
        server_node_name   = []
        switch_node_name   = []

        #server_node_name=node_name

        utls=svbk_utls.svbk_utls()
        utls.set_auth(self.token)
        retArray = utls.separate_node(node_name)

        server_node_name = retArray[0]
        switch_node_name = retArray[1]


        self.br_log(node_id, name, br_mode, '*** Resouce Manager DB SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** Resouce Manager DB SWITCH_NODE_NAME  : %s' % switch_node_name)


        retdata =[server_node_name, switch_node_name]

        return retdata

    def set_server_info(self, node_id, server_info_list,server_name, name, br_mode):

        #make Resource Manager Instance
        temp_server_info=server_info_list


        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        self.br_log(node_id, name, br_mode, "ddddd self.token:%s" %(self.token))

        #get IP address
        data=ori.get_nic_traffic_info(server_name, 'M-Plane')
        #get ip address
        if -1 != data[0]:
            data1={}
            data1=data[1][0]
            temp_server_info[1]=data1['ip_address']

        else:
            self.br_log(node_id, name, br_mode, "nic traffic_type error:%s" %(data[1]))
            ret = 1
            return [ret, temp_server_info]

        #get username password
        data=ori.get_device(server_name)
        ret=0

        if -1 != data[0]:
            data1={}
            data1=data[1]

            #input server info
            temp_server_info[0]=server_name
            temp_server_info[2]=data1['user_name']
            temp_server_info[3]=data1['password']

            #check info (length)
            for i in range(0, 4):
                self.br_log(node_id, name, br_mode, "set_server_info temp_server_info[%s]=%s" %(i,temp_server_info[i]))
                char_len=len(temp_server_info[i])
                if char_len == 0:
                    self.br_log(node_id, name, br_mode, "set_server_info len=0 err ")
                    ret=1
                    break
        else:
            self.br_log(node_id, name, br_mode, "set_server_info <device error:+ data[0]=%s " %(data[0]))
            ret=1

        return [ret, temp_server_info]


    def shellcmd_exec(self, exec_user, br_mode, node_id, name, cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        #self.logger.debug(run_cmd)
        self.br_log(node_id, name, br_mode, run_cmd)
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('shell '+line)
                self.br_log(node_id, name, br_mode, 'shell stdout : '+line)
            shell_ret=p.wait()
        except Exception, e:
            #self.logger.debug(e)
            self.br_log(node_id, name, br_mode, 'shell err : '+str(e))
            shell_ret= 1
        #self.logger.debug(shell_ret)
        self.br_log(node_id, name, br_mode, 'shell ret =  : '+str(shell_ret))

        return shell_ret

#    def _ssh_copyid(self, exec_user,DEST_IP, DEST_USER, DEST_PW):
    def _ssh_copyid(self, exec_user,br_mode, node_id, name, DEST_IP, DEST_USER, DEST_PW):
        ssh_newkey = 'Are you sure you want to continue connecting'

        pub_key_path =  '/home/' + exec_user + '/.ssh/id_rsa.pub'

        p=pexpect.spawn('sudo -u %s ssh-copy-id -i %s %s@%s' %(exec_user,pub_key_path,DEST_USER,DEST_IP))
        p.timeout=120

        i=p.expect([ssh_newkey,'password:',pexpect.EOF])
        if i==0:
            p.sendline('yes')
            i=p.expect([ssh_newkey,'password:',pexpect.EOF])
        if i==1:
            p.sendline(DEST_PW)
            p.expect(pexpect.EOF)
        elif i==2:
            pass
        #print p.before # print out the result
        self.br_log(node_id, name, br_mode, 'ssh_copy_id:%s' %(p.before) )

        return 0


    def shellcmd_exec_br_state(self, exec_user, br_mode, node_id, name, cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('status = '+line)
                self.br_log(node_id, name, br_mode, 'status = '+line)
            shell_ret=p.wait()
        except Exception, e:
            self.br_log(node_id, name, br_mode, 'shell br_state err : '+e)
            shell_ret= 1
        return shell_ret

    def make_exec(self, exec_user, DEST_IP, DEST_USER, DEST_PW, FILEDIR, br_mode, node_id, name):


        #known host key_delete
        cmd='ssh-keygen -f "/home/%s/.ssh/known_hosts" -R %s' %(exec_user, DEST_IP)

        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)


        #key_generate
        flug = os.path.exists("/home/%s/.ssh/id_rsa" %(exec_user))
        if not flug:
            self.br_log(node_id, name, br_mode, 'Plese make /home/%s/.ssh/id_rsa IP=%s' %(exec_user, DEST_IP) )

            return ret

        #ssh_copy_id
        self.br_log(node_id, name, br_mode, 'ssh_copy_id exec IP=%s' %(DEST_IP) )

        #print "make_exec"

        #self._ssh_copyid(exec_user,DEST_IP, DEST_USER, DEST_PW)
        self._ssh_copyid(exec_user,br_mode, node_id,  name, DEST_IP, DEST_USER, DEST_PW)

        #print "make_exec end"


        #key_root_copy
        cmd='scp %s/key_copy.py %s@%s:.' %(FILEDIR, DEST_USER, DEST_IP)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            self.br_log(node_id, name, br_mode, 'key_root_copy err IP=%s' %(DEST_IP))
            return ret

        #pexpect copy
        cmd='scp %s/pexpect.py %s@%s:.' %(FILEDIR, DEST_USER, DEST_IP)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            self.br_log(node_id, name, br_mode, 'pexpect copy err IP=%s' %(DEST_IP))
            return ret

        #key_copy.py exec
        cmd='ssh %s@%s python ./key_copy.py  %s %s' %(DEST_USER, DEST_IP,DEST_USER, DEST_PW)
        ret = self.shellcmd_exec(exec_user,br_mode, node_id, name, cmd)
        if ret!=0:
            self.br_log(node_id, name, br_mode, 'key_copy.py exec err IP=%s' %(DEST_IP))
            return ret

        return 0


    def check_inputdata(self, node_id, name, br_mode, domain, topology_name):

        ################
        #B Input DataCheck
        ################
        ngchar_list=["|"," "]

        for char in ngchar_list:
            ret1=domain.find(char)
            if ret1 != -1:
                self.br_log(node_id, name, br_mode, '#### Input DataCheck Err domain,space or pipe:[%s] ' %(domain))
                return [1, '#### Input DataCheck Err domain=[%s],space or pipe ' %(domain)]

            ret2=topology_name.find(char)
            if ret2 != -1:
                self.br_log(node_id, name, br_mode, '#### Input DataCheck Err topology_name,space or pipe:[%s] ' %topology_name)
                return [1, '#### Input DataCheck Err topology_name=[%s],space or pipe ' %(topology_name)]

        return [0, "ok"]


    def shellcmd_exec_rest_diskSize(self, exec_user, br_mode, node_id, name,cmd):
        shell_ret=0

        sudo_cmd='sudo -u %s ' %(exec_user)

        run_cmd=sudo_cmd+cmd
        try:
            p=subprocess.Popen(run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                #self.logger.debug('status = '+line)
                self.br_log(node_id, name, br_mode, 'Avaliavle Disk Size(K):'+line)
                ret_val=line
            shell_ret=p.wait()
        except Exception, e:
            self.br_log(node_id, name, br_mode, 'shell br_state err : '+e)
            shell_ret= 1

        return [shell_ret, int(ret_val)]

    def setdb_backup_data2(self,db_clster_name,db_folder_name,db_node_list):

        #make regist info
        #clastername_ID8
        #db_clster_name=  clster_name + "_ID%s" %(node_id)

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        #db_server_list = server_list
        #db_switch_list = switch_list

        #server & switch
        #db_node_list = db_server_list + db_switch_list


        #folder_name
        #d = datetime.datetime.today()
        #db_folder_name= self.folder_date_str  + folder_name

        print('#### set_backup_data :  db_clster_name = %s' %(db_clster_name))
        print('#### set_backup_data :  db_folder_name = %s' %(db_folder_name))
        print('#### set_backup_data :  db_node_list = %s'   %(db_node_list))

        #set db
        data=ori.set_backup_data(db_clster_name, db_folder_name, db_node_list)


        if -1 == data[0]:
            print('#### set_backup_data err  data=%s' %(data))
            return 1

        print('#### success data=%s' %(data))

        return 0

    def setdb_backup_data(self,clster_name,node_id,br_mode, folder_name, server_list, switch_list):

        #make regist info
        #clastername_ID8
        db_clster_name=  clster_name + "_ID%s" %(node_id)

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)

        db_server_list = server_list
        db_switch_list = switch_list

        #server & switch
        db_node_list = db_server_list + db_switch_list


        #folder_name
        d = datetime.datetime.today()
        db_folder_name= self.folder_date_str  + folder_name

        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_clster_name = %s' %(db_clster_name))
        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_folder_name = %s' %(db_folder_name))
        self.br_log(node_id, clster_name, br_mode, '#### set_backup_data :  db_node_list = %s'   %(db_node_list))

        #set db
        data=ori.set_backup_data(db_clster_name, db_folder_name, db_node_list)


        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode, '#### set_backup_data err  data=%s' %(data))
            return 1

        return 0

    def make_nodeinfo_file(self, clster_name, node_id, br_mode, backup_node_data_info):

        #define node list
        server_node_name   = backup_node_data_info[SV_NAME]
        switch_node_name   = backup_node_data_info[SW_NAME]


        server_num = len(server_node_name)

        switch_num = len(switch_node_name)

        jdata ={"server": [],"switch": []}

        #set server node
        for i in range(server_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(server_node_name[i])}
            jdata[BACKUP_DATA_SV].append(tmp)

        #set switch node
        for i in range(switch_num):
            tmp ={BACKUP_DATA_KEY_NAME: str(switch_node_name[i])}
            jdata[BACKUP_DATA_SW].append(tmp)


        #file name make
        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")
        TMP_FILE='/tmp/node_info' + tm

        #file OutPut
        with open(TMP_FILE, 'w') as f:
            json.dump(jdata, f, sort_keys=True, indent=4)

        return [0, TMP_FILE]


    #####################
    #Backup Module
    #####################
    def backup_cluster_sub(self, node_id, token, topology_name, domain, node_name,**kwargs):

        #####################
        #B predefine
        #####################

        br_mode="b"
        CLSTER_NAME = "TS_" + domain +"_" + topology_name

        ret=self.check_inputdata(node_id, CLSTER_NAME, br_mode, domain, topology_name)

        if ret[0]!=0:
            return [NG, ret[1]]

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Backup Start ')

        if not 'backup_name' in kwargs:
            return [NG, 'backup folder name is required']

        BACKUP_FOLDER_RNAME=CLSTER_NAME + "_ID%s" %(node_id)  +"/" +self.folder_date_str + kwargs['backup_name']


        self.br_log(node_id, CLSTER_NAME, br_mode, '###  Input Parameter  ####')
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_id              : %s' % node_id)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'token                : %s' % token)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'topology_name        : %s' % topology_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'domain               : %s' % domain)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_name            : %s' % node_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'kwargs[backup_name]  : %s' %(kwargs['backup_name']))



        self.br_log(node_id, CLSTER_NAME, br_mode, 'backup_folder_name: %s' % BACKUP_FOLDER_RNAME)

        ret=self.set_system_param(CLSTER_NAME,node_id,br_mode)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
            msg='#### set_system_param err'
            return [NG, msg]


        #####################
        #B define
        #####################
        SAVE_DIR_NAME=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/server"
        SAVE_DIR_NAME_SWITCH=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/switch"

        SERVER_INFO_FILE_NAME="serv_info"
        SERVER_LIST_FILE=SAVE_DIR_NAME+"/"+SERVER_INFO_FILE_NAME


        NODE_INFO_FILE_NAME="node_info"
        NODE_LIST_FILE=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, token)

        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return [NG, '###get token ng']

        #start time get
        time1 = time.time()

        ########################
        #B Get server name for opencenterDB
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for opencenterDB')

        ret_info = self.get_node_info( node_id, CLSTER_NAME, br_mode,node_name)
        server_node_name = ret_info[SV_NAME]
        server_num=len(server_node_name)

        switch_node_name = ret_info[SW_NAME]
        switch_num=len(switch_node_name)
        switch_node_name_char=','.join(switch_node_name)
        server_node_name_char=','.join(server_node_name)

        #server,switch node info
        backup_node_data_info = ret_info;


        self.br_log(node_id, CLSTER_NAME, br_mode, '*** SERVER_NODE_LIST :List %s, cnt=%s' %(server_node_name,server_num))
        self.br_log(node_id, CLSTER_NAME, br_mode, '*** SWITCH_NODE_LIST :List %s, cnt=%s' %(switch_node_name,switch_num))

        if (0 == server_num) and ( 0 == switch_num):
            #self.logger.debug('sever num = 0 then not backup')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
            return [OK, 'Backup ok']


        ######################
        #B Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        server_cnt=server_num+1
        server_info_num=4
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ########################
        #B Set Storage Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        retdata = self.set_server_info(node_id, server_info[0],self.storage_server_name, CLSTER_NAME, br_mode)
        if 0 != retdata[0]:
            #self.b_log(node_id, CLSTER_NAME, '#### server info set err')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
            msg='server info set err'
            return [NG, msg]

        ########################
        #B Set NovaClaster Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server info')

        for i in range(1, server_cnt):
            #self.b_log(node_id, CLSTER_NAME, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )
            self.br_log(node_id, CLSTER_NAME, br_mode, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )


            retdata = self.set_server_info(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
            if 0 != retdata[0]:
                #self.b_log(node_id, CLSTER_NAME, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )

                msg='Set Server info set server_node_name=%s' %(server_node_name[i-1])
                return [NG, msg]

        ################
        #B Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
        EXEC_USER=server_info[STORAGE_SV][USER_INDEX]


        #####################################
        #B Set NovaClaster Server Make Exec Env
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server Make Exec Env')

        for i in range(server_cnt):
            self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start all=%d index=%d' %(server_cnt, i))
            ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err all=%d index=%d' %(server_cnt, i) )
                msg='make_exec err'
                return [NG, msg]

        #######################
        #B Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret==0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory Err (already same name dirctory)')
            msg='Check Directory (already same name dirctory)'
            return [NG, msg]


        #################################
        #B Check Strorage Server Disk Size
        ################################
        cmd="ssh root@%s df -k | grep '/dev/sda1' | awk '{ print $4 }' " %(server_info[STORAGE_SV][IP_INDEX])
        ret_list = self.shellcmd_exec_rest_diskSize(EXEC_USER, br_mode, node_id, CLSTER_NAME,cmd)
        if 0 != ret_list[0]:
            #self.b_log(node_id, CLSTER_NAME, '#### Check Strorage Server Disk Size Err =%s' %( ret_list[0] ) )
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Strorage Server Disk Size Err =%s' %( ret_list[0] ) )

            msg='#### Check Strorage Server Disk Size Err =%s' %( ret_list[0] )
            return [NG, msg]


        diskSize_G = ret_list[1]/(1024*1024)

        #chek disk size
        if self.limit_disk_size >= diskSize_G:
            self.br_log(node_id, CLSTER_NAME, br_mode,'#### Check Strorage Server Disk Size Shortage Err limit_disk_size=%s > diskSize_G=%s ' %( self.limit_disk_size, diskSize_G ) )

            msg='#### Check Strorage Server Disk Size Shortage  Err limit_disk_size=%s > diskSize_G=%s ' %( self.limit_disk_size, diskSize_G )
            return [NG, msg]


        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Strorage SV Disk Size LIMIT(G)=%s diskSize(G)=%s ' %( self.limit_disk_size, diskSize_G ) )


        #########################################
        #B Make Backup directory to Storage Server
        #########################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Backup directory to Storage Server')
        cmd='%s/dirmake  %s  %s  %s  %s' %(FILEDIR, BASE_DIR_NAME, BACKUP_FOLDER_RNAME, server_info[STORAGE_SV][IP_INDEX], server_info[STORAGE_SV][USER_INDEX])
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### make dir err ')
            msg='make dir err'
            return [NG, msg]


        #######################
        #B Make Directory Check
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Directory Check ')

        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Directory Check Err (directory is none)')
            msg='Make Directory Check Err (directory is none) '
            return [NG, msg]


        #DDD debug DDD
        #switch_num=0

        #DDD
        if DEBUG == "ON":
            switch_num=0
        #################
        #B switch backup
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup Start')

            psbk=psbk_manager.psbk_manager(self.storage_server_name, SAVE_DIR_NAME_SWITCH,self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.set_PS_list')

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_name_char))


            ret=psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.set_PS_list Err')
                msg='####SWITCH  psbk.set_PS_list Err psbk.set_PS_list Err)'
                return [NG, msg]

            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_backup()')
            ret=psbk.exec_backup()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_backup() Err')
                msg='####SWITCH  psbk.exec_backup() Err'
                return [NG, msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch backup End')


        #DDD
        if DEBUG == "ON":
            server_num=0

        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            #return self._ok()
            return [OK, 'Backup ok']


        #########################################################################
        ret = self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server Back UP Start')

        ################
        #B Run backup
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run backup')

        for i in range(START_INDEX, server_cnt):

            #copy br_agent_update
            cmd='scp %s/%s %s@%s:%s' %(BR_AGENT_SRC_DIR, BR_AGENT, server_info[i][USER_INDEX], server_info[i][IP_INDEX], BR_AGENT_DST_DIR)
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### copy br_agent_update [%s] err ' %(server_info[i][IP_INDEX]) )
                msg='copy br_agent_update [%s] err ' % (server_info[i][IP_INDEX])
                return [NG, msg]

            #exec br_agent
            cmd='ssh root@%s  /boot/%s  %s  %s  b %s' %(server_info[i][IP_INDEX],BR_AGENT,  server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME)
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
            if ret!=0:
                #self.logger.debug('make run backup err '  )
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### make run backup err ')
                #return self._fail(msg='make run backup err')
                msg='make run backup err'
                return [NG, msg]


        ################
        #B Start status check
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

        while 1:
            all_ret=0

            ################
            #B time cal
            ################

            #end time get
            time2 = time.time()
            timedf = time2-time1

            timedf_int = int(timedf)
            HH= timedf_int / 3600
            #SS= timedf_int % 3600
            MM= (timedf_int % 3600)/60
            SS= timedf_int % 60

            #logout
            self.br_log(node_id, CLSTER_NAME, br_mode, "#### loop  status check Total Time: %s:%s:%s (h:m:s)" %(HH,MM,SS) )


            for i in range(START_INDEX, server_cnt):

                ################
                #B status roop check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_b_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_END_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

                self.br_log(node_id, CLSTER_NAME, br_mode,'server_info[%s] IP=%s SV_NAME=%s ' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )

                all_ret = ret + all_ret


                ################
                #B status view
                ################
                cmd='ssh root@%s tail -n 1 %s/status_b_%s  2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec_br_state(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

                ################
                #B status ng check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_b_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], B_NG_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                if ret==0:
                    self.br_log(node_id, CLSTER_NAME, br_mode, '#### Status roop check [backup_ng] Err IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                    msg='Status roop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX])
                    return [NG, msg]


                time.sleep(self.interval_time )

            if all_ret==0:
                break

            #force-stop
            if os.path.exists(FILEDIR+B_STOP_FILENAME):
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### force stop')
                os.remove(FILEDIR+B_STOP_FILENAME)
                msg='#### force stop '
                return [NG, msg]

                break

            #time-out
            if HH >= self.loop_timeout_m :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### HH is %s hour Over stop' %(self.loop_timeout_m))
                msg='#### HH is %s hour Over stop ' %(self.loop_timeout_m)
                return [NG, msg]
                break

        #################
        #set DB backup data
        #################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data  ')
        ret = self.setdb_backup_data(CLSTER_NAME, node_id, br_mode, kwargs['backup_name'], server_node_name, switch_node_name)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup data ng ')
            msg='#### set DB backup data ng '
            return [NG, msg]

        ##########################
        #set  backup info to storage
        ##########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup info  ')
        retdata = self.make_nodeinfo_file(CLSTER_NAME, node_id, br_mode, backup_node_data_info)
        if 0 != retdata[0]:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set DB backup info ng ')
            msg='#### set DB backup info ng'
            return [NG, msg]

        file_path = retdata[1]

        #scp to storage :backup info file
        cmd="scp %s root@%s:%s " %(file_path, server_info[STORAGE_SV][IP_INDEX] , NODE_LIST_FILE)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### B backup info ServerName trans err')
            msg='B #### backup info ServerName trans err'
            return [NG, msg]

        #remove file
        cmd="rm %s " %(file_path)
        commands.getoutput(cmd)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')

        return [OK, 'Backup ok'] #END


    def backup_cluster(self, node_id, token, topology_name, domain, node_name,**kwargs):

        try:

            br_mode="b"
            CLSTER_NAME = domain + "_"+ topology_name

            #conflict check method instance
            cflct=svbk_conflict.svbk_conflict()
            cflct.set_loger(self.logObj)

            # set conflict info


            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            #var = kwargs['backup_name']
            #if not var:
            #    self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder name is none')
            #    return [NG, '###backup folder name is none']

            self.make_log_file_name(CLSTER_NAME,node_id,br_mode,**kwargs)


            ###################
            #check mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            node_list=','.join(node_name)
            ret = cflct.chk_mode_state( "b", topology_name, node_list)
            if ret==1:
                return [NG, "backup already runnning"]
            elif ret==-1:
                msg='#### restore runnning then err'
                return [NG, msg]

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')


            ###############
            ####Backup ####
            ###############
            retArray = self.backup_cluster_sub(node_id, token, topology_name, domain, node_name,**kwargs)

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            return retArray

        except Exception,e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()) )

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            #raise
            return [NG, traceback.format_exc()]


##### RRR ####


    def get_restore_foldername(self, **kwargs):

        tmp_str = kwargs['restore_name']

        restore_folder_name = kwargs['restore_name']

        return restore_folder_name

    def get_node_info_from_serverFile(self, node_id, name, br_mode, node_info):

        #get all node id
        server_node_name   = []
        switch_node_name   = []

        for i in range(len(node_info[BACKUP_DATA_SV])):
            server_node_name.append(node_info[BACKUP_DATA_SV][i][BACKUP_DATA_KEY_NAME])

        for i in range(len(node_info[BACKUP_DATA_SW])):
            switch_node_name.append(node_info[BACKUP_DATA_SW][i][BACKUP_DATA_KEY_NAME])

        self.br_log(node_id, name, br_mode, '*** storege server file SERVER_NODE_NAME  : %s' % server_node_name)
        self.br_log(node_id, name, br_mode, '*** storege server file SWITCH_NODE_NAME  : %s' % switch_node_name)

        retdata =[server_node_name, switch_node_name]

        return retdata

    def check_novacluster_node(self,clster_name,node_id,br_mode, backupedCluster_node,nowClaster_node):

        self.br_log(node_id, clster_name, br_mode, '#### R NovaCluster_CheckStart')

        self.br_log(node_id, clster_name, br_mode, '#### R server node check  backupedCluster_node:%s' %(backupedCluster_node ))
        self.br_log(node_id, clster_name, br_mode, '#### R server node check  nowClaster_node      %s' %(nowClaster_node ))

        set_ab=set(nowClaster_node)-set(backupedCluster_node)
        if 0 != len(set_ab):
            self.br_log(node_id, clster_name, br_mode, '#### R server node check Err unnecessary node:%s' %(set_ab))
            return 1

        self.br_log(node_id, clster_name, br_mode, '#### R NovaClaster_Cherck OK')

        return 0

    def get_restoredb_list(self,node_id, clster_name, br_mode, db_clster_name):

        #self.br_log(node_id, clster_name, br_mode, "call ori.get_backup_cluster start db_clster_name=%s" %(db_clster_name))

        ori=ool_rm_if.ool_rm_if()
        ori.set_auth(self.token)
        data=ori.get_backup_cluster(db_clster_name)

        backup_list=[]

        #db err
        if -1 == data[0]:
            self.br_log(node_id, clster_name, br_mode, " ori.get_backup_cluster err data=%s  db_clster_name=%s" %(data,db_clster_name))
            return [1, ""]

        #get registed db list
        backupAllData = data[1]

        for i in range(len(backupAllData)):
            backup_list.append(backupAllData[i]["backup_name"])


        return [0, backup_list]

    def make_restore_list_key(self,node_id, clster_name, br_mode, restore_list):

        #delete time
        restore_key_list = []

        restore_key_list = restore_list

        return restore_key_list

    def get_restore_top_foldername(self,node_id, clster_name, br_mode, db_clster_name):

        #get db list
        retdata = self.get_restoredb_list(node_id, clster_name, br_mode,db_clster_name)
        if 0 !=retdata[0]:
            self.br_log(node_id, clster_name, br_mode, " get_restoredb_list err")
            return [1, retdata[1]]

        restore_list = retdata[1]

        #sort new data is top
        restore_list.sort(reverse=True)

        if len(restore_list) == 0:
            self.br_log(node_id, clster_name, br_mode, "restore_list is 0")
            return [1, ""]

        restore_key_list = self.make_restore_list_key(node_id, clster_name, br_mode,restore_list)


        return [0, restore_key_list[0]]


    #def restore_cluster(self, api, node_id, **kwargs):
    def restore_cluster(self, node_id, token, topology_name, domain, node_name,**kwargs):

        try:
            br_mode="r"
            CLSTER_NAME = domain + "_"+ topology_name

            #conflict check method instance
            cflct=svbk_conflict.svbk_conflict()
            cflct.set_loger(self.logObj)


            ##################
            #make file LogName
            ##################
            if False == os.path.isdir(self.logpath):
                os.mkdir(self.logpath)

            restore_folder_name = self.get_restore_foldername(**kwargs)

            if not restore_folder_name:
                self.br_log(node_id, CLSTER_NAME, br_mode, '###backup folder name is none')
                return [NG, '###backup folder name is none']

            self.make_log_file_name(CLSTER_NAME,node_id,br_mode,**kwargs)

            ###################
            #check mode
            ###################
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check Start')

            node_list=','.join(node_name)
            ret = cflct.chk_mode_state( "r", topology_name, node_list)
            if ret==1:
                return [NG, "resotre already runnning"]
            elif ret==-1:
                msg='#### backup runnning then err'
                return [NG, msg]
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Mode check OK ')



            ###############
            ####Restore####
            ###############
            retArray = self.restore_cluster_sub(node_id, token, topology_name, domain, node_name,**kwargs)

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            return retArray

        except Exception,e:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Exception !! #####')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### type   :'+ str(type(e)))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### args   :'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### message:'+ str(e.args))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### e_self :'+str(e))
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### trace  :%s' %(traceback.format_exc()) )

            #set mode none
            cflct.set_mode_state(topology_name , MODE_NONE)

            #raise
            return [NG, traceback.format_exc()]

            raise

    #####################
    #Restore Module
    #####################
    def restore_cluster_sub(self, node_id, token, topology_name, domain, node_name,**kwargs):

        #####################
        #R predefine
        #####################

        br_mode="r"
        CLSTER_NAME = "TS_" + domain +"_" + topology_name


        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Restore Start ')

        if not 'restore_name' in kwargs:
            return [NG, 'restore folder name is required']

        #get restore folder name
        restore_folder_name = self.get_restore_foldername(**kwargs)

        BACKUP_FOLDER_RNAME=CLSTER_NAME + "_ID%s" %(node_id) +"/" + restore_folder_name


        self.br_log(node_id, CLSTER_NAME, br_mode, '###  Input Parameter  ####')
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_id              : %s' % node_id)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'token                : %s' % token)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'topology_name        : %s' % topology_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'domain               : %s' % domain)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'node_name            : %s' % node_name)
        self.br_log(node_id, CLSTER_NAME, br_mode, 'kwargs[restore_name]  : %s' %(kwargs['restore_name']))

        self.br_log(node_id, CLSTER_NAME, br_mode, 'restore_folder: %s' % BACKUP_FOLDER_RNAME)

        ret=self.set_system_param(CLSTER_NAME,node_id,br_mode)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### set_system_param err'  )
            msg='#### set_system_param err'
            return [NG, msg]


        #####################
        #R define
        #####################
        SAVE_DIR_NAME=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/server"
        SAVE_DIR_NAME_SWITCH=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME +"/switch"

        SERVER_INFO_FILE_NAME="serv_info"
        SERVER_LIST_FILE=SAVE_DIR_NAME+"/"+SERVER_INFO_FILE_NAME

        NODE_INFO_FILE_NAME="node_info"
        NODE_LIST_FILE=BASE_DIR_NAME+"/"+ BACKUP_FOLDER_RNAME + "/" + NODE_INFO_FILE_NAME

        ##################
        #set token
        ##################
        ret = self.set_token(CLSTER_NAME, node_id, br_mode, token)

        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '###get token ng')
            return [NG, '###get token ng']

        #start time get
        time1 = time.time()


        ######################
        #R Make Server info list
        ######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Make Server info list')

        #make storage server save val
        server_cnt=1
        server_info_num=4
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]

        ########################
        #R Set Storage Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        #retdata = self.set_server_info(node_id, server_info[0],STORAGE_SERVER_NAME, CLSTER_NAME, br_mode)
        retdata = self.set_server_info(node_id, server_info[0],self.storage_server_name, CLSTER_NAME, br_mode)

        if 0 != retdata[0]:
            #self.logger.debug('server info set err')
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### server info set err')
            #return self._fail(msg='server info set err')
            msg='server info set err'
            return [NG, msg]

        ################
        #R Set Exec_User
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Exec_User')
        EXEC_USER=server_info[STORAGE_SV][USER_INDEX]


        #####################################
        #R Set Storage Server info
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Storage Server info')

        i=STORAGE_SV
        self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start  index=%d' %( i))
        ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err index=%d' %( i) )
            msg='make_exec err'
            return [NG, msg]

        #######################
        #R Get backup ServerInfo
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Get backup ServerInfo ')

        d = datetime.datetime.today()
        tm= d.strftime("%H%M%S")
        file_path='/tmp/' + NODE_INFO_FILE_NAME + tm

        cmd="scp  root@%s:%s %s  " %(server_info[STORAGE_SV][IP_INDEX] , NODE_LIST_FILE, file_path)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            NODE_LIST_FILE
            msg='R node_info trans err'
            return [NG, msg]


        f=open(file_path, 'r')
        backup_node_info = json.load(f)
        f.close()

        cmd="rm %s " %(file_path)
        commands.getoutput(cmd)

        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Get backup ServerInfo :%s' %(backup_node_info))


        ########################
        #R Get server name for backupfile
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Get server name for storage server')
        ret_restore_info = self.get_node_info_from_serverFile(node_id, CLSTER_NAME, br_mode, backup_node_info)

        server_node_name=ret_restore_info[SV_NAME]
        server_num=len(server_node_name)
        server_cnt=server_num+1

        tmp_server_info = server_info[STORAGE_SV]
        ###################
        #resize server info
        ###################
        server_info = [["null" for j in range(server_info_num)] for i in range(server_cnt)]
        server_info[STORAGE_SV] = tmp_server_info

        switch_node_name = ret_restore_info[SW_NAME]
        switch_num=len(switch_node_name)
        switch_node_name_char=','.join(switch_node_name)


        if (0 == server_num) and ( 0 == switch_num):
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### sever, switch num is 0 then "no action"')
            return [OK, 'Restore ok']

        ########################
        #R Check Cluster node
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### R Check Cluster node')

        backupedCluster_node = server_node_name + switch_node_name
        nowClaster_node      = node_name

        ret = self.check_novacluster_node(CLSTER_NAME,node_id,br_mode, backupedCluster_node,nowClaster_node)
        if 0 != ret:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#R Check Cluster node  check ng')
            msg='R Check Cluster node  check ng'
            return [NG, msg]



        ########################
        #R Set NovaClaster Server info
        ########################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server info')

        for i in range(1, server_cnt):
            self.br_log(node_id, CLSTER_NAME, br_mode, 'Set Server info server_node_name[%s]=%s' %( (i-1), server_node_name[i-1] ) )

            retdata = self.set_server_info(node_id, server_info[i], server_node_name[i-1], CLSTER_NAME, br_mode)
            if 0 != retdata[0]:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set Server info  server_node_name=%s' %(server_node_name[i-1] ) )
                msg='Set Server info set server_node_name=%s' %(server_node_name[i-1])
                return [NG, msg]


        #####################################
        #R Set NovaClaster Server Make Exec Env
        #####################################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Set NovaClaster Server Make Exec Env')

        for i in range(1, server_cnt):
            self.br_log(node_id, CLSTER_NAME, br_mode, 'backup exec start all=%d index=%d' %(server_cnt, i))
            ret=self.make_exec(EXEC_USER, server_info[i][IP_INDEX], server_info[i][USER_INDEX], server_info[i][PW_INDEX], FILEDIR, br_mode, node_id, CLSTER_NAME)
            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### backup exec start err all=%d index=%d' %(server_cnt, i) )

                #return self._fail(msg='make_exec err')
                msg='make_exec err'
                return [NG, msg]



        #######################
        #R Check Directory
        #######################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory')

        cmd='ssh root@%s ls -d  %s 2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Check Directory Err %s is no directory' %(SAVE_DIR_NAME))
            #return self._fail(msg='Check Directory ')
            msg='Check Directory '
            return [NG, msg]

        #DDD debug DDD
        #switch_num=0

        #DDD debug
        if DEBUG == "ON":
            switch_num=0
        #################
        #R switch restore
        #################
        if switch_num != 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run Switch restore Start')

            psbk=psbk_manager.psbk_manager(self.storage_server_name, SAVE_DIR_NAME_SWITCH,self.logObj)

            self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH Call psbk.set_PS_list')

            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH_NODE_LIST :List(char) %s ' %(switch_node_name_char))

            ret=psbk.set_PS_list(switch_node_name_char)
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### SWITCH psbk.set_PS_list Err')
                msg='#### SWITCH psbk.set_PS_list Err psbk.set_PS_list Err)'
                return [NG, msg]


            psbk.set_auth(self.token)
            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Call psbk.exec_restore()')
            ret=psbk.exec_restore()
            if 0 != ret:
                self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  psbk.exec_restore() Err')
                msg='####SWITCH  psbk.exec_restore() '
                return [NG, msg]


            self.br_log(node_id, CLSTER_NAME, br_mode, '####SWITCH  Run Switch restore End')

        #DDD
        if DEBUG == "ON":
            server_num = 0


        if server_num == 0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Server node is 0 then "no action"')
            return [OK, 'Restore ok']


        #######################
        #R Dell Restore Status File
        #######################
        cmd='ssh root@%s rm -rf %s/status_r*  2> /dev/null' %(server_info[STORAGE_SV][IP_INDEX],  SAVE_DIR_NAME,)
        ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
        if ret!=0:
            self.br_log(node_id, CLSTER_NAME, br_mode, '#### Dell Restore Status File Err ')
            msg='Dell Restore Status File Err '
            return [NG, msg]

        ################
        #R Run backup
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Run backup')

        for i in range(START_INDEX, server_cnt):
            #copy br_agent_update
            cmd='scp %s/%s %s@%s:%s' %(BR_AGENT_SRC_DIR, BR_AGENT, server_info[i][USER_INDEX], server_info[i][IP_INDEX], BR_AGENT_DST_DIR)
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)

            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### copy br_agent_update [%s] err ' %(server_info[i][IP_INDEX]) )
                msg='copy br_agent_update [%s] err ' % (server_info[i][IP_INDEX])
                return [NG, msg]

            #exec br_agent
            cmd='ssh root@%s  /boot/%s  %s  %s  r %s' %(server_info[i][IP_INDEX],BR_AGENT,  server_info[i][USER_INDEX], server_info[i][PW_INDEX], SAVE_DIR_NAME)
            ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
            if ret!=0:
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### make run backup err ')
                msg='make run backup err'
                return [NG, msg]


        ################
        #R Start status check
        ################
        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Start status check')

        while 1:
            all_ret=0

            ################
            #R time cal
            ################

            #end time get
            time2 = time.time()
            timedf = time2-time1

            timedf_int = int(timedf)
            HH= timedf_int / 3600
            #SS= timedf_int % 3600
            MM= (timedf_int % 3600)/60
            SS= timedf_int % 60

            #logout
            self.br_log(node_id, CLSTER_NAME, br_mode, "#### loop  status check Total Time: %s:%s:%s (h:m:s)" %(HH,MM,SS) )


            for i in range(START_INDEX, server_cnt):

                ################
                #R status roop check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_END_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                self.br_log(node_id, CLSTER_NAME, br_mode, 'server_info[%s] IP=%s SV_NAME=%s ' %(i, server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                all_ret = ret + all_ret

                ################
                #R status view
                ################
                cmd='ssh root@%s tail -n 1 %s/status_r_%s  2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], SAVE_DIR_NAME, server_info[i][NAME_INDEX])
                ret = self.shellcmd_exec_br_state(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)


                ################
                #R status ng check
                ################
                cmd='ssh root@%s grep -wq %s %s/status_r_%s 2> /dev/null ' %(server_info[STORAGE_SV][IP_INDEX], R_NG_STATUS, SAVE_DIR_NAME, server_info[i][NAME_INDEX])

                ret = self.shellcmd_exec(EXEC_USER,br_mode, node_id, CLSTER_NAME, cmd)
                if ret==0:
                    self.br_log(node_id, CLSTER_NAME, br_mode, '#### Status roop check [backup_ng] Err IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX]) )
                    msg='Status roop check [backup_ng] IP:%s NAME:%s' %(server_info[i][IP_INDEX], server_info[i][NAME_INDEX])
                    return [NG, msg]


                time.sleep(self.interval_time )

            if all_ret==0:
                break

            #force-stop
            if os.path.exists(FILEDIR+R_STOP_FILENAME) :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### force stop')
                os.remove(FILEDIR+R_STOP_FILENAME)
                msg='#### force stop '
                return [NG, msg]
                break

            #time-out
            if HH >= self.loop_timeout_m :
                self.br_log(node_id, CLSTER_NAME, br_mode, '#### HH is %s hour Over stop' %(self.loop_timeout_m))
                msg='#### HH is %s hour Over stop ' %(self.loop_timeout_m)
                return [NG, msg]
                break


        self.br_log(node_id, CLSTER_NAME, br_mode, '#### Complete Success')


        return [OK, 'Restore ok']
