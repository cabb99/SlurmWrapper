#S=slurm.options()
#S.change('aa','bb')
#S.help('qq')
#S.options()

import subprocess,os,pickle
from daemon import Daemon

#Locate folder
def my_path():
	import p
	path=str(p).split()[-1][1:-7] #BlackMagic
	return path

# Time manipulation
def stime(t):
    '''Convert from seconds to slurm time format'''
    s=0    
    D=t.split('-')
    T=[int(i) for i in D[-1].split(':')]
    if len(D)==2:
        s+=int(D[0])*24*60*60
        if len(T)==1:#days-hours
            s+=T[0]*60*60
        elif len(T)==2:#days-hours:minutes
            s+=T[0]*60*60+T[1]*60
        elif len(T)==3:#days-hours:minutes:seconds
            s+=T[0]*60*60+T[1]*60+T[2]
    elif len(D)==1:
        if len(T)==1: #minutes
            s+=T[0]*60
        elif len(T)==2: #minutes:seconds
            s+=T[0]*60+T[1]
        elif len(T)==3: #hours:minutes:seconds
            s+=T[0]*60*60+T[1]*60+T[2]
    return s

def sformat(t):
    t=int(t)
    s=t%60
    t-=s
    m=t%(60*60)/60
    t-=m*60
    h=t%(24*60*60)/60/60
    t-=h*60*60
    d=t/24/60/60
    if d>0:
        return '%i-%02i:%02i:%02i'%(d,h,m,s)
    elif h>0:
        return '%02i:%02i:%02i'%(h,m,s)
    else:
        return '%02i:%02i'%(m,s)

#Parse SlurmQueue

def SlurmQueue():
    '''Call slurm queue and return a list of dictionaries'''
    squeue=subprocess.check_output(['squeue']) #Call squeue
    squeue=squeue.split('\n') #Split per line
    squeue=[s.split() for s in squeue] #Split per column
    try:
        squeue={int(s[0]):dict(zip(squeue[0][1:],s[1:])) for s in squeue[1:-1]} #Create a Dict and clean
    except ValueError: #jobid not always numbers: 359330_[1-227]
        sq=squeue    
        squeue={}        
        for s in sq[1:-1]:
            try:
                squeue.update({int(s[0]):dict(zip(sq[0][1:],s[1:]))})
            except ValueError:
                squeue.update({s[0]:dict(zip(sq[0][1:],s[1:]))})
    return squeue

#Manipulate Slurm Options
class SlurmOptions(object):    
        
    def __init__(self):
        self.S={}               
        #Known options
        self.define_option('job-name'          ,'Unnamed'  ,True   ,str    ,'Assign a job name')
        self.define_option('export'            ,'ALL'      ,True   ,str    ,'Exports all environment variables to the job.  See our FAQ for details.')
        self.define_option('ntasks'            ,2          ,True   ,int    ,'Number of tasks per job. Used for MPI jobs')
        self.define_option('nodes'             ,1          ,True   ,int    ,'Number of nodes requested.')
        self.define_option('ntasks-per-node'   ,1          ,True   ,int    ,'Number of tasks per node')
        self.define_option('partition'         ,'commons'  ,True   ,int    ,'Specify the name of the Partition (queue) to use')
        self.define_option('time'              ,'08:00:00' ,True   ,'stime','Maximum run time needed')
        self.define_option('cpus-per-task'     ,1          ,False  ,int    ,'Number processes per task')
        self.define_option('mem-per-cpu'       ,'1024M'    ,False  ,int    ,'Maximum amount of physical memory used per process')
        self.define_option('mail-user'         ,'mail'     ,False  ,str    ,'Email address for job status messages.')
        self.define_option('mail-type'         ,'ALL'      ,False  ,['ALL','BEGIN','END','FAIL','REQUEUE'] ,'Will notify when job reaches BEGIN, END, FAIL or REQUEUE.')
        self.define_option('workdir'             ,''         ,False  ,str     ,'Remote cwd')
        self.define_option('output'            ,''         ,False  ,str     ,'Standard Output path')
        self.define_option('error'             ,''         ,False  ,str     ,'Standard Error path')
        

    def define_option(self,variable,default_value,explicit,var_type,description):    
        self.S.update({variable:{'value':default_value,'add':explicit,'check':var_type,'help':description}})
    
    def option(self,variable,value,explicit=True,var_type='pass',description=''):
        if variable in self.S.keys():
            assert self.check(self.S[variable],value),'Not a valid value for %s: %s'%(variable,value)
            self.S[variable]['value']=value
            self.S[variable]['add']=True
        else:
            self.define_option(variable,value,explicit,var_type,description)
    
    def del_option(self,option):
        self.S[option]['add']=False        

    def help(self, option=''):
        if option=='':
            return 'Select from:\n %s'%'\n'.join(S.keys)   
        return self.S[option]['help']        


    def get(self,option):
        if option in self.S.keys() and self.S[option]['add']:
            return self.S[option]['value']
        else:
            return None

    def options(self):
        t=self.S.keys()
        t.sort()
        return t
   
    def check(self,Var,value):
        a=Var['check']
        if type(a)==type:
            return True if type(value)==a else False
        elif type(a)==list:
            return True if type(value) in a else False
        elif type(a)==str:
            if a=='stime':
                try:
                    test=value.split('-')
                    test2=a[-1].split(':')
                    if len(test2)==3 and 0<len(test)<=2:
                        try:
                            [int(a) for a in test2]
                            return True
                        except:
                            return False
                    return False
                except:
                    return False
            if a=='pass':
                pass
        elif a==None:
            return True if value==None else False

    def __str__(self):
        s=''   
        s+='#!/bin/bash\n'
        for opt in self.options():
            if self.S[opt]['add']:        
                s+='#SBATCH --%s=%s\n'%(opt,str(self.S[opt]['value']))
        return s

#Track Slurm Job

class SlurmJob():
    def __init__(self,commands,Soptions=SlurmOptions(),config_file='slurm_temp.conf'):
        self.commands=commands
        self.restart_commands=commands
        self.config_file=config_file        
        '''Send the job to the slurm queue'''
        #Write the config file
        with open(config_file,'w+') as f:
            f.write(str(Soptions))
            f.write('\necho "I ran on:"\ncat $SLURM_JOB_NODELIST\n')
            f.write(commands)
        #Submit the file
        self.jobid=int(subprocess.check_output(['sbatch',config_file]).split()[-1])
        self.slurm_options=SlurmOptions()
        self.slurm_options.S=Soptions.S.copy()
        if self.slurm_options.get('workdir'):        
            self.path=self.slurm_options.get('workdir')       
        else:
            self.path=os.getcwd()
     
        if self.slurm_options.get('output'):
            self.stdout=self.slurm_options.get('output')
        else:        
            self.stdout='%s/slurm-%i.out'%(self.path,self.jobid)        

        if self.slurm_options.get('error'):
            self.stderr=self.slurm_options.get('error')
        elif self.slurm_options.get('output'):
            self.stderr=self.slurm_options.get('output')        
        else:
            self.stderr='%s/slurm-%i.out'%(self.path,self.jobid)
        self.save()


    def status(self):
        '''Get the status of the job'''
        Q=SlurmQueue()    
        #Get the status on the queue
        if self.jobid in Q.keys():
            return Q[self.jobid]['ST']
        #If not in the queue get the status in the output
        else:
            return 'END'

    def WallTime(self):
        if self.status()=='PD':
            return stime(self.slurm_options.get('time'))   
        elif self.status()=='R':
            return stime(self.slurm_options.get('time'))-stime(SlurmQueue()[self.jobid]['TIME'])
        else:
            return 0

    def cancel(self):
        subprocess.call(['scancel',str(self.jobid)])

    def save(self):
        with open('%s/Tracking/slurm-%i.pickle'%(my_path(),self.jobid),'w+') as f:     
            pickle.dump(self,f)

    def delete(self):
        os.remove('%s/Tracking/slurm-%i.pickle'%(my_path(),self.jobid))

    def restart():
        if self.status=='END':
            self.delete()
            self.__init__(self,sekf.restart_commands,self.slurm_config,self.config_file)

class MyDaemon(Daemon):
    def run(self):
        Log_file=my_path()+'Tracking/daemon.log'   
        Still_running=True
        while Still_running:
            daemon_log=open(Log_file,'w+')
            
            #Kill daemon when pidfile is erased
            try:
                pf = file(self.pidfile,'r')
                pid,hostname = pf.read().strip().split('\t')
                pid = int(pid)
                pf.close()
                #daemon_log.write(str(self.pidfile))
            except IOError:
                pid = None 
                            
            if not pid:
                daemon_log.write("Daemon killed remotely.\nDaemon stop at %s\n"%time.ctime(time.time()))                
                daemon_log.close()   
                break
            
            #List jobs
            daemon_log.write("Status at %s\n"%time.ctime(time.time()))
            joblist=[]
            for f in glob.glob("%s/Tracking/slurm-*.pickle"%my_path()):
                with open(f) as F:
                    joblist+=[pickle.load(F)]
            
            wait_time=60*60 #Check every hour just in case            
            for job in joblist:
                os.chdir(job.path)
                if job.status()='END':
                    Due2Time=False                    
                    with open(last_slurm) as handle:
                        for line in handle:
                            if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                                Due2Time=True
                    if Due2Time:
                        job.restart()
                    else:
                        job.delete()
                else:
                    wait_time=job.WallTime() if job.WallTime()<wait_time else wait_time
                time.sleep(10+wait_time)
   



################################################################################
            #print dir(subprocess)
            daemon_log.write("Status at %s\n"%time.ctime(time.time()))
            Still_running=False
            #Get a list of all subfolders
            subfolders=next(os.walk(Folder))[1]
                       
            #Call slurm queue
            jobid_list=[s['JOBID'] for s in self.SlurmQueue()]
            
            for folder in subfolders:
                #Enter folder    
                f=Folder+folder
                os.chdir(f)    
                
                #Find most recent Output from slurm
                last_time=0
                last_slurm=None
                for f in glob.glob("slurm*.out"):
                    last_slurm=f if os.path.getmtime(f) > last_time else last_slurm
                    last_time=os.path.getmtime(f)
                if last_slurm==None: #First run
                    Running=True
                    Still_running=True
                    daemon_log.write('MD in %s without log\n'%folder)
                    continue        

                #Check if running
                Running=False    
                try:
                    #Open slurm.jobid and check if in Slurm Queue        
                    with open('slurm.jobid') as jobid:
                        jid=jobid.readline().split()[-1]
                    if jid in jobid_list:
                        Running=True
                        Still_running=True
                        daemon_log.write('MD in %s still running\n'%folder) 
                        continue
                #If not slurm.jobid check if most recent output older than md.log    
                except IOError:
                    #Check most recent log
                    for f in glob.glob("*.log"):
                        if os.path.getmtime(f) > last_time:
                            Running=True
                            break
                    if Running:              
                        Still_running=True
                        daemon_log.write('MD in %s still running\n'%folder)               
                        continue
                
                #Check if the ran stop due to time
                Due2Time=False    
                handle=open(last_slurm)
                for line in handle:
                    if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                        Due2Time=True
                handle.close()
                
                if not Due2Time:
                    daemon_log.write('MD in %s has ended\n'%folder)
                    continue
                
                #Restart if needed
                if not Running and Due2Time:
                    #Run sbatch
                    with open('slurm.jobid','w+') as jobid:        
                        subprocess.call(['sbatch','rst.slurm'],stdout=jobid)
                    daemon_log.write('Restarting %s\n'%folder)
                    Still_running=True
            daemon_log.close()            
            time.sleep(20*60)
        daemon_log=open(Log_file,'a+')        
        daemon_log.write("No more processes active, Daemon stop at %s\n"%time.ctime(time.time()))  
        daemon_log.close()

if __name__=='__main__':
    S=SlurmOptions()
    S.option('ntasks',1)
    S.option('workdir','/home/cab22/Git/Daemon/1')
    #S.option('output','/home/cab22/Git/Daemon/2/oo')
    #S.option('error','/home/cab22/Git/Daemon/3/aa')
    j=SlurmJob('sleep 10\nhostname\nsleep 10\n',S)
    print j.jobid, j.status()
    #import time
    #time.sleep(5)    
    while j.status()<>'END':
        print j.jobid,j.WallTime(),j.status()
    daemon = MyDaemon('%s/Tracking/AutoRestart.pid'%my_path())

    if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                    daemon.start()
            elif 'stop' == sys.argv[1]:
                    daemon.stop()
            elif 'restart' == sys.argv[1]:
                    daemon.restart()
            elif 'test' == sys.argv[1]:
                daemon.pidfile=('%s/Tracking/dummy.pid'%my_path())                    
                daemon.run()
            else:
                    print "Unknown command"
                    sys.exit(2)
            sys.exit(0)
    else:
            print "usage: %s start|stop|restart|test" % sys.argv[0]
            sys.exit(2)

