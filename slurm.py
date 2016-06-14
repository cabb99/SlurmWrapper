#!/usr/bin/env python

##################
#   Parameters   #
##################

name='SlurmTracker'
Tracking='Tracking'
pid='SlurmTracker.pid'
log='SlurmTracker.log'

import subprocess
import os
import pickle
import time
from daemon import Daemon


#Locate folder
def my_path():
	import p
	path=str(p).split()[-1][1:-7] #BlackMagic
	return path

# Time manipulation
def stime(t):
    '''Convert slurm time format to seconds'''
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
    '''Convert from seconds to slurm time format'''
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
        '''Define all default and required options'''        
        self.S={}               
        #Known options
        self.define_option('job-name'          ,'Unnamed'  ,True   ,str    ,'Assign a job name')
        self.define_option('export'            ,'ALL'      ,True   ,str    ,'Exports all environment variables to the job.  See our FAQ for details.')
        self.define_option('ntasks'            ,1          ,True   ,int    ,'Number of tasks per job. Used for MPI jobs')
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
    
    def read(self,script):
        script_args={}
        print "Old Configuration:"
        with open(script) as S:
            for line in S:
                if len(line)>10 and line[:7]=='#SBATCH' and len(line.split('--'))==2:
                    var,val=line.split('--')[1].split('=')
                    print line[:-1]
                    self.option(var,val.split('\n')[0])
    
    def define_option(self,variable,default_value,explicit,var_type,description):
        '''Define new options after init'''
        self.S.update({variable:{'value':default_value,'add':explicit,'check':var_type,'help':description}})
    
    def option(self,variable,value,explicit=True,var_type='pass',description=''):
        '''Change the value of an option, if non-existant then it will be defined'''
        if variable in self.S.keys():
            assert self.check(self.S[variable],value),'Not a valid value for %s: %s'%(variable,value)
            self.S[variable]['value']=value
            self.S[variable]['add']=True
        else:
            self.define_option(variable,value,explicit,var_type,description)
    
    def del_option(self,option):
        '''Do not use an option, it will not be undefined'''
        self.S[option]['add']=False        

    def help(self, option=''):
        '''Return the option description'''
        if option=='':
            return 'Select from:\n %s'%'\n'.join(S.keys)   
        return self.S[option]['help']        


    def get(self,option):
        '''Return the value of an option'''
        if option in self.S.keys() and self.S[option]['add']:
            return self.S[option]['value']
        else:
            return None

    def options(self):
        '''Return all defined options'''        
        t=self.S.keys()
        t.sort()
        return t
   
    def check(self,Var,value):
        '''Check if the option format is correct'''
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
        '''Print the options as expected by slurm'''        
        s=''   
        s+='#!/bin/bash\n'
        for opt in self.options():
            if self.S[opt]['add']:        
                s+='#SBATCH --%s=%s\n'%(opt,str(self.S[opt]['value']))
        return s


#Track Slurm Job

class NewSlurmJob():
    def __init__(self,commands,Soptions=SlurmOptions(),config_file='slurm_temp.conf'):
        '''Start a job'''    
        self.commands=commands
        self.restart_commands=commands
        self.config_file=config_file        
        '''Send the job to the slurm queue'''
        #Write the config file
        with open(config_file,'w+') as f:
            f.write(str(Soptions))
            f.write('\necho "I ran on:"\ncat $SLURM_JOB_NODELIST\n')
            f.write(commands)
        self.slurm_options=SlurmOptions()
        self.slurm_options.S=Soptions.S.copy()
        if self.slurm_options.get('workdir'):        
            self.path=self.slurm_options.get('workdir')       
        else:
            self.path=os.getcwd()
     
        if self.slurm_options.get('output'):
            if self.slurm_options.get('output')[0]=='/':
                self.stdout=self.slurm_options.get('output')
            else:
                self.stdout='%s/%s'%(self.path,self.slurm_options.get('output'))
        else:        
            self.stdout=None       

        if self.slurm_options.get('error'):
            if self.slurm_options.get('output')[0]=='/':
                self.stderr=self.slurm_options.get('error')
            else:
                self.stderr='%s/%s'%(self.path,self.slurm_options.get('error'))
        elif self.slurm_options.get('output'):
            self.stderr=self.slurm_options.get('output')        
        else:
            self.stderr=None
        self.state='OFF'
        self.jobid=None
    
    def run(self):
        #Submit the file
        self.jobid=int(subprocess.check_output(['sbatch',self.config_file]).split()[-1])
        if self.stdout==None:
            self.stdout='%s/slurm-%i.out'%(self.path,self.jobid)
        if self.stderr==None:
            self.stderr='%s/slurm-%i.out'%(self.path,self.jobid)
        self.state='Starting'
        self.save()

    def status(self):
        '''Get the status of the job'''
        Q=SlurmQueue()
        #Get the status on the queue
        if self.jobid in Q.keys():
            self.state=Q[self.jobid]['ST']
        elif os.path.isfile(self.stdout):
            self.state='END'
        self.clean()
        return self.state

    def WallTime(self):
        '''Get max time before job stops'''
        if self.status() in ['PD','Starting']:
            return stime(self.slurm_options.get('time'))   
        elif self.status()=='R':
            return stime(self.slurm_options.get('time'))-stime(SlurmQueue()[self.jobid]['TIME'])
        elif self.status() in ['END','OFF']:
            return 60*60
        else:
            return 0

    def cancel(self):
        '''Cancel the job'''
        subprocess.call(['scancel',str(self.jobid)])
        self.remove()
        

    def save(self,pickle_path='%s/Tracking'%my_path()):
        '''add the job to the tracker'''
        self.pickle_path=pickle_path
        with open(self.pickle_path,'w+') as f:     
            pickle.dump(self,f)
            
    def remove(self):
        '''remove the job from the tracker'''
        try:
            os.remove(self.pickle_path)
        except OSError:
            pass

    def clean():
        if self.status in ['END','OFF']:
            self.remove()

class SlurmJob(NewSlurmJob):
    def __init__(self,script):
        '''Start a job'''    
        self.config_file=config_file
        self.slurm_options=SlurmOptions()
        self.slurm_options.read(script)
        if self.slurm_options.get('workdir'):        
            self.path=self.slurm_options.get('workdir')       
        else:
            self.path=os.getcwd()
     
        if self.slurm_options.get('output'):
            if self.slurm_options.get('output')[0]=='/':
                self.stdout=self.slurm_options.get('output')
            else:
                self.stdout='%s/%s'%(self.path,self.slurm_options.get('output'))
        else:        
            self.stdout=None       

        if self.slurm_options.get('error'):
            if self.slurm_options.get('output')[0]=='/':
                self.stderr=self.slurm_options.get('error')
            else:
                self.stderr='%s/%s'%(self.path,self.slurm_options.get('error'))
        elif self.slurm_options.get('output'):
            self.stderr=self.slurm_options.get('output')        
        else:
            self.stderr=None
        self.state='OFF'
        self.jobid=None

class SlurmTracker(Daemon):
    def checkpid(self):
        try:
            pf = file(self.pidfile,'r')
            pid,self.hostname = pf.read().strip().split('\t')
            self.pid = int(pid)
            pf.close()
        except IOError:
            self.pid = None
            self.hostname = None
            self.status='Not running'   
    
    def listjobs(self):
        joblist=[]
        for f in glob.glob("%s/Tracking/slurm-*.pickle"%my_path()):
            with open(f) as F:
                joblist+=[pickle.load(F)]
        self.joblist=joblist
        
    def checkjobs(self):
        self.wait_time=self.max_wait_time
        for job in self.joblist:
                os.chdir(job.path)
                if job.status()=='END':
                    Due2Time=False                    
                    with open(last_slurm) as handle:
                        for line in handle:
                            if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                                Due2Time=True
                    if Due2Time:
                        job.restart()
                    else:
                        job.remove()
                else:
                    self.wait_time=job.WallTime() if job.WallTime()<wait_time else wait_time
    
    def run(self,max_wait_time=60*60):
        Log_file='%s/Tracking/SlurmTracker.log'%my_path() 
        self.pickle_path='%s/Tracking/SlurmTracker.pickle'%my_path()
        self.max_wait_time=max_wait_time
        while True:
            self.log=open(Log_file,'w+')
            
            #Kill daemon when pidfile is erased
            self.checkpid()
            if not pid:
                self.log.write("Daemon killed remotely.\n")
                self.log.write("Daemon stop at %s\n"%time.ctime(time.time()))              
                self.log.close()
                break
            
            #List jobs
            self.log.write("Job list at %s\n"%time.ctime(time.time()))
            self.listjobs()
            self.log.write(joblist)
           
            #Check every job         
            self.checkjobs()
            self.log.close()            
            time.sleep(10+self.wait_time) #If end near, check in 10s
            with open(self.pickle_path,'w+') as f:     
                pickle.dump(self,f)
        self.log=open(Log_file,'a+')        
        self.log.write("No more processes active, Daemon stop at %s\n"%time.ctime(time.time()))  
        self.log.close()
 
class SlurmCommander():
    def __init__(self):
        #Check if tracker has started
        self.pidfile='%s/Tracking/SlurmTracker.pid'%my_path()
        self.pickle_file='%s/Tracking/SlurmTracker.pickle'%my_path()
        self.tracking_path='%s/Tracking/'%my_path()
        try:
            pf = file(self.pidfile,'r')
            pid,self.hostname = pf.read().strip().split('\t')
            self.pid = int(pid)
            pf.close()
            self.state='Running'
            #daemon_log.write(str(self.pidfile))
        except IOError:
            self.pid = None
            self.hostname = None
            self.state='Not running'

            

    def start(self,slurm_script):
        '''Starts a job'''
        #Start a slurm job
        Job=SlurmJob(slurm_script)
        Job.run()
        if self.state=='Not Running':
            S = SlurmTracker('%s/Tracking/SlurmTracker.pid'%my_path())
            S.start()
        
    def track(self,script,jobid):
        print "Not implemented"
        #Need information from the script
        #May try to find jobid from last output
        #Job=SlurmJob(slurm_script)
        #Job.run()
        #if self.state='Not Running':
        #    S = SlurmTracker('%s/Tracking/SlurmTracker.pid'%my_path())
        #    S.start()
            
    def list(self,):
        J=glob.glob("%s/Tracking/slurm-*.pickle"%my_path())
        '\n'.join([j.split('/')[-1] for j in J])
            
    def cancel(self,jobid):
        print "Not implemented"
    
    def test(self,test=False):
        try:
            print SlurmQueue()
            print "function SlurmQueue: succeded"
        except:
            pass
            print "function SlurmQueue: failed"
        self.status()
        
    def status(self,):
        try:
            pf = file(self.pidfile,'r')
            pid,self.hostname = pf.read().strip().split('\t')
            self.pid = int(pid)
            pf.close()
            self.state='Running'
            #daemon_log.write(str(self.pidfile))
        except IOError:
            self.pid = None
            self.hostname = None
            self.state='Not running'
        if self.state=='Running':
            with open(self.pickle_file) as F:
                print pickle.load(F)
        else:
            print "Not Running"
        

if __name__=='__main__':
    
    import argparse,sys
    main=SlurmCommander()
    
    # create the top-level parser
    parser = argparse.ArgumentParser(description='This software contains utilities to execute slurm scripts in a tracked environment. It will restart the scripts when stopped due to time limit')
    parser.add_argument('--test', action='store_true', help='Write the output of a command without executing it')
    subparsers = parser.add_subparsers()#help='Possible actions')
    
    # Start parser
    start_parser = subparsers.add_parser('start', help='Starts a job from a slurm script')
    start_parser.add_argument('slurm_script', help='Slurm script') #Slurm script
    start_parser.set_defaults(func=main.start)
    
    # Tracking parser
    track_parser = subparsers.add_parser('track', help='Tracks a slurm job that already started')
    start_parser.add_argument('slurm_script', help='Slurm script') #Slurm script
    track_parser.add_argument('job', help='Slurm job') #Slurm job
    track_parser.set_defaults(func=main.track)
    
    # Status
    list_parser = subparsers.add_parser('status', help='Prints status of the tracker and the list jobs being currently tracked')
    list_parser.set_defaults(func=main.list)
    
    # Cancel jobs
    cancel_parser = subparsers.add_parser('cancel', help='Cancels a job')
    cancel_parser.add_argument('job', help='Slurm job') #Slurm job
    list_parser.set_defaults(func=main.cancel)
    
    # Test
    test_parser = subparsers.add_parser('test', help='Tests if every command on the scripts works correctly')
    test_parser.set_defaults(func=main.test)
    
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args()
    print args
    args.func(args)
    
    '''
    #Testing    
    #Start options
    #S=SlurmOptions()
    #Define new option
    #Define old option
    #Change new option
    #Change old option
    #Give bad option value
    #Get the value of an option
    #Print option help
    #Start job
    #Cancel job
    #Restart job
    #Save job
    #Delete job


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
    '''
