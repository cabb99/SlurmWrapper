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
import glob
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
        self.define_option('partition'         ,'commons'  ,True   ,str    ,'Specify the name of the Partition (queue) to use')
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
        with open(script) as S:
            for line in S:
                if len(line)>10 and line[:7]=='#SBATCH' and len(line.split('--'))==2:
                    var,val=line.split('--')[1].split('=')
                    #print var,val.split('\n')[0]
                    self.option(var,val.split('\n')[0])
    
    def define_option(self,variable,default_value,explicit,var_type,description):
        '''Define new options after init'''
        self.S.update({variable:{'value':default_value,'add':explicit,'check':var_type,'help':description}})
    
    def option(self,variable,value,explicit=True,var_type='pass',description=''):
        '''Change the value of an option, if non-existant then it will be defined'''
        if variable in self.S.keys():
            if type(self.S[variable]['check'])==type:
                value=self.S[variable]['check'](value)
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
            if type(value)==a:
                return True
            else:
                try: 
                    a(value)
                    return True
                except ValueError:
                    return False
        elif type(a)==list:
            return True if type(value) in a else False
        elif type(a)==str:
            if a=='stime':
                try:
                    test=value.split('-')
                    test2=test[-1].split(':')
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
        
    def restart(self):
        self.run()

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
        else:
            return 0

    def cancel(self):
        '''Cancel the job'''
        subprocess.call(['scancel',str(self.jobid)])
        self.remove()
        

    def save(self,):
        '''add the job to the tracker'''
        self.pickle_file='%s/Tracking/slurm-%i.pickle'%(my_path(),self.jobid)
        with open(self.pickle_file,'w+') as f:     
            pickle.dump(self,f)
            
    def remove(self):
        '''remove the job from the tracker'''
        try:
            os.remove(self.pickle_file)
        except OSError:
            pass

    def clean(self):
        if self.status in ['END','OFF']:
            self.remove()

class SlurmJob(NewSlurmJob):
    def __init__(self,config_file):
        '''Start a job'''    
        print config_file
        self.config_file=config_file
        self.slurm_options=SlurmOptions()
        self.slurm_options.read(config_file)
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
        joblist=[]
        for job in self.joblist:
                #os.chdir(job.path)
                if job.status() == 'Starting':
                    time.sleep(10)
                if job.status()=='END':
                    Due2Time=False                    
                    with open(job.stdout) as handle:
                        for line in handle:
                            if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                                Due2Time=True
                    if Due2Time:
                        job.restart()
                        joblist+=[job]
                    else:
                        job.remove()
                elif job.status() in ['PD','R']:
                    joblist+=[job]
                    self.wait_time=job.WallTime() if job.WallTime()<self.wait_time else self.wait_time
                else:
                    job.remove()
                    self.wait_time=0
        self.joblist=joblist
    
    def run(self,max_wait_time=60*60):
        self.log_file='%s/Tracking/SlurmTracker.log'%my_path() 
        self.pickle_file='%s/Tracking/SlurmTracker.pickle'%my_path()
        self.max_wait_time=max_wait_time
        time.sleep(10)
        while True:
            self.last_check=time.ctime()
            log=open(self.log_file,'w+')
            #Kill daemon when pidfile is erased
            self.checkpid()
            if not pid:
                log.write("Daemon killed remotely.\n")
                log.write("Daemon stop at %s\n"%time.ctime(time.time()))              
                log.close()
                break
            
            #List jobs
            log.write("Job list at %s\n"%time.ctime(time.time()))
            self.listjobs()
            log.write('\n'.join(['\t'.join([str(j.jobid),j.state]) for j in self.joblist])+'\n')
           
            #Check every job         
            self.checkjobs()             
            if len(self.joblist)==0:
                break
            with open(self.pickle_file,'w+') as f:     
                pickle.dump(self,f)
            log.write('Waiting %i seconds\n'%(self.wait_time+10))
            log.close()  
            time.sleep(10+self.wait_time) #If end near, check in 10s
        log=open(self.log_file,'a+')        
        log.write("No more processes active, Daemon stop at %s\n"%time.ctime(time.time()))  
        log.close()
        os.remove(self.pickle_file)
 
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
            self.state='Not Running'

            

    def start(self,args):
        '''Starts a job'''
        dry=args.test
        #Start a slurm job
        Job=SlurmJob(args.slurm_script)
        Job.run()
        print 'Job sent to queue as ',Job.jobid
        if self.state=='Not Running':
            S = SlurmTracker('%s/Tracking/SlurmTracker.pid'%my_path())
            print 'Tracker starts'
            S.start()
            
        
    def track(self,script,jobid):
        print "Not implemented"
            
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
        
    def status(self,args):
        dry=args.test
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
                ST=pickle.load(F)
            print 'Running'
            print 'Host: ',self.hostname
            print 'Pid: ',self.pid
            print 'Wait time: ',ST.wait_time
            print 'Last check: ',ST.last_check
            with open(ST.log_file) as F:
                for line in F:
                    print line.strip()
        else:
            print "Not Running"
            
    def stop(self,args):
        if self.state=='Running':
            with open(self.pickle_file) as F:
                ST=pickle.load(F)
                ST.stop()
        try:
            os.remove(self.pidfile)
        except OSError:
            pass
        try:
            os.remove(self.pickle_file)
        except OSError:
            pass
        
        

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
    track_parser.add_argument('slurm_script', help='Slurm script') #Slurm script
    track_parser.add_argument('job', help='Slurm job') #Slurm job
    track_parser.set_defaults(func=main.track)
    
    # Status
    status_parser = subparsers.add_parser('status', help='Prints status of the tracker and the list jobs being currently tracked')
    status_parser.set_defaults(func=main.status)
    
    # Cancel jobs
    cancel_parser = subparsers.add_parser('cancel', help='Cancels a job')
    cancel_parser.add_argument('job', help='Slurm job') #Slurm job
    cancel_parser.set_defaults(func=main.cancel)
    
    # Test
    test_parser = subparsers.add_parser('test', help='Tests if every command on the scripts works correctly')
    test_parser.set_defaults(func=main.test)
    
    #Stop
    stop_parser = subparsers.add_parser('stop', help='Stops the tracker')
    stop_parser.set_defaults(func=main.stop)
    
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args()
    #print args
    args.func(args)
