#S=slurm.options()
#S.change('aa','bb')
#S.help('qq')
#S.options()

class Options:    
    S={}    
    
    def __init__(self):
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

    def define_option(self,variable,default_value,explicit,var_type,description):    
        self.S.update({variable:{'value':default_value,'add':explicit,'check':var_type,'help':description}})
    
    def option(self,variable,value,explicit=True,var_type='pass',description=''):
        if variable in self.S.keys():
            assert self.check(self.S[variable],value),'Not a valid value for %s: %s'%(variable,value)
            self.S[variable]['value']=value
        else:
            self.define_option(variable,value,explicit,var_type,description)
    
    def del_option(self,option):
        self.S.pop(option,None)        

    def help(self, option=''):
        if option=='':
            return 'Select from:\n %s'%'\n'.join(S.keys)   
        return self.S[option]['help']        

    def options(self):
        t=self.S.keys()
        t.sort()
        return t

    def Op(self,variable,default_value,explicit,var_type,description):
        return{variable:{'value':default_value,'add':explicit,'check':var_type,'help':description}}
   
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


        
    #def __iter__(self):
    #    I=[]        
    #    for s in self.S.keys():
    #        I+=[self.opt(s,self.S[s]['value'])]
    #    return iter(I)

    def __str__(self):
        s=''   
        s+='#!/bin/bash\n'
        for opt in self.options():
            if self.S[opt]['add']:        
                s+='#SBATCH --%s=%s\n'%(opt,str(self.S[opt]['value']))
        return s

import subprocess

def queue():
    '''Call slurm queue and return a list of dictionaries'''
    squeue=subprocess.check_output(['squeue']) #Call squeue
    squeue=squeue.split('\n') #Split per line
    squeue=[s.split() for s in squeue] #Split per column
    squeue=[dict(zip(squeue[0],s)) for s in squeue[1:-1]] #Create a Dict and clean
    return squeue

def send(commands,options=Options(),config_file='slurm_temp.conf'):
    '''Send the job to the slurm queue'''
    #Write the config file
    with open(config_file,'w+') as f:
        f.write(str(options))
        f.write(commands)
    #Submit the file
    jobid=subprocess.check_output(['sbatch',config_file])
    return jobid

def status(jobid):
    '''Get the status of the job'''
    #Get the status on the queue
    #If not in the queue get the status in the output

def cancel(jobid):
    '''Cancel the job'''
    subprocess.call(['scancel',jobid])

#S=Options()
#S.option('ntasks',5)
